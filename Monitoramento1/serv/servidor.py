from flask import (
    Flask,
    request,
    render_template,
    jsonify,
    redirect,
    url_for,
    session,
    flash,
    send_file,
)
import sqlite3
import os
from datetime import datetime, date
import ctypes
import ctypes.wintypes
from functools import wraps
import pandas as pd  # type: ignore
import io
import re
import hashlib
import secrets

app = Flask(__name__)
# Use variável de ambiente ou gere uma chave segura
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))

# Caminho relativo para o banco de dados
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE_DIR, "print_events.db")


def hash_password(password):
    """Cria hash seguro da senha"""
    return hashlib.sha256(password.encode()).hexdigest()


def verify_password(password, hashed):
    """Verifica se a senha corresponde ao hash"""
    return hashlib.sha256(password.encode()).hexdigest() == hashed


def validate_date(date_string):
    """Valida formato de data"""
    if not date_string:
        return True
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False


# --- SID para nome de usuário ---
def sid_to_username(sid_str):
    try:
        ConvertStringSidToSid = ctypes.windll.advapi32.ConvertStringSidToSidW
        ConvertStringSidToSid.argtypes = [
            ctypes.wintypes.LPCWSTR,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        ConvertStringSidToSid.restype = ctypes.wintypes.BOOL

        LookupAccountSid = ctypes.windll.advapi32.LookupAccountSidW
        LookupAccountSid.argtypes = [
            ctypes.wintypes.LPCWSTR,
            ctypes.c_void_p,
            ctypes.wintypes.LPWSTR,
            ctypes.POINTER(ctypes.wintypes.DWORD),
            ctypes.wintypes.LPWSTR,
            ctypes.POINTER(ctypes.wintypes.DWORD),
            ctypes.POINTER(ctypes.wintypes.DWORD),
        ]
        LookupAccountSid.restype = ctypes.wintypes.BOOL

        pSid = ctypes.c_void_p()
        if not ConvertStringSidToSid(sid_str, ctypes.byref(pSid)):
            return None

        name_len = ctypes.wintypes.DWORD(0)
        domain_len = ctypes.wintypes.DWORD(0)
        peUse = ctypes.wintypes.DWORD()

        LookupAccountSid(
            None,
            pSid,
            None,
            ctypes.byref(name_len),
            None,
            ctypes.byref(domain_len),
            ctypes.byref(peUse),
        )
        name = ctypes.create_unicode_buffer(name_len.value)
        domain = ctypes.create_unicode_buffer(domain_len.value)

        success = LookupAccountSid(
            None,
            pSid,
            name,
            ctypes.byref(name_len),
            domain,
            ctypes.byref(domain_len),
            ctypes.byref(peUse),
        )
        if not success:
            return None

        ctypes.windll.kernel32.LocalFree(pSid)
        return f"{domain.value}\\{name.value}"
    except Exception:
        return None


# --- Inicializa DB ---
def init_db():
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    with sqlite3.connect(DB) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            user TEXT,
            machine TEXT,
            pages_printed INTEGER DEFAULT 1)"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS users (
            user TEXT PRIMARY KEY,
            sector TEXT)"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS materiais (
            nome TEXT,
            preco REAL,
            rendimento INTEGER,
            valor REAL,
            data_inicio TEXT)"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS login (
            username TEXT PRIMARY KEY,
            password TEXT,
            is_admin INTEGER DEFAULT 0)"""
        )
        
        # Verifica se coluna is_admin existe
        colunas = conn.execute("PRAGMA table_info(login)").fetchall()
        if "is_admin" not in [col[1] for col in colunas]:
            conn.execute(
                "ALTER TABLE login ADD COLUMN is_admin INTEGER DEFAULT 0")
        
        # Cria usuário admin padrão com senha hasheada
        if not conn.execute(
                "SELECT 1 FROM login WHERE username = 'admin'").fetchone():
            admin_password_hash = hash_password('123')
            conn.execute(
                "INSERT INTO login (username, password, is_admin) VALUES ('admin', ?, 1)",
                (admin_password_hash,)
            )


# --- Decorators de login ---
def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("is_admin"):
            return "Acesso negado: apenas administradores", 403
        return f(*args, **kwargs)
    return wrapper


def custo_unitario_por_data(data_evento: str) -> float:
    """Calcula o custo unitário por página somando os custos dos materiais vigentes na data."""
    with sqlite3.connect(DB) as conn:
        conn.row_factory = sqlite3.Row
        query = """
            SELECT preco, rendimento FROM materiais
            WHERE date(data_inicio) <= date(?)
            ORDER BY data_inicio DESC
        """
        materiais = conn.execute(query, (data_evento,)).fetchall()

    custo_total = 0.0
    for mat in materiais:
        if mat["rendimento"] and mat["rendimento"] > 0:
            custo_total += mat["preco"] / mat["rendimento"]
    return custo_total


# --- API para receber eventos do agente ---
@app.route("/api/print_events", methods=["POST"])
def receive_print_events():
    """Recebe eventos de impressão do agente"""
    try:
        data = request.get_json()
        if not data or "events" not in data:
            return jsonify({"status": "error", "message": "Dados inválidos"}), 400
        
        events = data.get("events", [])
        if not isinstance(events, list):
            return jsonify({"status": "error", "message": "Events deve ser uma lista"}), 400
        
        with sqlite3.connect(DB) as conn:
            for event in events:
                # Validação básica dos dados do evento
                if not all(key in event for key in ["date", "user", "machine", "pages"]):
                    continue
                
                # Converte SID para nome de usuário se possível
                user = event.get("user", "Desconhecido")
                if user.startswith("S-1-"):  # É um SID
                    username = sid_to_username(user)
                    if username:
                        user = username
                
                # Validação de páginas
                pages = event.get("pages", 1)
                if not isinstance(pages, int) or pages < 1:
                    pages = 1
                
                conn.execute(
                    "INSERT INTO events (date, user, machine, pages_printed) VALUES (?, ?, ?, ?)",
                    (event["date"], user, event["machine"], pages)
                )
            conn.commit()
        
        return jsonify({"status": "success", "message": f"{len(events)} eventos processados"}), 200
    
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        user = request.form.get("username", "").strip()
        pwd = request.form.get("password", "")
        
        if not user or not pwd:
            flash("Usuário e senha são obrigatórios", "danger")
            return render_template("login.html")
        
        with sqlite3.connect(DB) as conn:
            result = conn.execute(
                "SELECT username, password, is_admin FROM login WHERE username = ?", (user,)
            ).fetchone()
        
        if result and verify_password(pwd, result[1]):
            session["logged_in"] = True
            session["user"] = user
            session["is_admin"] = result[2] == 1
            return redirect(url_for("home"))
        else:
            flash("Usuário ou senha inválidos", "danger")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def home():
    return redirect(url_for("all_users"))


@app.route("/usuarios")
@login_required
def all_users():
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    filtro_usuario = request.args.get("filtro_usuario", "").strip()

    # Validação de datas
    if start_date and not validate_date(start_date):
        flash("Data de início inválida", "warning")
        start_date = ""
    if end_date and not validate_date(end_date):
        flash("Data de fim inválida", "warning")
        end_date = ""

    query = """SELECT user, machine, COUNT(*) as total_impressos, SUM(pages_printed) as total_paginas
               FROM events WHERE 1=1"""
    params = []
    if start_date:
        query += " AND date(date) >= date(?)"
        params.append(start_date)
    if end_date:
        query += " AND date(date) <= date(?)"
        params.append(end_date)
    if filtro_usuario:
        query += " AND user LIKE ?"
        params.append(f"%{filtro_usuario}%")
    query += " GROUP BY user, machine ORDER BY user"

    with sqlite3.connect(DB) as conn:
        data = conn.execute(query, params).fetchall()
    return render_template(
        "usuarios.html",
        data=data,
        start_date=start_date,
        end_date=end_date,
        filtro_usuario=filtro_usuario,
    )


@app.route("/usuarios/export")
@login_required
def export_usuarios_excel():
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    filtro_usuario = request.args.get("filtro_usuario", "").strip()

    # Validação de datas
    if start_date and not validate_date(start_date):
        flash("Data de início inválida", "warning")
        return redirect(url_for("all_users"))
    if end_date and not validate_date(end_date):
        flash("Data de fim inválida", "warning")
        return redirect(url_for("all_users"))

    query = """SELECT user, machine, COUNT(*) as total_impressos, SUM(pages_printed) as total_paginas
               FROM events WHERE 1=1"""
    params = []
    if start_date:
        query += " AND date(date) >= date(?)"
        params.append(start_date)
    if end_date:
        query += " AND date(date) <= date(?)"
        params.append(end_date)
    if filtro_usuario:
        query += " AND user LIKE ?"
        params.append(f"%{filtro_usuario}%")
    query += " GROUP BY user, machine ORDER BY user"

    with sqlite3.connect(DB) as conn:
        df = pd.read_sql_query(query, conn, params=params)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Usuários")
    output.seek(0)

    return send_file(
        output,
        download_name="usuarios.xlsx",
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/admin/usuarios", methods=["GET", "POST"])
@admin_required
def admin_usuarios():
    message = ""
    conn = sqlite3.connect(DB)
    cursor = conn.cursor()

    if request.method == "POST":
        action = request.form.get("action")
        usuario = request.form.get("usuario", "").strip()
        setor = request.form.get("setor", "").strip()

        if not usuario:
            message = "Nome de usuário é obrigatório"
        elif action == "edit":
            if not setor:
                message = "Setor é obrigatório"
            else:
                cursor.execute("SELECT 1 FROM users WHERE user = ?", (usuario,))
                exists = cursor.fetchone()
                if exists:
                    cursor.execute(
                        "UPDATE users SET sector = ? WHERE user = ?", (setor, usuario)
                    )
                else:
                    cursor.execute(
                        "INSERT INTO users (user, sector) VALUES (?, ?)", (usuario, setor)
                    )
                conn.commit()
                message = f"Setor do usuário '{usuario}' atualizado para '{setor}'."
        elif action == "delete":
            cursor.execute("DELETE FROM users WHERE user = ?", (usuario,))
            conn.commit()
            message = f"Usuário '{usuario}' excluído com sucesso."

    cursor.execute("""
        SELECT DISTINCT e.user, u.sector
        FROM events e
        LEFT JOIN users u ON e.user = u.user
        ORDER BY e.user
    """)
    usuarios = cursor.fetchall()
    conn.close()

    return render_template("admin_usuarios.html", usuarios=usuarios, message=message)


@app.route("/admin/precos", methods=["GET", "POST"])
@admin_required
def admin_precos():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    message = None
    data_ref = request.args.get("data_ref")
    custo_total = 0.0

    if request.method == "POST":
        action = request.form.get("action")
        nome = request.form.get("nome", "").strip()
        preco = request.form.get("preco", "")
        rendimento = request.form.get("rendimento", "")
        data_inicio = request.form.get("data_inicio", "")

        if action in ["add", "update"]:
            if not all([nome, preco, rendimento, data_inicio]):
                flash("Todos os campos são obrigatórios.", "warning")
            elif not validate_date(data_inicio):
                flash("Data de início inválida", "warning")
            else:
                try:
                    preco_f = float(preco)
                    rendimento_i = int(rendimento)
                    
                    if preco_f <= 0 or rendimento_i <= 0:
                        flash("Preço e rendimento devem ser maiores que zero.", "warning")
                    else:
                        valor = preco_f / rendimento_i

                        if action == "add":
                            conn.execute(
                                "INSERT INTO materiais (nome, preco, rendimento, valor, data_inicio) VALUES (?, ?, ?, ?, ?)",
                                (nome, preco_f, rendimento_i, valor, data_inicio),
                            )
                            flash("Material cadastrado com sucesso!", "success")
                        else:  # update
                            conn.execute(
                                "UPDATE materiais SET preco=?, rendimento=?, valor=?, data_inicio=? WHERE nome=?",
                                (preco_f, rendimento_i, valor, data_inicio, nome),
                            )
                            flash("Material atualizado com sucesso!", "success")
                        conn.commit()
                        return redirect(url_for("admin_precos"))
                except ValueError:
                    flash("Preço deve ser um número válido e rendimento um número inteiro.", "danger")
                except sqlite3.IntegrityError:
                    flash("Material já existe.", "danger")
                except Exception as e:
                    flash(f"Erro ao salvar material: {e}", "danger")

        elif action == "delete":
            if nome:
                conn.execute("DELETE FROM materiais WHERE nome = ?", (nome,))
                conn.commit()
                flash(f"Material '{nome}' excluído com sucesso.", "success")
                return redirect(url_for("admin_precos"))

    materiais = conn.execute(
        "SELECT nome, preco, rendimento, valor, data_inicio FROM materiais ORDER BY data_inicio DESC"
    ).fetchall()

    if data_ref:
        if validate_date(data_ref):
            materiais_ativos = conn.execute(
                "SELECT valor FROM materiais WHERE date(data_inicio) <= date(?)",
                (data_ref,),
            ).fetchall()
            custo_total = sum(row["valor"] for row in materiais_ativos)
        else:
            flash("Data de referência inválida", "warning")
    else:
        data_ref = datetime.now().date().isoformat()

    conn.close()

    return render_template(
        "admin_precos.html",
        materiais=materiais,
        custo_total=custo_total,
        data_ref=data_ref,
        message=message,
    )


@app.route("/admin/logins", methods=["GET", "POST"])
@admin_required
def admin_logins():
    message = ""
    if request.method == "POST":
        action = request.form.get("action")
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        is_admin = int(bool(request.form.get("is_admin")))

        if not username:
            message = "Nome de usuário é obrigatório"
        elif not password and action in ["add", "edit"]:
            message = "Senha é obrigatória"
        else:
            with sqlite3.connect(DB) as conn:
                if action == "add":
                    try:
                        hashed_password = hash_password(password)
                        conn.execute(
                            "INSERT INTO login (username, password, is_admin) VALUES (?, ?, ?)",
                            (username, hashed_password, is_admin),
                        )
                        message = "Usuário adicionado."
                    except sqlite3.IntegrityError:
                        message = "Usuário já existe."
                elif action == "edit":
                    hashed_password = hash_password(password)
                    conn.execute(
                        "UPDATE login SET password = ?, is_admin = ? WHERE username = ?",
                        (hashed_password, is_admin, username),
                    )
                    message = "Usuário atualizado."
                elif action == "delete":
                    if username != "admin":
                        conn.execute(
                            "DELETE FROM login WHERE username = ?", (username,))
                        message = "Usuário removido."
                    else:
                        message = "Não é possível remover o admin."
                conn.commit()

    with sqlite3.connect(DB) as conn:
        usuarios = conn.execute(
            "SELECT username, is_admin FROM login ORDER BY username"
        ).fetchall()
    return render_template("admin_logins.html",
                           usuarios=usuarios, message=message)


@app.route("/dashboard")
@login_required
def dashboard():
    with sqlite3.connect(DB) as conn:
        conn.row_factory = sqlite3.Row

        total_impressos = conn.execute(
            "SELECT COUNT(*) FROM events").fetchone()[0]
        total_paginas = (
            conn.execute("SELECT SUM(pages_printed) FROM events").fetchone()[
                0] or 0
        )
        total_usuarios = conn.execute(
            "SELECT COUNT(DISTINCT user) FROM events"
        ).fetchone()[0]
        total_setores = conn.execute(
            "SELECT COUNT(DISTINCT sector) FROM users"
        ).fetchone()[0]

        setores_data = conn.execute(
            """
            SELECT COALESCE(u.sector, 'Sem Setor') as sector, COUNT(*) as total_impressos
            FROM events e
            LEFT JOIN users u ON e.user = u.user
            GROUP BY sector
            ORDER BY total_impressos DESC
            LIMIT 5
        """
        ).fetchall()

        usuarios_data = conn.execute(
            """
            SELECT user, COUNT(*) as total_impressos
            FROM events
            GROUP BY user
            ORDER BY total_impressos DESC
            LIMIT 5
        """
        ).fetchall()

        impressao_7dias = conn.execute(
            """
            SELECT date(date) as dia, COUNT(*) as total
            FROM events
            WHERE date(date) >= date('now', '-7 days')
            GROUP BY dia ORDER BY dia
        """
        ).fetchall()

    setores_labels = [row["sector"] for row in setores_data]
    setores_values = [row["total_impressos"] for row in setores_data]

    usuarios_labels = [row["user"] for row in usuarios_data]
    usuarios_values = [row["total_impressos"] for row in usuarios_data]

    dias_labels = [row["dia"] for row in impressao_7dias]
    dias_values = [row["total"] for row in impressao_7dias]

    return render_template(
        "dashboard.html",
        total_impressos=total_impressos,
        total_paginas=total_paginas,
        total_usuarios=total_usuarios,
        total_setores=total_setores,
        setores_labels=setores_labels,
        setores_values=setores_values,
        usuarios_labels=usuarios_labels,
        usuarios_values=usuarios_values,
        dias_labels=dias_labels,
        dias_values=dias_values,
    )


@app.route("/setores")
@login_required
def painel_setores():
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    filtro_setor = request.args.get("filtro_setor", "").strip()

    # Validação de datas
    if start_date and not validate_date(start_date):
        flash("Data de início inválida", "warning")
        start_date = ""
    if end_date and not validate_date(end_date):
        flash("Data de fim inválida", "warning")
        end_date = ""

    query = """SELECT COALESCE(u.sector, 'Sem Setor') as sector,
                      e.date,
                      SUM(e.pages_printed) as total_paginas,
                      COUNT(e.id) as total_impressos
               FROM events e
               LEFT JOIN users u ON e.user = u.user
               WHERE 1=1"""
    params = []
    if start_date:
        query += " AND date(e.date) >= date(?)"
        params.append(start_date)
    if end_date:
        query += " AND date(e.date) <= date(?)"
        params.append(end_date)
    if filtro_setor:
        query += " AND u.sector LIKE ?"
        params.append(f"%{filtro_setor}%")

    query += " GROUP BY u.sector, date(e.date) ORDER BY u.sector, e.date"

    with sqlite3.connect(DB) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()

    setores_agrupados = {}
    for row in rows:
        setor = row["sector"] or "Sem Setor"
        data_evento = row["date"]
        total_paginas = row["total_paginas"]
        total_impressos = row["total_impressos"]

        custo_unitario = custo_unitario_por_data(data_evento)
        valor_estimado = total_paginas * custo_unitario

        if setor not in setores_agrupados:
            setores_agrupados[setor] = {
                "total_paginas": 0,
                "total_impressos": 0,
                "valor_estimado": 0.0,
            }

        setores_agrupados[setor]["total_paginas"] += total_paginas
        setores_agrupados[setor]["total_impressos"] += total_impressos
        setores_agrupados[setor]["valor_estimado"] += valor_estimado

    setores_final = []
    for setor, dados in setores_agrupados.items():
        setores_final.append({
            "sector": setor,
            "total_paginas": dados["total_paginas"],
            "total_impressos": dados["total_impressos"],
            "valor_estimado": round(dados["valor_estimado"], 2),
        })

    # Consulta usuários por setor
    with sqlite3.connect(DB) as conn:
        conn.row_factory = sqlite3.Row
        usuarios_por_setor = conn.execute("""
            SELECT COALESCE(u.sector, 'Sem Setor') as sector, e.user
            FROM events e
            LEFT JOIN users u ON e.user = u.user
            GROUP BY sector, e.user
        """).fetchall()

    usuarios_dict = {}
    for row in usuarios_por_setor:
        setor = row["sector"]
        user = row["user"]
        usuarios_dict.setdefault(setor, []).append(user)

    return render_template(
        "setores.html",
        setores=setores_final,
        usuarios_por_setor=usuarios_dict,
        start_date=start_date,
        end_date=end_date,
        filtro_setor=filtro_setor,
    )


@app.route("/setores/export")
@login_required
def export_setores_excel():
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    filtro_setor = request.args.get("filtro_setor", "").strip()

    # Validação de datas
    if start_date and not validate_date(start_date):
        flash("Data de início inválida", "warning")
        return redirect(url_for("painel_setores"))
    if end_date and not validate_date(end_date):
        flash("Data de fim inválida", "warning")
        return redirect(url_for("painel_setores"))

    # Query corrigida usando materiais ao invés de precos
    query = """
        SELECT 
            COALESCE(u.sector, 'Sem Setor') as sector,
            COUNT(e.id) as total_impressos,
            SUM(e.pages_printed) as total_paginas
        FROM events e
        LEFT JOIN users u ON e.user = u.user
        WHERE 1=1
    """
    params = []
    if start_date:
        query += " AND date(e.date) >= date(?)"
        params.append(start_date)
    if end_date:
        query += " AND date(e.date) <= date(?)"
        params.append(end_date)
    if filtro_setor:
        query += " AND u.sector LIKE ?"
        params.append(f"%{filtro_setor}%")
    query += " GROUP BY sector ORDER BY sector"

    with sqlite3.connect(DB) as conn:
        df = pd.read_sql_query(query, conn, params=params)
        
        # Adiciona coluna de valor estimado calculado
        if not df.empty:
            # Para simplificar, usar o custo atual para todos
            custo_atual = custo_unitario_por_data(datetime.now().date().isoformat())
            df['valor_estimado'] = df['total_paginas'] * custo_atual

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Setores")
    output.seek(0)

    return send_file(
        output,
        download_name="setores.xlsx",
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/api/impressao-tendencia")
@login_required
def api_impressao_tendencia():
    with sqlite3.connect(DB) as conn:
        data = conn.execute(
            """
            SELECT date(date) as dia, COUNT(*) as total_impressos
            FROM events
            WHERE date >= date('now', '-30 days')
            GROUP BY dia
            ORDER BY dia
        """
        ).fetchall()

    resultados = []
    prev = None
    for row in data:
        dia, total = row
        crescimento = None
        if prev is not None and prev[1] > 0:
            crescimento = round((total - prev[1]) / prev[1] * 100, 2)
        resultados.append(
            {"dia": dia, "total_impressos": total, "crescimento_pct": crescimento}
        )
        prev = row

    return jsonify(resultados)


@app.route("/api/impressao-dia")
@login_required
def api_impressao_dia():
    hoje = datetime.now().date()
    with sqlite3.connect(DB) as conn:
        data = conn.execute(
            """
            SELECT user, COUNT(*) as total_impressos
            FROM events
            WHERE date(date) = ?
            GROUP BY user
            ORDER BY total_impressos DESC
            LIMIT 10
        """,
            (hoje,),
        ).fetchall()

    resultados = [{"user": row[0], "total_impressos": row[1]} for row in data]
    return jsonify(resultados)


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5001, debug=True)