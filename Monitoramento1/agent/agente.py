import subprocess
import requests
import re
import time
import logging
import json
import os
import socket
from datetime import datetime
from typing import List, Dict, Optional

# Configuração
CONFIG_FILE = "config.json"
PROCESSED_EVENTS_FILE = "processed_events.json"
DEFAULT_CONFIG = {
    "server_url": "http://192.168.0.4:5002/api/print_events",
    "retry_interval": 30,
    "check_interval": 5,
    "max_retries": 3,
    "log_level": "INFO",
    "batch_size": 50,  # Tamanho do lote para envio
    "process_all_on_start": True  # Processar todos os eventos na inicialização
}

def load_config() -> dict:
    """Carrega configuração do arquivo JSON ou cria com valores padrão"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
            # Verifica se todas as chaves necessárias estão presentes
            for key in DEFAULT_CONFIG:
                if key not in config:
                    config[key] = DEFAULT_CONFIG[key]
            return config
        except Exception as e:
            print(f"Erro ao carregar configuração: {e}")
    
    # Cria arquivo de configuração padrão
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(DEFAULT_CONFIG, f, indent=4, ensure_ascii=False)
    
    return DEFAULT_CONFIG.copy()

# Carrega configuração
config = load_config()

# Configurar logging
log_level = getattr(logging, config["log_level"].upper(), logging.INFO)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('print_monitor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class PrintMonitor:
    def __init__(self):
        self.server_url = config["server_url"]
        self.retry_interval = config["retry_interval"]
        self.check_interval = config["check_interval"]
        self.max_retries = config["max_retries"]
        self.batch_size = config.get("batch_size", 50)
        self.process_all_on_start = config.get("process_all_on_start", True)
        self.machine_name = socket.gethostname()
        
        # Carrega eventos já processados do arquivo
        self.eventos_processados = self.carregar_eventos_processados()
        self.highest_record_id = 0
        
        # Calcula o maior record_id desta máquina
        for evento_id in self.eventos_processados:
            if evento_id.startswith(f"{self.machine_name}_"):
                try:
                    record_id = int(evento_id.split('_', 1)[1])
                    self.highest_record_id = max(self.highest_record_id, record_id)
                except:
                    pass
        
        logger.info(f"💻 Máquina: {self.machine_name}")
        logger.info(f"📋 Carregados {len(self.eventos_processados)} eventos já processados (todas as máquinas)")
        
        # Conta eventos desta máquina
        eventos_desta_maquina = sum(1 for e in self.eventos_processados if e.startswith(f"{self.machine_name}_"))
        logger.info(f"📌 Eventos desta máquina já processados: {eventos_desta_maquina}")
        logger.info(f"📌 Maior ID processado nesta máquina: {self.highest_record_id}")
    
    def criar_id_unico(self, record_id: int, machine_name: str = None) -> str:
        """Cria um ID único combinando máquina e record_id"""
        if not machine_name:
            machine_name = self.machine_name
        return f"{machine_name}_{record_id}"
    
    def carregar_eventos_processados(self) -> set:
        """Carrega IDs de eventos já processados do arquivo"""
        if os.path.exists(PROCESSED_EVENTS_FILE):
            try:
                with open(PROCESSED_EVENTS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Compatibilidade com versão anterior (só números)
                    processed_ids = data.get('processed_ids', [])
                    
                    # Converte IDs antigos para novo formato se necessário
                    converted_ids = set()
                    for pid in processed_ids:
                        if isinstance(pid, int):
                            # ID antigo, adiciona com nome da máquina atual
                            converted_ids.add(f"{self.machine_name}_{pid}")
                        else:
                            # Já está no formato novo
                            converted_ids.add(str(pid))
                    
                    return converted_ids
            except Exception as e:
                logger.error(f"Erro ao carregar eventos processados: {e}")
        return set()
    
    def salvar_eventos_processados(self):
        """Salva IDs de eventos processados no arquivo"""
        try:
            # Mantém apenas os últimos 50000 IDs para não crescer infinitamente
            if len(self.eventos_processados) > 50000:
                # Separa por máquina e mantém os mais recentes
                eventos_por_maquina = {}
                for evento_id in self.eventos_processados:
                    if '_' in evento_id:
                        machine, record_id = evento_id.split('_', 1)
                        if machine not in eventos_por_maquina:
                            eventos_por_maquina[machine] = []
                        try:
                            eventos_por_maquina[machine].append((int(record_id), evento_id))
                        except:
                            pass
                
                # Mantém os últimos 10k eventos de cada máquina
                novos_eventos = set()
                for machine, eventos in eventos_por_maquina.items():
                    eventos.sort(key=lambda x: x[0])  # Ordena por record_id
                    for _, evento_id in eventos[-10000:]:  # Últimos 10k
                        novos_eventos.add(evento_id)
                
                self.eventos_processados = novos_eventos
                logger.info(f"🧹 Arquivo de eventos limpo (mantidos {len(self.eventos_processados)} eventos)")
            
            # Conta eventos por máquina para estatísticas
            stats_por_maquina = {}
            for evento_id in self.eventos_processados:
                if '_' in evento_id:
                    machine = evento_id.split('_', 1)[0]
                    stats_por_maquina[machine] = stats_por_maquina.get(machine, 0) + 1
            
            data = {
                'processed_ids': list(self.eventos_processados),
                'last_update': datetime.now().isoformat(),
                'highest_id_this_machine': self.highest_record_id,
                'total_processed': len(self.eventos_processados),
                'stats_by_machine': stats_por_maquina
            }
            
            with open(PROCESSED_EVENTS_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
                
            logger.debug(f"💾 Salvos {len(self.eventos_processados)} IDs de eventos processados")
        except Exception as e:
            logger.error(f"Erro ao salvar eventos processados: {e}")
        
    def buscar_todos_eventos_powershell(self) -> List[Dict]:
        """Busca TODOS os eventos 307 usando PowerShell"""
        # Script PowerShell para buscar TODOS os eventos
        ps_script = """
        # Buscar TODOS os eventos 307 do log de impressão
        $eventos = Get-WinEvent -FilterHashtable @{
            LogName='Microsoft-Windows-PrintService/Operational'
            ID=307
        } -ErrorAction SilentlyContinue
        
        # Contador para feedback
        $total = $eventos.Count
        Write-Host "Total de eventos encontrados: $total"
        
        # Processar cada evento
        $contador = 0
        foreach ($evento in $eventos) {
            $contador++
            
            # Mostrar progresso a cada 100 eventos
            if ($contador % 100 -eq 0) {
                Write-Host "Processando evento $contador de $total..."
            }
            
            # Extrair dados do evento
            $output = @{
                RecordId = $evento.RecordId
                TimeCreated = $evento.TimeCreated.ToString('yyyy-MM-dd HH:mm:ss')
                UserId = if ($evento.UserId) { $evento.UserId.Value } else { 'Sistema' }
                MachineName = $evento.MachineName
                Message = $evento.Message
                Level = $evento.LevelDisplayName
            }
            
            # Converter para JSON em uma linha
            $output | ConvertTo-Json -Compress
        }
        
        Write-Host "Processamento concluído!"
        """
        
        try:
            logger.info("🔍 Buscando TODOS os eventos 307 no log...")
            
            # Executar PowerShell
            result = subprocess.run(
                ["powershell", "-ExecutionPolicy", "Bypass", "-Command", ps_script],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.stdout:
                eventos = []
                linhas = result.stdout.strip().split('\n')
                
                for linha in linhas:
                    linha = linha.strip()
                    if linha.startswith('{'):
                        try:
                            evento_json = json.loads(linha)
                            eventos.append(evento_json)
                        except json.JSONDecodeError:
                            continue
                    elif "Total de eventos encontrados:" in linha:
                        logger.info(f"📊 {linha}")
                    elif "Processando evento" in linha:
                        logger.debug(linha)
                
                logger.info(f"✅ Carregados {len(eventos)} eventos do PowerShell")
                
                # Log uma amostra das mensagens para debug
                if eventos and logger.isEnabledFor(logging.DEBUG):
                    logger.debug("📝 Amostra de mensagens de eventos:")
                    for i, evento in enumerate(eventos[:5]):
                        msg = evento.get('Message', '')[:200]
                        logger.debug(f"   Evento {i+1}: {msg}...")
                
                return eventos
            else:
                if result.stderr:
                    logger.warning(f"PowerShell stderr: {result.stderr}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Erro ao executar PowerShell: {e}")
            return []
    
    def buscar_eventos_recentes_powershell(self, minutos: int = 5) -> List[Dict]:
        """Busca eventos recentes usando PowerShell"""
        ps_script = f"""
        $startTime = (Get-Date).AddMinutes(-{minutos})
        
        # Buscar eventos 307 recentes
        $eventos = Get-WinEvent -FilterHashtable @{{
            LogName='Microsoft-Windows-PrintService/Operational'
            ID=307
            StartTime=$startTime
        }} -ErrorAction SilentlyContinue
        
        foreach ($evento in $eventos) {{
            $output = @{{
                RecordId = $evento.RecordId
                TimeCreated = $evento.TimeCreated.ToString('yyyy-MM-dd HH:mm:ss')
                UserId = if ($evento.UserId) {{ $evento.UserId.Value }} else {{ 'Sistema' }}
                MachineName = $evento.MachineName
                Message = $evento.Message
                Level = $evento.LevelDisplayName
            }}
            
            $output | ConvertTo-Json -Compress
        }}
        """
        
        try:
            result = subprocess.run(
                ["powershell", "-ExecutionPolicy", "Bypass", "-Command", ps_script],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.stdout:
                eventos = []
                for linha in result.stdout.strip().split('\n'):
                    if linha.strip() and linha.startswith('{'):
                        try:
                            eventos.append(json.loads(linha))
                        except json.JSONDecodeError:
                            continue
                return eventos
            return []
                
        except Exception as e:
            logger.error(f"Erro ao buscar eventos recentes: {e}")
            return []
    
    def extrair_dados_evento(self, evento_raw: Dict) -> Optional[Dict]:
        """Extrai dados relevantes do evento"""
        try:
            mensagem = evento_raw.get('Message', '')
            record_id = evento_raw.get('RecordId')
            
            # Dados base
            dados = {
                'record_id': record_id,  # Incluir para controle
                'date': evento_raw.get('TimeCreated', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                'user': 'Desconhecido',
                'machine': evento_raw.get('MachineName', self.machine_name),
                'pages': 1,
                'document': 'Documento',
                'printer': 'Impressora'
            }
            
            # Log da mensagem completa para debug
            logger.debug(f"Mensagem do evento {record_id}: {mensagem[:200]}...")
            
            # Detectar idioma e extrair dados
            if 'pertencente a' in mensagem or 'foi impresso' in mensagem:  # Português
                # Padrão: "O documento X, NOME pertencente a USUARIO em \\COMPUTADOR foi impresso em IMPRESSORA"
                
                # Extrair documento e usuário
                match = re.search(r'O documento \d+, (.+?) pertencente a (.+?) em', mensagem)
                if match:
                    dados['document'] = match.group(1).strip()
                    dados['user'] = match.group(2).strip()
                
                # Extrair impressora
                match = re.search(r'foi impresso em (.+?)(?:\s+pela porta|\s+através|\.|$)', mensagem)
                if match:
                    dados['printer'] = match.group(1).strip()
                
                # Extrair páginas - MÚLTIPLOS PADRÕES
                pages_found = False
                
                # Padrão 1: "Páginas impressas: X"
                match = re.search(r'Páginas impressas:\s*(\d+)', mensagem)
                if match:
                    dados['pages'] = int(match.group(1))
                    pages_found = True
                    logger.debug(f"Páginas encontradas (padrão PT 1): {dados['pages']}")
                
                # Padrão 2: "Total de páginas impressas: X"
                if not pages_found:
                    match = re.search(r'Total de páginas impressas:\s*(\d+)', mensagem)
                    if match:
                        dados['pages'] = int(match.group(1))
                        pages_found = True
                        logger.debug(f"Páginas encontradas (padrão PT 2): {dados['pages']}")
                
                # Padrão 3: "X página(s)"
                if not pages_found:
                    match = re.search(r'(\d+)\s+páginas?\b', mensagem, re.IGNORECASE)
                    if match:
                        dados['pages'] = int(match.group(1))
                        pages_found = True
                        logger.debug(f"Páginas encontradas (padrão PT 3): {dados['pages']}")
                    
            else:  # Inglês ou outros idiomas
                # Padrão: "Document X, NAME owned by USER on \\COMPUTER was printed on PRINTER"
                
                # Extrair documento e usuário
                match = re.search(r'Document \d+, (.+?) owned by (.+?) on', mensagem)
                if match:
                    dados['document'] = match.group(1).strip()
                    dados['user'] = match.group(2).strip()
                
                # Extrair impressora
                match = re.search(r'was printed on (.+?)(?:\s+through|\s+via|\.|$)', mensagem)
                if match:
                    dados['printer'] = match.group(1).strip()
                
                # Extrair páginas - MÚLTIPLOS PADRÕES
                pages_found = False
                
                # Padrão 1: "Pages printed: X"
                match = re.search(r'Pages printed:\s*(\d+)', mensagem)
                if match:
                    dados['pages'] = int(match.group(1))
                    pages_found = True
                    logger.debug(f"Páginas encontradas (padrão EN 1): {dados['pages']}")
                
                # Padrão 2: "Total pages printed: X"
                if not pages_found:
                    match = re.search(r'Total pages printed:\s*(\d+)', mensagem)
                    if match:
                        dados['pages'] = int(match.group(1))
                        pages_found = True
                        logger.debug(f"Páginas encontradas (padrão EN 2): {dados['pages']}")
                
                # Padrão 3: "X page(s)"
                if not pages_found:
                    match = re.search(r'(\d+)\s+pages?\b', mensagem, re.IGNORECASE)
                    if match:
                        dados['pages'] = int(match.group(1))
                        pages_found = True
                        logger.debug(f"Páginas encontradas (padrão EN 3): {dados['pages']}")
                
                # Padrão 4: Número isolado no final da mensagem
                if not pages_found:
                    match = re.search(r'(?:Size in bytes:|Tamanho em bytes:)\s*\d+\.\s*(?:Pages printed:|Páginas impressas:)?\s*(\d+)', mensagem)
                    if match:
                        dados['pages'] = int(match.group(1))
                        pages_found = True
                        logger.debug(f"Páginas encontradas (padrão 4): {dados['pages']}")
            
            # Se ainda não encontrou páginas, procura por padrões genéricos
            if dados['pages'] == 1:
                # Procura por qualquer número após palavras-chave
                patterns = [
                    r'(?:páginas?|pages?)\s*:\s*(\d+)',
                    r'(\d+)\s*(?:páginas?|pages?)',
                    r'(?:total|Total)\s*:\s*(\d+)',
                    r'(?:impressas?|printed)\s*:\s*(\d+)'
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, mensagem, re.IGNORECASE)
                    if match:
                        pages = int(match.group(1))
                        if 1 <= pages <= 10000:  # Validação
                            dados['pages'] = pages
                            logger.debug(f"Páginas encontradas (padrão genérico): {dados['pages']}")
                            break
            
            # Validação final
            if dados['pages'] < 1 or dados['pages'] > 10000:
                logger.warning(f"Número de páginas inválido ({dados['pages']}) para evento {record_id}, usando 1")
                dados['pages'] = 1
            
            # Log final com páginas
            logger.debug(f"Evento extraído: ID={record_id}, User={dados['user']}, Doc={dados['document'][:30]}..., Pages={dados['pages']}")
            
            return dados
            
        except Exception as e:
            logger.error(f"Erro ao processar evento: {e}")
            return None
    
    def send_events_batch(self, events: List[Dict]) -> bool:
        """Envia eventos em lotes para o servidor"""
        if not events:
            return True
        
        total_events = len(events)
        logger.info(f"📤 Enviando {total_events} eventos em lotes de {self.batch_size}...")
        
        # Remove record_id antes de enviar (é só para controle interno)
        events_to_send = []
        for event in events:
            event_copy = event.copy()
            event_copy.pop('record_id', None)
            events_to_send.append(event_copy)
        
        # Enviar em lotes
        success_count = 0
        failed_batches = []
        
        for i in range(0, total_events, self.batch_size):
            batch = events_to_send[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (total_events + self.batch_size - 1) // self.batch_size
            
            logger.info(f"📦 Enviando lote {batch_num}/{total_batches} ({len(batch)} eventos)...")
            
            if self.send_events(batch):
                success_count += len(batch)
            else:
                logger.warning(f"⚠️ Falha ao enviar lote {batch_num}")
                failed_batches.append(i)
            
            # Pequena pausa entre lotes
            if i + self.batch_size < total_events:
                time.sleep(1)
        
        logger.info(f"✅ Enviados {success_count}/{total_events} eventos com sucesso")
        
        # Retorna True apenas se TODOS os eventos foram enviados
        return len(failed_batches) == 0
    
    def send_events(self, events: List[Dict]) -> bool:
        """Envia eventos para o servidor com retry automático"""
        if not events:
            return True
        
        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    self.server_url, 
                    json={"events": events},
                    timeout=30,
                    headers={'Content-Type': 'application/json'}
                )
                
                if response.status_code == 200:
                    result = response.json()
                    logger.debug(f"Resposta do servidor: {result.get('message', '')}")
                    return True
                else:
                    logger.error(f"❌ Erro HTTP {response.status_code}: {response.text}")
                    
            except requests.exceptions.ConnectionError:
                logger.warning(f"⚠️ Tentativa {attempt + 1}: Servidor indisponível")
            except requests.exceptions.Timeout:
                logger.warning(f"⚠️ Tentativa {attempt + 1}: Timeout na requisição")
            except Exception as e:
                logger.error(f"❌ Erro na tentativa {attempt + 1}: {e}")
            
            if attempt < self.max_retries - 1:
                time.sleep(5)
        
        return False
    
    def processar_todos_eventos(self):
        """Processa e envia TODOS os eventos 307 existentes"""
        logger.info("🚀 Iniciando processamento de eventos...")
        
        # Busca todos os eventos
        eventos_raw = self.buscar_todos_eventos_powershell()
        
        if not eventos_raw:
            logger.info("ℹ️ Nenhum evento encontrado no log")
            return
        
        # Filtra apenas eventos não processados
        eventos_nao_processados = []
        eventos_ja_processados = 0
        
        for evento_raw in eventos_raw:
            record_id = evento_raw.get('RecordId', 0)
            machine_name = evento_raw.get('MachineName', self.machine_name)
            evento_id_unico = self.criar_id_unico(record_id, machine_name)
            
            if evento_id_unico not in self.eventos_processados:
                eventos_nao_processados.append(evento_raw)
            else:
                eventos_ja_processados += 1
        
        logger.info(f"📊 Total de eventos no log: {len(eventos_raw)}")
        logger.info(f"✅ Eventos já processados anteriormente: {eventos_ja_processados}")
        logger.info(f"🆕 Eventos novos para processar: {len(eventos_nao_processados)}")
        
        if not eventos_nao_processados:
            logger.info("ℹ️ Todos os eventos já foram processados anteriormente")
            return
        
        # Processa apenas eventos novos
        eventos_processados = []
        total_paginas = 0
        eventos_com_multiplas_paginas = 0
        
        for evento_raw in eventos_nao_processados:
            evento = self.extrair_dados_evento(evento_raw)
            if evento:
                eventos_processados.append(evento)
                total_paginas += evento['pages']
                if evento['pages'] > 1:
                    eventos_com_multiplas_paginas += 1
        
        if not eventos_processados:
            logger.info("ℹ️ Nenhum evento novo para enviar")
            return
        
        logger.info(f"📊 Total de {len(eventos_processados)} eventos novos processados")
        logger.info(f"📄 Total de {total_paginas} páginas impressas nos novos eventos")
        logger.info(f"📑 {eventos_com_multiplas_paginas} eventos novos com múltiplas páginas")
        
        # Mostra alguns exemplos de eventos com múltiplas páginas para debug
        exemplos = [e for e in eventos_processados if e['pages'] > 1][:5]
        if exemplos:
            logger.info("📋 Exemplos de eventos novos com múltiplas páginas:")
            for e in exemplos:
                logger.info(f"   - {e['date']} | {e['user']} | {e['document'][:30]}... | {e['pages']} páginas")
        
        # Envia em lotes
        if self.send_events_batch(eventos_processados):
            # Marca todos os eventos como processados
            for evento_raw in eventos_nao_processados:
                record_id = evento_raw.get('RecordId', 0)
                machine_name = evento_raw.get('MachineName', self.machine_name)
                evento_id_unico = self.criar_id_unico(record_id, machine_name)
                self.eventos_processados.add(evento_id_unico)
                
                # Atualiza highest_record_id apenas para esta máquina
                if machine_name == self.machine_name and record_id > self.highest_record_id:
                    self.highest_record_id = record_id
            
            # Salva estado
            self.salvar_eventos_processados()
            logger.info("✅ Eventos enviados e marcados como processados")
        else:
            logger.warning("⚠️ Alguns eventos não foram enviados, serão tentados novamente na próxima execução")
        
        logger.info(f"📌 Maior ID processado nesta máquina: {self.highest_record_id}")
    
    def monitor_events(self):
        """Loop principal de monitoramento"""
        logger.info("=== Iniciando monitoramento de eventos de impressão ===")
        logger.info(f"📡 Servidor: {self.server_url}")
        logger.info(f"⏱️ Intervalo de verificação: {self.check_interval}s")
        logger.info(f"💻 Máquina: {self.machine_name}")
        
        # Processar todos os eventos se configurado
        if self.process_all_on_start:
            logger.info("📋 Processamento inicial de TODOS os eventos habilitado")
            self.processar_todos_eventos()
        else:
            logger.info("⏭️ Processamento inicial desabilitado")
        
        logger.info("👀 Monitorando novos eventos...")
        eventos_buffer = []  # Buffer para eventos não enviados
        
        while True:
            try:
                # Busca eventos recentes
                eventos_raw = self.buscar_eventos_recentes_powershell(5)
                
                # Processa apenas eventos novos
                novos_eventos = []
                for evento_raw in eventos_raw:
                    record_id = evento_raw.get('RecordId', 0)
                    machine_name = evento_raw.get('MachineName', self.machine_name)
                    evento_id_unico = self.criar_id_unico(record_id, machine_name)
                    
                    # Só processa se for novo E desta máquina E maior que o último processado
                    if (evento_id_unico not in self.eventos_processados and 
                        machine_name == self.machine_name and 
                        record_id > self.highest_record_id):
                        
                        evento = self.extrair_dados_evento(evento_raw)
                        if evento:
                            novos_eventos.append(evento)
                            self.eventos_processados.add(evento_id_unico)
                
                # Adiciona ao buffer se houver novos eventos
                if novos_eventos:
                    eventos_buffer.extend(novos_eventos)
                    logger.info(f"🆕 Encontrados {len(novos_eventos)} novos eventos")
                
                # Tenta enviar eventos do buffer
                if eventos_buffer:
                    logger.info(f"📤 Tentando enviar {len(eventos_buffer)} eventos do buffer...")
                    
                    # Remove record_id antes de enviar
                    eventos_para_enviar = []
                    for e in eventos_buffer:
                        e_copy = e.copy()
                        e_copy.pop('record_id', None)
                        eventos_para_enviar.append(e_copy)
                    
                    if self.send_events(eventos_para_enviar):
                        # Atualiza highest_record_id e marca como processados
                        for e in eventos_buffer:
                            rid = e.get('record_id', 0)
                            # Cria ID único com nome da máquina
                            evento_id_unico = self.criar_id_unico(rid)
                            self.eventos_processados.add(evento_id_unico)
                            
                            # Atualiza highest_record_id apenas para esta máquina
                            if rid > self.highest_record_id:
                                self.highest_record_id = rid
                        
                        eventos_buffer.clear()
                        self.salvar_eventos_processados()  # Salva após enviar com sucesso
                        logger.info("✅ Buffer enviado com sucesso")
                    else:
                        logger.warning(f"⚠️ Mantendo {len(eventos_buffer)} eventos no buffer")
                        # Limita o buffer
                        if len(eventos_buffer) > 1000:
                            logger.warning("📦 Buffer muito grande, removendo eventos antigos")
                            eventos_buffer = eventos_buffer[-500:]
                
                # Limpa cache se muito grande
                if len(self.eventos_processados) > 10000:
                    logger.info("🧹 Limpando cache de eventos processados")
                    # Mantém apenas IDs acima do highest_record_id - 5000
                    self.eventos_processados = {
                        rid for rid in self.eventos_processados 
                        if rid > self.highest_record_id - 5000
                    }
                
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                logger.info("⏹️ Interrupção solicitada pelo usuário")
                break
            except Exception as e:
                logger.error(f"❌ Erro no loop: {e}")
                logger.info(f"⏳ Aguardando {self.retry_interval}s...")
                time.sleep(self.retry_interval)
        
        # Envia eventos restantes
        if eventos_buffer:
            logger.info(f"📤 Enviando {len(eventos_buffer)} eventos restantes...")
            eventos_para_enviar = []
            for e in eventos_buffer:
                e_copy = e.copy()
                e_copy.pop('record_id', None)
                eventos_para_enviar.append(e_copy)
            
            if self.send_events(eventos_para_enviar):
                # Marca como processados antes de sair
                for e in eventos_buffer:
                    rid = e.get('record_id', 0)
                    evento_id_unico = self.criar_id_unico(rid)
                    self.eventos_processados.add(evento_id_unico)
                self.salvar_eventos_processados()
        
        # Salva estado final
        self.salvar_eventos_processados()
        logger.info("👋 Monitoramento finalizado")

def test_connection():
    """Testa conexão com o servidor"""
    try:
        url_teste = config["server_url"].replace("/api/print_events", "/")
        response = requests.get(url_teste, timeout=5)
        if response.status_code == 200:
            logger.info("✅ Conexão com servidor OK")
            return True
        else:
            logger.warning(f"⚠️ Servidor retornou status {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"❌ Falha na conexão com servidor: {e}")
        return False

def test_powershell():
    """Testa se PowerShell está disponível"""
    try:
        result = subprocess.run(
            ["powershell", "-Command", "Write-Host 'PowerShell OK'"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            logger.info("✅ PowerShell funcionando")
            return True
        else:
            logger.error("❌ PowerShell não está funcionando corretamente")
            return False
    except Exception as e:
        logger.error(f"❌ PowerShell não disponível: {e}")
        return False

def main():
    """Função principal"""
    print("=" * 60)
    print("   AGENTE DE MONITORAMENTO DE IMPRESSÃO v3.1")
    print("=" * 60)
    
    logger.info(f"📁 Configuração carregada de: {CONFIG_FILE}")
    
    # Testes iniciais
    logger.info("🧪 Executando testes iniciais...")
    
    # Teste PowerShell
    if not test_powershell():
        logger.critical("PowerShell é necessário para este agente funcionar!")
        logger.info("Certifique-se de estar executando no Windows com PowerShell instalado")
        return
    
    # Teste de conectividade
    if not test_connection():
        logger.warning("Servidor pode estar indisponível, mas continuando...")
        logger.info("Os eventos serão armazenados e enviados quando disponível")
    
    # Inicializa e executa monitor
    monitor = PrintMonitor()
    try:
        monitor.monitor_events()
    except Exception as e:
        logger.critical(f"💥 Erro crítico: {e}")
        raise

if __name__ == "__main__":
    main()