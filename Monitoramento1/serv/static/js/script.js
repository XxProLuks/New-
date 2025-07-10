// Main JavaScript functionality
document.addEventListener("DOMContentLoaded", function() {
    // Delete confirmation
    document.querySelectorAll(".btn-delete").forEach(function(btn) {
        btn.addEventListener("click", function(e) {
            if (!confirm("Tem certeza que deseja excluir?")) {
                e.preventDefault();
            }
        });
    });

    // Form validation
    const forms = document.querySelectorAll('form');
    forms.forEach(function(form) {
        form.addEventListener('submit', function(e) {
            const requiredFields = form.querySelectorAll('[required]');
            let isValid = true;

            requiredFields.forEach(function(field) {
                if (!field.value.trim()) {
                    field.style.borderColor = '#dc3545';
                    isValid = false;
                } else {
                    field.style.borderColor = '#ccc';
                }
            });

            if (!isValid) {
                e.preventDefault();
                alert('Por favor, preencha todos os campos obrigatórios.');
            }
        });
    });

    // Table sorting (optional)
    const tableHeaders = document.querySelectorAll('th[data-sort]');
    tableHeaders.forEach(function(header) {
        header.style.cursor = 'pointer';
        header.addEventListener('click', function() {
            // Add sorting functionality here if needed
            console.log('Sorting by:', header.textContent);
        });
    });
});
