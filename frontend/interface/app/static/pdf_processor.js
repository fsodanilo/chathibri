// Sidebar functionality
function toggleSidebar() {
    const sidebar = document.getElementById('sidebar');
    const title = sidebar.querySelector('.sidebar-title');
    const isCollapsed = sidebar.classList.toggle('collapsed');
    
    if (isCollapsed) {
        title.style.display = 'none';
    } else {
        title.style.display = 'flex';
    }
    
    localStorage.setItem('sidebarCollapsed', isCollapsed);
}

function toggleSection(button) {
    const section = button.nextElementSibling;
    section.classList.toggle("expanded");
}

window.addEventListener('DOMContentLoaded', () => {
    const sidebar = document.getElementById('sidebar');
    const title = sidebar.querySelector('.sidebar-title');
    const isCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
    
    if (isCollapsed) {
        sidebar.classList.add('collapsed');
        title.style.display = 'none';
    } else {
        sidebar.classList.remove('collapsed');
        title.style.display = 'flex';
    }
});

// DEBUG_INJECTION - Código de debug temporário
console.log("🔍 DEBUG: Interface carregada com debug ativo");

let selectedFile = null;
let selectedPdfForExtraction = null;
let selectedPdfForCustom = null;  // Nova variável para extração customizada

// Drag and drop functionality
const uploadArea = document.getElementById('uploadArea');

uploadArea.addEventListener('dragover', (e) => {
    e.preventDefault();
    uploadArea.classList.add('dragover');
});

uploadArea.addEventListener('dragleave', () => {
    uploadArea.classList.remove('dragover');
});

uploadArea.addEventListener('drop', (e) => {
    e.preventDefault();
    uploadArea.classList.remove('dragover');
    
    const files = e.dataTransfer.files;
    if (files.length > 0 && files[0].type === 'application/pdf') {
        handleFile(files[0]);
    } else {
        alert('Por favor, selecione um arquivo PDF válido.');
    }
});

function handleFileSelect(event) {
    const file = event.target.files[0];
    if (file && file.type === 'application/pdf') {
        handleFile(file);
    } else {
        alert('Por favor, selecione um arquivo PDF válido.');
    }
}

function handleFile(file) {
    selectedFile = file;
    
    document.getElementById('fileName').textContent = file.name;
    document.getElementById('fileDetails').textContent = 
        `Tamanho: ${(file.size / 1024 / 1024).toFixed(2)} MB | Tipo: ${file.type}`;
    document.getElementById('selectedFile').style.display = 'block';
}

// Funções para controle do loading e progresso
function showLoading(message = 'Processando...') {
    const loading = document.getElementById('loading');
    const loadingText = document.getElementById('loadingText');
    
    loadingText.textContent = message;
    loading.style.display = 'block';
    
    // Rola para o loading se necessário
    loading.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

function hideLoading() {
    const loading = document.getElementById('loading');
    loading.style.display = 'none';
    
    // Reset progress
    resetProgress();
}

function showProgressiveLoading() {
    showLoading('Iniciando processamento...');
    resetProgress();
}

function updateProgress(step, message, percentage) {
    // Atualiza etapa ativa
    document.querySelectorAll('.progress-step').forEach((stepEl, index) => {
        stepEl.classList.remove('active', 'completed');
        if (index + 1 < step) {
            stepEl.classList.add('completed');
        } else if (index + 1 === step) {
            stepEl.classList.add('active');
        }
    });
    
    // Atualiza barra de progresso
    const progressFill = document.getElementById('progressFill');
    const progressText = document.getElementById('progressText');
    const loadingText = document.getElementById('loadingText');
    
    progressFill.style.width = percentage + '%';
    progressText.textContent = message;
    loadingText.textContent = message;
}

function resetProgress() {
    document.querySelectorAll('.progress-step').forEach(step => {
        step.classList.remove('active', 'completed');
    });
    
    const progressFill = document.getElementById('progressFill');
    const progressText = document.getElementById('progressText');
    
    progressFill.style.width = '0%';
    progressText.textContent = 'Preparando...';
}

function showResult(title, data, type = 'success') {
    const results = document.getElementById('results');
    const sectionClass = type === 'success' ? 'result-section' : 'error-section';
    
    let html = `
        <div class="${sectionClass}">
            <h3>${title}</h3>
    `;
    
    if (type === 'success') {
        if (data.extracted_tables) {
            html += '<h4>📊 Tabelas Extraídas:</h4>';
            data.extracted_tables.forEach(table => {
                html += `
                    <div style="margin: 15px 0; padding: 15px; background: white; border-radius: 8px;">
                        <h5>${table.table_name}</h5>
                        <div style="overflow-x: auto;">
                            <table style="width: 100%; border-collapse: collapse;">
                                <thead>
                                    <tr style="background: #f8f9fa;">
                `;
                
                if (table.data && table.data.length > 0) {
                    Object.keys(table.data[0]).forEach(key => {
                        html += `<th style="border: 1px solid #dee2e6; padding: 8px; background: #e9ecef; color: #212529; font-weight: bold;">${key}</th>`;
                    });
                    html += '</tr></thead><tbody>';
                    
                    table.data.forEach((row, index) => {
                        const rowColor = index % 2 === 0 ? '#ffffff' : '#f8f9fa';
                        html += `<tr style="background: ${rowColor};">`;
                        Object.values(row).forEach(value => {
                            html += `<td style="border: 1px solid #dee2e6; padding: 8px; color: #495057; background: ${rowColor};">${value || ''}</td>`;
                        });
                        html += '</tr>';
                    });
                }
                
                html += '</tbody></table></div></div>';
            });
        }
        
        if (data.filename) {
            html += `<p><strong>📄 Arquivo:</strong> ${data.filename}</p>`;
        }
        
        // Exibe informações sobre tabelas deltas geradas
        if (data.delta_tables_generated && data.delta_tables_generated > 0) {
            html += `
                <div style="margin: 20px 0; padding: 15px; background: #e7f3ff; border-radius: 8px; border-left: 4px solid #007bff;">
                    <h4 style="color: #0056b3; margin-top: 0;">🗂️ Tabelas Delta Geradas (${data.delta_tables_generated})</h4>
                    <p style="margin-bottom: 10px;">As seguintes tabelas em formato Delta (Parquet) foram geradas automaticamente:</p>
            `;
            
            if (data.delta_files) {
                for (const [deltaType, deltaPath] of Object.entries(data.delta_files)) {
                    const deltaName = deltaType.replace('delta_', '').replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                    const folderName = deltaPath.split('/').pop();
                    html += `
                        <div style="margin: 8px 0; padding: 10px; background: white; border-radius: 4px; border-left: 3px solid #28a745;">
                            <strong style="color: #28a745;">📊 ${deltaName}:</strong> ${folderName}
                            <div style="margin-top: 5px; font-size: 0.9em; color: #666;">
                                <span style="background: #f8f9fa; padding: 2px 6px; border-radius: 3px; margin-right: 8px;">
                                    📁 Formato: Delta (Parquet + Log)
                                </span>
                                <span style="background: #e9ecef; padding: 2px 6px; border-radius: 3px;">
                                    📍 Local: ${deltaPath}
                                </span>
                            </div>
                        </div>
                    `;
                }
            }
            
            html += `
                    <div style="margin-top: 15px; padding: 10px; background: #f8f9fa; border-radius: 6px;">
                        <h5 style="margin: 0 0 8px 0; color: #495057;">💡 Sobre o Formato Delta:</h5>
                        <ul style="margin: 0; padding-left: 20px; color: #6c757d; font-size: 0.9em;">
                            <li><strong>Parquet:</strong> Formato colunar otimizado para análise</li>
                            <li><strong>Log de Transações:</strong> Versionamento e auditoria de mudanças</li>
                            <li><strong>Metadados:</strong> Schema, estatísticas e histórico</li>
                            <li><strong>Compatibilidade:</strong> Pode ser lido por Pandas, Spark, etc.</li>
                        </ul>
                    </div>
                    
                    <div style="margin-top: 10px; text-align: center;">
                        <button onclick="loadDeltaTables()" style="background: #17a2b8; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer;">
                            🔍 Ver Tabelas Delta
                        </button>
                    </div>
                </div>
            `;
        }
        
        if (data.processing_time) {
            html += `<p><strong>⏱️ Tempo de processamento:</strong> ${data.processing_time}s</p>`;
        }
    } else {
        html += `<p style="color: #721c24;"><strong>Erro:</strong> ${data.detail || 'Erro desconhecido'}</p>`;
    }
    
    html += '</div>';
    results.innerHTML = html;
    
    // Rola para o resultado
    results.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function resetUploadInterface() {
    selectedFile = null;
    document.getElementById('selectedFile').style.display = 'none';
    document.getElementById('fileInput').value = '';
}

function resetCompleteInterface() {
    // Reset upload section
    resetUploadInterface();
    
    // Reset extraction section
    selectedPdfForExtraction = null;
    document.getElementById('existingFiles').innerHTML = 
        '<button class="upload-btn" onclick="loadExistingFiles()">🔄 Carregar PDFs Disponíveis</button>';
    document.getElementById('tableOptions').style.display = 'none';
    
    // Reset custom section
    selectedPdfForCustom = null;
    document.getElementById('customPdfsList').innerHTML = 
        '<p style="color: #6c757d;">Clique em "Carregar PDFs" para selecionar um arquivo para extração customizada.</p>';
    document.getElementById('customDescription').value = '';
    document.getElementById('customPrompt').value = '';
    
    // Clear results
    document.getElementById('results').innerHTML = '';
    
    // Hide loading
    hideLoading();
    
    console.log('Interface completamente resetada');
}

// Função auxiliar para carregar arquivos sem mostrar loading (para recarregamentos automáticos)
async function loadExistingFilesQuiet() {
    try {
        const response = await fetch('/available-pdfs');
        const result = await response.json();
        
        if (response.ok && result.available_files && result.available_files.length > 0) {
            displayFileList(result.available_files);
        }
    } catch (error) {
        console.log('Erro silencioso ao recarregar arquivos:', error);
    }
}

async function uploadFile() {
    console.log('🔍 DEBUG: uploadFile() chamada');
    console.log('🔍 DEBUG: selectedFile =', selectedFile);
    
    if (!selectedFile) {
        console.log('❌ DEBUG: Nenhum arquivo selecionado');
        alert('Selecione um arquivo PDF primeiro.');
        return;
    }
    
    console.log('✅ DEBUG: Arquivo válido, iniciando upload...');
    
    // Inicia a barra de progresso com etapas
    showProgressiveLoading();
    
    console.log('🔍 DEBUG: Criando FormData...');
    const formData = new FormData();
    formData.append('file', selectedFile);
    console.log('🔍 DEBUG: FormData criado:', formData);
    
    try {
        // Etapa 1: Upload
        updateProgress(1, 'Enviando arquivo para o servidor...', 20);
        
        console.log('🔍 DEBUG: Enviando requisição...');
        const response = await fetch('/upload-pdf', {
            method: 'POST',
            body: formData
        });
        console.log('🔍 DEBUG: Resposta recebida:', response.status);
        
        const result = await response.json();
        
        if (response.ok) {
            console.log('✅ DEBUG: Upload aceito, iniciando polling de status...');
            
            // Se temos task_id, inicia polling de status
            if (result.task_id) {
                await pollProcessingStatus(result.task_id, selectedFile.name);
            } else {
                // Fallback para resposta imediata (compatibilidade)
                updateProgress(5, 'Processamento concluído!', 100);
                await new Promise(resolve => setTimeout(resolve, 1000));
                
                showResult('Upload realizado com sucesso!', result, 'success');
                
                setTimeout(() => {
                    hideLoading();
                    resetUploadInterface();
                    loadExistingFilesQuiet();
                }, 2000);
            }
        } else {
            hideLoading();
            showResult('Erro no upload', result, 'error');
            resetUploadInterface();
        }
    } catch (error) {
        hideLoading();
        showResult('Erro de conexão', {detail: error.message}, 'error');
        resetUploadInterface();
    }
}

async function pollProcessingStatus(taskId, filename) {
    console.log('🔍 DEBUG: Iniciando polling para task:', taskId);
    
    if (!taskId || taskId === 'unknown' || taskId === null) {
        console.error('❌ DEBUG: Task ID inválido:', taskId);
        hideLoading();
        showResult('Erro no upload', {
            detail: 'Task ID não recebido do backend'
        }, 'error');
        resetUploadInterface();
        return;
    }
    
    const maxAttempts = 60; // 5 minutos máximo (5s * 60)
    let attempts = 0;
    
    const poll = async () => {
        try {
            attempts++;
            console.log(`🔍 DEBUG: Polling tentativa ${attempts}/${maxAttempts} para task ${taskId}`);
            
            const response = await fetch(`/upload-status/${taskId}`);
            const statusData = await response.json();
            
            if (response.ok) {
                console.log('🔍 DEBUG: Status recebido:', statusData);
                
                // Atualiza progresso baseado no status
                let progress = statusData.progress || 0;
                let message = statusData.message || 'Processando...';
                
                // Verifica se foi concluído (múltiplas formas de verificar)
                const isCompleted = statusData.status === 'completed' || 
                                  statusData.is_completed === true ||
                                  progress >= 100;
                
                const isError = statusData.status === 'error' || 
                               statusData.is_error === true;
                
                console.log(`🔍 DEBUG: isCompleted=${isCompleted}, isError=${isError}, status=${statusData.status}, progress=${progress}`);
                
                // Mapeia status para etapas da interface
                if (statusData.status === 'pending') {
                    updateProgress(2, message, Math.max(progress, 30));
                } else if (statusData.status === 'processing') {
                    updateProgress(3, message, Math.max(progress, 50));
                } else if (isCompleted) {
                    console.log('🎉 DEBUG: Processamento concluído!');
                    updateProgress(5, 'Processamento concluído!', 100);
                    
                    // Mostra resultado final
                    if (statusData.result) {
                        showResult('Upload realizado com sucesso!', statusData.result, 'success');
                    } else {
                        showResult('Upload realizado com sucesso!', {
                            filename: filename,
                            message: message || 'PDF processado com sucesso!'
                        }, 'success');
                    }
                    
                    setTimeout(() => {
                        hideLoading();
                        resetUploadInterface();
                        loadExistingFilesQuiet();
                    }, 2000);
                    
                    return; // Para o polling
                } else if (isError) {
                    console.error('❌ DEBUG: Erro no processamento:', statusData);
                    hideLoading();
                    showResult('Erro no processamento', {
                        detail: statusData.message || 'Erro desconhecido'
                    }, 'error');
                    resetUploadInterface();
                    return; // Para o polling
                }
                
                // Continua polling se não terminou
                if (attempts < maxAttempts && !isCompleted && !isError) {
                    console.log(`🔄 DEBUG: Continuando polling em 3 segundos...`);
                    setTimeout(poll, 3000); // Polling a cada 3 segundos (mais rápido)
                } else if (attempts >= maxAttempts) {
                    // Timeout
                    console.warn('⏰ DEBUG: Timeout no polling');
                    hideLoading();
                    showResult('Timeout no processamento', {
                        detail: 'O processamento está demorando muito. Verifique o status mais tarde.'
                    }, 'error');
                    resetUploadInterface();
                }
            } else {
                console.log('❌ DEBUG: Erro no polling:', response.status, response.statusText);
                
                if (attempts < maxAttempts) {
                    console.log(`🔄 DEBUG: Tentando novamente em 3 segundos...`);
                    setTimeout(poll, 3000); // Tenta novamente
                } else {
                    hideLoading();
                    showResult('Erro no monitoramento', {
                        detail: 'Não foi possível monitorar o processamento'
                    }, 'error');
                    resetUploadInterface();
                }
            }
        } catch (error) {
            console.error('❌ DEBUG: Erro na requisição de polling:', error);
            
            if (attempts < maxAttempts) {
                console.log(`🔄 DEBUG: Erro de conexão, tentando novamente em 3 segundos...`);
                setTimeout(poll, 3000); // Tenta novamente
            } else {
                console.error('❌ DEBUG: Máximo de tentativas excedido');
                hideLoading();
                showResult('Erro de conexão', {detail: error.message}, 'error');
                resetUploadInterface();
            }
        }
    };
    
    // Inicia o polling
    poll();
}

async function loadExistingFiles() {
    showLoading('Carregando PDFs disponíveis...');
    
    try {
        const response = await fetch('/available-pdfs');
        const result = await response.json();
        
        if (response.ok && result.available_files && result.available_files.length > 0) {
            displayFileList(result.available_files);
        } else {
            document.getElementById('existingFiles').innerHTML = 
                '<button class="upload-btn" onclick="loadExistingFiles()">🔄 Carregar PDFs Disponíveis</button>' +
                '<p style="margin-top: 10px; color: #6c757d;">Nenhum PDF encontrado. Faça upload de um PDF primeiro.</p>';
        }
        hideLoading();
    } catch (error) {
        hideLoading();
        document.getElementById('existingFiles').innerHTML = 
            '<button class="upload-btn" onclick="loadExistingFiles()">🔄 Carregar PDFs Disponíveis</button>' +
            '<p style="margin-top: 10px; color: #dc3545;">Erro ao carregar arquivos: ' + error.message + '</p>';
    }
}

function displayFileList(files) {
    let html = '<h4>PDFs Disponíveis:</h4>';
    
    files.forEach(file => {
        html += `
            <div class="file-item">
                <div class="file-info">
                    <div class="file-name">${file.filename}</div>
                    <div class="file-details">
                        ${file.total_pages} páginas | ${file.total_chunks} chunks | 
                        Upload: ${new Date(file.upload_date).toLocaleString('pt-BR')}
                    </div>
                </div>
                <button class="extract-btn" onclick="selectFileForExtraction('${file.filename}')">
                    Selecionar
                </button>
            </div>
        `;
    });
    
    document.getElementById('existingFiles').innerHTML = html;
}

function selectFileForExtraction(filename) {
    selectedPdfForExtraction = filename;
    
    // Destaca arquivo selecionado
    document.querySelectorAll('.file-item').forEach(item => {
        item.style.backgroundColor = '#f8f9fa';
    });
    
    event.target.parentElement.parentElement.style.backgroundColor = '#d4edda';
    
    // Mostra opções de tabela
    document.getElementById('tableOptions').style.display = 'block';
}

async function extractTables() {
    alert('⚠️ Funcionalidade em desenvolvimento. Use o sistema de chat para consultar os PDFs após o upload.');
    return;
    
    if (!selectedPdfForExtraction) {
        alert('Selecione um PDF primeiro.');
        return;
    }
    
    const selectedTables = [];
    document.querySelectorAll('.table-option input:checked').forEach(checkbox => {
        selectedTables.push(checkbox.value);
    });
    
    if (selectedTables.length === 0) {
        alert('Selecione pelo menos um tipo de tabela.');
        return;
    }
    
    showLoading('Extraindo tabelas com IA...');
    
    try {
        const response = await fetch('/extract-tables-from-existing', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                filename: selectedPdfForExtraction,
                target_tables: selectedTables
            })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showResult('Tabelas extraídas com sucesso!', result, 'success');
        } else {
            showResult('Erro na extração', result, 'error');
        }
        hideLoading();
    } catch (error) {
        hideLoading();
        showResult('Erro de conexão', {detail: error.message}, 'error');
    }
}

// Função para carregar PDFs armazenados (atualizada para usar dados do backend)
async function loadStoredPdfs() {
    showLoading('Carregando PDFs armazenados no DynamoDB...');
    
    try {
        // Obter user_id do usuário autenticado (passado pelo template)
        const userId = '{{ user.email if user else "user_default_001" }}';
        
        // Faz uma nova requisição para garantir dados atualizados
        const response = await fetch(`http://backend-service:8000/pdfs?user_id=${encodeURIComponent(userId)}`);
        const result = await response.json();
        
        if (response.ok && result.pdfs && result.pdfs.length > 0) {
            displayStoredPdfs(result.pdfs);
        } else {
            document.getElementById('storedPdfsList').innerHTML = 
                '<p style="color: #6c757d;">Nenhum PDF encontrado no DynamoDB para este usuário.</p>';
        }
        hideLoading();
    } catch (error) {
        hideLoading();
        document.getElementById('storedPdfsList').innerHTML = 
            '<p style="color: #dc3545;">Erro ao carregar PDFs: ' + error.message + '</p>';
    }
}

function displayStoredPdfs(pdfs) {
    let html = '';
    
    if (pdfs.length === 0) {
        html = '<p style="color: #6c757d;">Nenhum PDF encontrado no DynamoDB.</p>';
    } else {
        html = `
            <div style="margin-bottom: 15px;">
                <h4>📁 PDFs Armazenados no DynamoDB (${pdfs.length} arquivos)</h4>
            </div>
        `;
        
        pdfs.forEach(pdf => {
            const pdfName = pdf.pdf_name || 'Nome não disponível';
            const wordCount = pdf.word_count || 0;
            
            html += `
                <div class="file-item" style="margin-bottom: 15px; padding: 15px; background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px;">
                    <div class="file-info" style="flex: 1;">
                        <div class="file-name" style="display: flex; align-items: center; gap: 8px;">
                            <span style="color: #28a745;">📄</span>
                            <strong>${pdfName}</strong>
                        </div>
                        <div class="file-details" style="margin-top: 5px; color: #6c757d;">
                            💬 Total de palavras: ${wordCount.toLocaleString('pt-BR')}
                        </div>
                    </div>
                </div>
            `;
        });
    }
    
    document.getElementById('storedPdfsList').innerHTML = html;
}

// Utility functions
function formatBytes(bytes) {
    if (!bytes || bytes === 'N/A') return 'N/A';
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    if (bytes === 0) return '0 Bytes';
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
}

function formatDate(dateString) {
    if (!dateString || dateString === 'N/A') return 'N/A';
    try {
        // Se for timestamp em milissegundos
        if (typeof dateString === 'number') {
            return new Date(dateString).toLocaleString('pt-BR');
        }
        // Se for string ISO
        return new Date(dateString).toLocaleString('pt-BR');
    } catch (e) {
        return dateString;
    }
}

// Função para fechar o card de dados da tabela
function hideDeltaTableData() {
    const deltaTableDataSection = document.getElementById('deltaTableDataSection');
    deltaTableDataSection.style.display = 'none';
    deltaTableDataSection.innerHTML = '';
}

// Inicialização automática quando a página carrega
window.addEventListener('DOMContentLoaded', function() {
    console.log("🚀 Página carregada, inicializando PDF processor...");
    
    // Inicializa sidebar
    const sidebar = document.getElementById('sidebar');
    const title = sidebar.querySelector('.sidebar-title');
    const isCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
    
    if (isCollapsed) {
        sidebar.classList.add('collapsed');
        title.style.display = 'none';
    } else {
        sidebar.classList.remove('collapsed');
        title.style.display = 'flex';
    }
});
