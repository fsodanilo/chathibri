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

function toggleResposta(item) {
    item.classList.toggle("expanded");
}

function toggleSection(button) {
    const section = button.nextElementSibling;
    section.classList.toggle("expanded");
}

// Initialize sidebar state on page load
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

document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll(".section-body").forEach(body => {
        body.classList.remove("expanded");
    });
    
    // Configura o sistema de loading no chat
    setupChatLoadingSystem();
});

// Sistema de Loading para o Chat
function setupChatLoadingSystem() {
    const chatForm = document.getElementById('chat-form');
    const processingIndicator = document.getElementById('processing-indicator');
    const submitBtn = document.getElementById('submit-btn');
    const questionInput = document.getElementById('question');
    
    if (chatForm) {
        chatForm.addEventListener('submit', function(e) {
            const question = questionInput.value.trim();
            const questionBackup = document.getElementById('question-backup');
            
            // Validação mais rigorosa
            if (!question || question.length < 2) {
                e.preventDefault();
                
                // Mostra erro visual
                questionInput.style.borderColor = '#f44336';
                questionInput.placeholder = 'Por favor, digite uma pergunta válida...';
                questionInput.focus();
                
                // Remove o erro após 3 segundos
                setTimeout(() => {
                    questionInput.style.borderColor = '#2c2c38';
                    questionInput.placeholder = 'Faça uma pergunta sobre os PDFs enviados...';
                }, 3000);
                
                return;
            }
            
            console.log('📤 Enviando pergunta:', question);
            
            // GARANTIR que o valor esteja preservado em múltiplos campos
            questionInput.value = question;
            questionBackup.value = question;
            
            // Log para debug
            console.log('🔍 DEBUG: questionInput.value =', questionInput.value);
            console.log('🔍 DEBUG: questionBackup.value =', questionBackup.value);
            
            // Mostra indicador de loading
            showProcessingIndicator();
            
            // Desabilita o botão (mas não o input ainda)
            submitBtn.disabled = true;
            
            // Muda visual do botão
            submitBtn.style.opacity = '0.5';
            submitBtn.innerHTML = '⏳';
            
            // Adiciona a pergunta do usuário imediatamente ao chat
            addUserMessageToChat(question);
            
            // Agora desabilita o input após adicionar ao chat
            questionInput.disabled = true;
            questionInput.placeholder = 'Processando sua pergunta...';
            
            // Scroll para baixo
            scrollToBottom();
            
            // Timeout de segurança (30 segundos)
            setTimeout(() => {
                if (processingIndicator && processingIndicator.style.display === 'block') {
                    console.warn('⏰ Timeout do processamento');
                    hideProcessingIndicator();
                    addErrorMessageToChat('⏰ A resposta está demorando muito. Tente novamente.');
                }
            }, 30000);
        });
    }
}

function showProcessingIndicator() {
    const indicator = document.getElementById('processing-indicator');
    if (indicator) {
        indicator.style.display = 'block';
        scrollToBottom();
    }
}

function hideProcessingIndicator() {
    const indicator = document.getElementById('processing-indicator');
    const submitBtn = document.getElementById('submit-btn');
    const questionInput = document.getElementById('question');
    
    if (indicator) {
        indicator.style.display = 'none';
    }
    
    // Reabilita os controles
    if (submitBtn) {
        submitBtn.disabled = false;
        submitBtn.style.opacity = '1';
        submitBtn.innerHTML = '🎤';
    }
    
    if (questionInput) {
        questionInput.disabled = false;
        questionInput.placeholder = 'Faça uma pergunta sobre os PDFs enviados...';
        questionInput.focus();
    }
}

function addUserMessageToChat(message) {
    const chatHistory = document.querySelector('.chat-history');
    const processingIndicator = document.getElementById('processing-indicator');
    
    // Cria elemento da mensagem do usuário
    const userMessage = document.createElement('div');
    userMessage.className = 'chat-message user new-message';
    userMessage.innerHTML = `
        <div class="bubble user-bubble">${escapeHtml(message)}</div>
    `;
    
    // Insere antes do indicador de processamento
    chatHistory.insertBefore(userMessage, processingIndicator);
    
    // Remove a classe de animação após um tempo
    setTimeout(() => {
        userMessage.classList.remove('new-message');
    }, 500);
}

function addErrorMessageToChat(errorMessage) {
    const chatHistory = document.querySelector('.chat-history');
    const processingIndicator = document.getElementById('processing-indicator');
    
    // Cria elemento da mensagem de erro
    const errorMsg = document.createElement('div');
    errorMsg.className = 'chat-message bot new-message';
    errorMsg.innerHTML = `
        <div class="bubble bot-bubble">
            <div style="color: #ea4335; line-height: 1.6; padding: 16px 20px; background: rgba(234, 67, 53, 0.1); border-radius: 18px;">
                ${escapeHtml(errorMessage)}
            </div>
        </div>
    `;
    
    // Insere antes do indicador de processamento
    chatHistory.insertBefore(errorMsg, processingIndicator);
    
    // Remove a classe de animação após um tempo
    setTimeout(() => {
        errorMsg.classList.remove('new-message');
    }, 500);
    
    scrollToBottom();
}

function addBotMessageToChat(message, messageId = null) {
    const chatHistory = document.querySelector('.chat-history');
    const processingIndicator = document.getElementById('processing-indicator');
    
    // Cria elemento da mensagem do bot
    const botMessage = document.createElement('div');
    botMessage.className = 'chat-message bot new-message';
    botMessage.innerHTML = `
        <div class="bubble bot-bubble">
            <div class="message-content" style="color: #e3e3e3; line-height: 1.6;">
                ${escapeHtml(message)}
            </div>
            <div class="feedback-section" data-message-id="${messageId || Date.now()}">
                <div class="feedback-buttons">
                    <button class="feedback-btn like-btn" onclick="showFeedbackForm(this, 0)" title="Resposta útil">
                        <div class="icon thumb-up"></div>
                    </button>
                    <button class="feedback-btn dislike-btn" onclick="showFeedbackForm(this, 1)" title="Resposta não útil">
                        <div class="icon thumb-down"></div>
                    </button>
                </div>
                <div class="feedback-form" style="display: none;">
                    <textarea placeholder="Comentário (opcional)..." maxlength="500"></textarea>
                    <div class="feedback-form-buttons">
                        <button onclick="submitFeedback(this)" class="submit-feedback">Enviar</button>
                        <button onclick="cancelFeedback(this)" class="cancel-feedback">Cancelar</button>
                    </div>
                </div>
                <div class="feedback-status" style="display: none;"></div>
            </div>
        </div>
    `;
    
    // Insere antes do indicador de processamento
    chatHistory.insertBefore(botMessage, processingIndicator);
    
    // Remove a classe de animação após um tempo
    setTimeout(() => {
        botMessage.classList.remove('new-message');
    }, 500);
    
    scrollToBottom();
}

function scrollToBottom() {
    const chatHistory = document.querySelector('.chat-history');
    if (chatHistory) {
        setTimeout(() => {
            chatHistory.scrollTop = chatHistory.scrollHeight;
        }, 100);
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Detecta quando a página carrega após submissão do formulário
window.addEventListener('load', function() {
    // Se o indicador estiver visível, esconde após a resposta chegar
    hideProcessingIndicator();
    
    // Limpa o input apenas quando a página recarrega após submissão bem-sucedida
    const questionInput = document.getElementById('question');
    if (questionInput) {
        questionInput.value = '';
    }
    
    // Adiciona animação às mensagens novas
    const lastBotMessage = document.querySelector('.chat-message.bot:last-of-type');
    if (lastBotMessage) {
        lastBotMessage.classList.add('new-message');
        setTimeout(() => {
            lastBotMessage.classList.remove('new-message');
        }, 500);
    }
    
    // Remove mensagens de erro após 5 segundos
    const errorMessages = document.querySelectorAll('.error-message');
    errorMessages.forEach(msg => {
        setTimeout(() => {
            msg.style.opacity = '0';
            setTimeout(() => {
                if (msg.parentNode) {
                    msg.parentNode.removeChild(msg);
                }
            }, 300);
        }, 5000);
    });
    
    scrollToBottom();
});

// Sistema de Feedback
function showFeedbackForm(button, feedbackType) {
    const feedbackSection = button.closest('.feedback-section');
    const buttons = feedbackSection.querySelectorAll('.feedback-btn');
    const form = feedbackSection.querySelector('.feedback-form');
    const status = feedbackSection.querySelector('.feedback-status');
    
    // Remove seleções anteriores
    buttons.forEach(btn => btn.classList.remove('active'));
    
    // Marca o botão selecionado
    button.classList.add('active');
    
    // Armazena o tipo de feedback
    form.dataset.feedbackType = feedbackType;
    
    // Mostra o formulário
    form.style.display = 'block';
    status.style.display = 'none';
    
    // Foca no textarea
    const textarea = form.querySelector('textarea');
    textarea.focus();
}

function cancelFeedback(button) {
    const feedbackSection = button.closest('.feedback-section');
    const buttons = feedbackSection.querySelectorAll('.feedback-btn');
    const form = feedbackSection.querySelector('.feedback-form');
    const textarea = form.querySelector('textarea');
    
    // Remove seleções
    buttons.forEach(btn => btn.classList.remove('active'));
    
    // Limpa e esconde o formulário
    textarea.value = '';
    form.style.display = 'none';
}

async function submitFeedback(button) {
    const feedbackSection = button.closest('.feedback-section');
    const form = feedbackSection.querySelector('.feedback-form');
    const status = feedbackSection.querySelector('.feedback-status');
    const textarea = form.querySelector('textarea');
    const messageId = feedbackSection.dataset.messageId;
    const feedbackType = parseInt(form.dataset.feedbackType);
    const comment = textarea.value.trim();
    
    // Desabilita o botão durante o envio
    button.disabled = true;
    button.textContent = 'Enviando...';
    
    try {
        const response = await fetch('/feedback', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                message_id: messageId,
                feedback_type: feedbackType,
                comment: comment
            })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            // Sucesso
            status.className = 'feedback-status success';
            status.textContent = '✅ Obrigado pelo seu feedback!';
            status.style.display = 'block';
            
            // Esconde o formulário
            form.style.display = 'none';
            
            // Desabilita os botões de feedback
            const buttons = feedbackSection.querySelectorAll('.feedback-btn');
            buttons.forEach(btn => {
                btn.disabled = true;
                btn.style.opacity = '0.5';
            });
            
        } else {
            throw new Error(result.message || 'Erro ao enviar feedback');
        }
        
    } catch (error) {
        console.error('Erro ao enviar feedback:', error);
        status.className = 'feedback-status error';
        status.textContent = '❌ Erro ao enviar feedback. Tente novamente.';
        status.style.display = 'block';
    } finally {
        // Restaura o botão
        button.disabled = false;
        button.textContent = 'Enviar';
    }
}

// Atalhos de teclado para o feedback
document.addEventListener('keydown', function(event) {
    // Se estiver digitando em um textarea de feedback
    if (event.target.tagName === 'TEXTAREA' && event.target.closest('.feedback-form')) {
        // Ctrl + Enter para enviar
        if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
            event.preventDefault();
            const submitBtn = event.target.closest('.feedback-form').querySelector('.submit-feedback');
            if (submitBtn && !submitBtn.disabled) {
                submitFeedback(submitBtn);
            }
        }
        // Escape para cancelar
        else if (event.key === 'Escape') {
            event.preventDefault();
            const cancelBtn = event.target.closest('.feedback-form').querySelector('.cancel-feedback');
            if (cancelBtn) {
                cancelFeedback(cancelBtn);
            }
        }
    }
});
