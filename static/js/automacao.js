document.addEventListener('DOMContentLoaded', () => {
    // --- Variáveis de Estado ---
    let isSapLoggedIn = false;
    let isBwLoggedIn = false;
    let isSalesforceLoggedIn = false;
    let currentTaskInfo = null;
    let statusTimeout;
    let currentHubUser = null; // <-- ADICIONE ESTA LINHA
    
    let savedConnections = {}; // Será preenchido com { sap: {...}, bw: {...}, salesforce: {...} }

    // --- Variáveis do Agendador ---
    let jobQueue = []; 
    let jobHistory = [];
    let schedulerInterval = null; 
    let isSchedulerRunning = false; 

    let schedulerAllTasks = []; // Armazena todas as tarefas disponíveis
    let currentTaskPage = 0; // Página atual da seleção de tarefas
    const TASKS_PER_PAGE = 3; // O limite de 3 botões visíveis

    // --- Seletores dos Elementos ---
    const statusBox = document.getElementById('status');
    const systemRadios = document.querySelectorAll('input[name="login_system"]');
    const sapTasksSection = document.getElementById('sap-tasks-section');
    const bwTasksSection = document.getElementById('bw-tasks-section');
    const salesforceTasksSection = document.getElementById('salesforce-tasks-section');
    const allTaskButtons = document.querySelectorAll('.task-button');
    const logoutBtn = document.getElementById('logout-btn');

    // --- Seletores do Modal de Login ---
    const modalOverlay = document.getElementById('login-modal-overlay');
    const modalUser = document.getElementById('modal-user');
    const modalPass = document.getElementById('modal-pass');
    const modalExecuteBtn = document.getElementById('modal-execute-btn');
    const modalLoginCloseBtn = document.getElementById('login-modal-close-btn');
    const modalSaveConnBtn = document.getElementById('modal-save-conn-btn'); 
    const modalLogoSap = document.getElementById('modal-logo-sap');
    const modalLogoBw = document.getElementById('modal-logo-bw');
    const modalLogoSalesforce = document.getElementById('modal-logo-salesforce');

    // --- Seletores do Modal de Confirmação ---
    const confirmModalOverlay = document.getElementById('confirm-modal-overlay');
    const confirmModalCloseBtn = document.getElementById('confirm-modal-close-btn');
    const confirmModalYesBtn = document.getElementById('confirm-modal-yes-btn');
    const confirmModalNoBtn = document.getElementById('confirm-modal-no-btn');

    const schedulerBtn = document.getElementById('scheduler-btn');

    // --- Seletores do Preview Panel (Req 1) ---
    const previewPanel = document.getElementById('preview-panel');
    const previewImage = document.getElementById('preview-image');
    const previewDescription = document.getElementById('preview-description');
    const previewTagsContainer = document.getElementById('preview-tags-container');
    
    // --- Funções de Feedback (Status Box) ---
    function showStatus(message, type = 'processing', sticky = false) {
        clearTimeout(statusTimeout);
        statusBox.className = `status-box ${type} visible`;
        statusBox.textContent = message;

        if (type === 'success' || type === 'error') {
            const delay = sticky ? 15000 : (type === 'success' ? 3000 : 5000);
            statusTimeout = setTimeout(() => {
                statusBox.classList.remove('visible');
            }, delay);
        }
    }

    function setProcessing(message, isSchedulerJob = false) {
        if (!isSchedulerJob) {
            allTaskButtons.forEach(btn => btn.classList.add('disabled'));
            if(modalExecuteBtn) modalExecuteBtn.disabled = true;
            if(modalSaveConnBtn) modalSaveConnBtn.disabled = true;
            if(modalLoginCloseBtn) modalLoginCloseBtn.disabled = true; 
            if(logoutBtn) logoutBtn.disabled = true;
            if(schedulerBtn) schedulerBtn.disabled = true;
        }
        
        showStatus(message, 'processing');
    }
    
    function enableUI() {
        allTaskButtons.forEach(btn => btn.classList.remove('disabled'));
        if (modalExecuteBtn) modalExecuteBtn.disabled = false;
        if (modalSaveConnBtn) modalSaveConnBtn.disabled = false;
        if (modalLoginCloseBtn) modalLoginCloseBtn.disabled = false; 
        if (logoutBtn) logoutBtn.disabled = false;
        if(schedulerBtn) schedulerBtn.disabled = false;
    }
    
    function handleFetchError(error, isSchedulerJob = false) {
        if (!isSchedulerJob) {
            enableUI();
        }
        showStatus('Erro de comunicação com o servidor.', 'error', true);
        console.error('Erro de Fetch:', error);
        
        if (isSchedulerJob) {
            const job = jobQueue.find(j => j.status === 'running');
            if (job) {
                job.status = 'failed';
                jobHistory.unshift(job);
                jobHistory = jobHistory.slice(0, 4); 
                jobQueue = jobQueue.filter(j => j.id !== job.id);
                renderJobQueue();
                renderJobHistory();
                saveScheduleToServer(); // <-- ADICIONE ESTA LINHA
            }
            isSchedulerRunning = false;
        }
    }
    
    function handleTaskResult(data, isSchedulerJob = false) {
        if (!isSchedulerJob) {
            enableUI();
        }
        
        if (data.status === 'sucesso') {
            showStatus(data.mensagem, 'success');
            
            if (data.download_file && currentTaskInfo && currentTaskInfo.name === "Base Mãe") {
                const downloadLink = document.querySelector('.task-button-download');
                if (downloadLink) {
                    downloadLink.href = '/download/' + data.download_file;
                    downloadLink.classList.remove('inactive');
                    downloadLink.title = `Baixar ${data.download_file}`;
                    showStatus("Sucesso! O download do relatório iniciará em 3 segundos...", 'success', true);
                    setTimeout(() => {
                        window.location.href = downloadLink.href;
                    }, 3000);
                }
            }
        } else {
            showStatus('ERRO: ' + data.mensagem, 'error', true);
        }
        
        if (isSchedulerJob) {
            const job = jobQueue.find(j => j.status === 'running');
            if (job) {
                job.status = (data.status === 'sucesso') ? 'done' : 'failed';
                jobHistory.unshift(job);
                jobHistory = jobHistory.slice(0, 4);
                jobQueue = jobQueue.filter(j => j.id !== job.id);
                renderJobQueue();
                renderJobHistory();
                saveScheduleToServer(); // <-- ADICIONE ESTA LINHA
            }
            isSchedulerRunning = false;
        }
    }

    function injectSchedulerHTML() {
        const schedulerModal = document.createElement('div');
        schedulerModal.id = 'scheduler-modal-overlay';
        schedulerModal.className = 'settings-modal-overlay';
        schedulerModal.innerHTML = `
            <div class="settings-modal scheduler-modal-panel">
                <div class="settings-body scheduler-modal-body">
                    <nav class="profile-nav-tabs scheduler-nav-tabs" role="tablist" aria-label="Seções do agendador">
                        <button type="button" id="scheduler-tab-btn-tasks" class="profile-nav-link scheduler-tab active" data-tab="tasks">
                            <i class="fas fa-calendar-check"></i>
                            <span>Tarefas</span>
                        </button>
                        <button type="button" id="scheduler-tab-btn-queue" class="profile-nav-link scheduler-tab" data-tab="queue">
                            <i class="fas fa-list-check"></i>
                            <span>Fila</span>
                        </button>
                        <button type="button" id="scheduler-tab-btn-history" class="profile-nav-link scheduler-tab" data-tab="history">
                            <i class="fas fa-clock-rotate-left"></i>
                            <span>Histórico</span>
                        </button>
                    </nav>

                    <div class="scheduler-tab-panels">
                        <section id="scheduler-panel-tasks" class="scheduler-tab-panel active" data-tab="tasks" role="tabpanel" aria-labelledby="scheduler-tab-btn-tasks">
                            <div class="scheduler-form">
                                <div class="scheduler-datetime-group">
                                    <div class="modal-input-group">
                                        <label for="scheduler-date">Data:</label>
                                        <div class="input-with-icon-wrapper">
                                            <input type="text" id="scheduler-date" aria-label="Data para iniciar a tarefa" maxlength="10">
                                            <i class="fas fa-calendar-alt input-icon" id="scheduler-calendar-icon"></i>
                                            <input type="date" id="scheduler-date-native" class="scheduler-native-input">
                                        </div>
                                    </div>
                                    <div class="modal-input-group">
                                        <label for="scheduler-time">Hora:</label>
                                        <div class="input-with-icon-wrapper">
                                            <input type="text" id="scheduler-time" aria-label="Hora para iniciar a tarefa" maxlength="5">
                                            <i class="fas fa-clock input-icon" id="scheduler-clock-icon"></i>
                                            <input type="time" id="scheduler-time-native" class="scheduler-native-input">
                                        </div>
                                    </div>
                                </div>
                                <div class="modal-input-group">
                                    <label class="scheduler-task-selection-label">Selecione a automação:</label>
                                    <div id="scheduler-tasks-container" class="scheduler-tasks-container"></div>
                                </div>
                                <div class="scheduler-button-container">
                                    <button id="scheduler-add-btn" class="button btn-execute">Adicionar à Fila</button>
                                </div>
                            </div>
                        </section>

                        <section id="scheduler-panel-queue" class="scheduler-tab-panel" data-tab="queue" role="tabpanel" aria-labelledby="scheduler-tab-btn-queue" hidden>
                            <div class="scheduler-queue">
                                <div id="queue-container" class="queue-list-container">
                                    <ul id="scheduler-queue-list" aria-live="polite"></ul>
                                </div>
                            </div>
                        </section>

                        <section id="scheduler-panel-history" class="scheduler-tab-panel" data-tab="history" role="tabpanel" aria-labelledby="scheduler-tab-btn-history" hidden>
                            <div class="scheduler-queue">
                                <div id="history-container" class="queue-list-container">
                                    <ul id="scheduler-history-list" aria-live="polite"></ul>
                                </div>
                            </div>
                        </section>
                    </div>
                </div>
                <div class="settings-footer scheduler-modal-footer">
                    <button class="settings-close-action" id="scheduler-close-btn">Fechar</button>
                </div>
            </div>
        `;
        document.body.appendChild(schedulerModal);
    }

    /**
     * Esconde o painel de preview
     */
    function hidePreview() {
        if (previewPanel) {
            previewPanel.classList.remove('visible');
        }
    }

    /**
     * Atualiza o painel de preview com os dados do botão focado
     */
    function showPreview(button) {
        if (!previewPanel || !button) return;

        const isMobile = window.matchMedia('(max-width: 768px)').matches;
        if (isMobile) return;

        const gifPath = button.dataset.previewGif;
        const text = button.dataset.previewText;
        const tagsString = button.dataset.previewTags;

        if (!gifPath && !text) {
            hidePreview();
            return;
        }

        if (gifPath && previewImage) {
            previewImage.src = gifPath;
            previewImage.alt = text ? `Preview de ${text}` : 'Preview da Automação';
        }

        if (previewDescription) {
            previewDescription.textContent = text || 'Automação disponível';
        }

        if (previewTagsContainer) {
            previewTagsContainer.innerHTML = '';

            if (tagsString) {
                const tagsArray = tagsString.split(',');
                const knownNonKpiTags = [
                    'sap', 'bw', 'login', 'daily', 'weekly', 'monthly',
                    'planilha', 'relatorio', 'download', 'consulta', 'reversa'
                ];

                tagsArray.forEach(rawTag => {
                    const cleanTag = rawTag.trim();
                    if (!cleanTag) return;

                    const tagElement = document.createElement('span');
                    tagElement.className = 'preview-tag';
                    tagElement.textContent = cleanTag;

                    const lower = cleanTag.toLowerCase();
                    if (['sap', 'bw', 'login'].includes(lower)) {
                        tagElement.classList.add('tag-blue');
                    } else if (['daily', 'weekly', 'monthly'].includes(lower)) {
                        tagElement.classList.add('tag-green');
                    } else if (knownNonKpiTags.includes(lower)) {
                        tagElement.classList.add('tag-gray');
                    } else {
                        tagElement.classList.add('tag-purple');
                    }

                    previewTagsContainer.appendChild(tagElement);
                });
            }
        }

        previewPanel.classList.add('visible');
    }

    // --- Funções do Modal de Login ---

    function openLoginModal(taskInfo) {
        currentTaskInfo = taskInfo; 
        
        // Esconde todos os logos primeiro
        document.getElementById('modal-logo-sap-light').classList.add('hidden');
        document.getElementById('modal-logo-sap-dark').classList.add('hidden');
        modalLogoBw.classList.add('hidden');
        modalLogoSalesforce.classList.add('hidden');

        if (taskInfo.type === 'sap') {
            document.getElementById('modal-logo-sap-light').classList.remove('hidden');
            document.getElementById('modal-logo-sap-dark').classList.remove('hidden');
        } else if (taskInfo.type === 'bw') {
            modalLogoBw.classList.remove('hidden');
        } else if (taskInfo.type === 'salesforce') {
            modalLogoSalesforce.classList.remove('hidden');
        }
        
        modalUser.value = '';
        modalPass.value = '';
        modalOverlay.classList.add('visible');
        modalUser.focus();
    }

    function closeLoginModal() {
        modalOverlay.classList.remove('visible');
    }

    function handleLogin(saveConnection = false) {
        const user = modalUser.value;
        const pass = modalPass.value;

        if (!user || !pass) {
            alert('Por favor, preencha o usuário e a senha.');
            return;
        }

        const formData = new URLSearchParams();
        formData.append('usuario', user);
        formData.append('senha', pass);
        if (saveConnection) {
            formData.append('save_connection', 'true');
        }

        let endpoint = '';
        let systemType = currentTaskInfo.type;

        if (systemType === 'sap') {
            endpoint = '/login-sap';
            setProcessing("Realizando login no SAP...");
        } else if (systemType === 'bw') {
            endpoint = '/login-bw-hana';
            setProcessing("Realizando login no BW HANA...");
        } else if (systemType === 'salesforce') {
            endpoint = '/login-salesforce';
            setProcessing("Realizando login no Salesforce...");
        }

        fetch(endpoint, { method: 'POST', body: formData })
            .then(response => response.json())
            .then(data => {
                enableUI();
                if (data.status === 'sucesso') {
                    showStatus(data.mensagem, 'success');
                    closeLoginModal();
                    
                    if (systemType === 'sap') {
                        isSapLoggedIn = true;
                        isBwLoggedIn = false;
                        isSalesforceLoggedIn = false;
                    } else if (systemType === 'bw') {
                        isSapLoggedIn = false;
                        isBwLoggedIn = true;
                        isSalesforceLoggedIn = false;
                    } else if (systemType === 'salesforce') {
                        isSapLoggedIn = false;
                        isBwLoggedIn = false;
                        isSalesforceLoggedIn = true;
                    }
                    
                    currentHubUser = localStorage.getItem('hubUsername') || null;

                    if (saveConnection) {
                        if (!savedConnections[systemType]) savedConnections[systemType] = {};
                        savedConnections[systemType].user = user;
                        savedConnections[systemType].pass = pass;
                        showStatus("Conexão salva com sucesso. Você pode fechar o Hub para persistir o login.", 'success', true);
                    }
                    
                    updateUiState();
                    
                } else {
                    showStatus('ERRO: ' + data.mensagem, 'error', true);
                }
            })
            .catch(handleFetchError);
    }

    // --- Funções do Modal de Confirmação ---
    function openConfirmModal(onConfirm, onCancel) {
        confirmModalOverlay.classList.add('visible');
        
        confirmModalYesBtn.replaceWith(confirmModalYesBtn.cloneNode(true));
        confirmModalNoBtn.replaceWith(confirmModalNoBtn.cloneNode(true));
        
        const newYesBtn = document.getElementById('confirm-modal-yes-btn');
        const newNoBtn = document.getElementById('confirm-modal-no-btn');
        
        newYesBtn.addEventListener('click', () => {
            onConfirm();
            closeConfirmModal();
        });
        newNoBtn.addEventListener('click', () => {
            onCancel();
            closeConfirmModal();
        });
        
        if (confirmModalCloseBtn) {
            confirmModalCloseBtn.replaceWith(confirmModalCloseBtn.cloneNode(true));
            const newCloseBtn = document.getElementById('confirm-modal-close-btn');
            newCloseBtn.addEventListener('click', () => {
                onCancel();
                closeConfirmModal();
            });
        }
    }
    function closeConfirmModal() {
        confirmModalOverlay.classList.remove('visible');
    }

    // --- Funções de Execução de Tarefa ---
    
    function executeTask(taskInfo, isSchedulerJob = false) {
        currentTaskInfo = taskInfo; 
        
        if (taskInfo.type === 'sap') {
            setProcessing(`Executando '${taskInfo.name}'...`, isSchedulerJob);
            const formData = new URLSearchParams();
            formData.append('macro', taskInfo.name);
            fetch('/executar-macro', { method: 'POST', body: formData })
                .then(response => response.json())
                .then(data => handleTaskResult(data, isSchedulerJob))
                .catch(error => handleFetchError(error, isSchedulerJob));

        } else if (taskInfo.type === 'bw') {
            setProcessing('Executando extração BW HANA...', isSchedulerJob);
            fetch('/executar-bw-hana', { method: 'POST' })
                .then(response => response.json())
                .then(data => handleTaskResult(data, isSchedulerJob))
                .catch(error => handleFetchError(error, isSchedulerJob));

        } else if (taskInfo.type === 'salesforce') {
            setProcessing(`Executando '${taskInfo.name}'...`, isSchedulerJob);
            const sfFormData = new URLSearchParams();
            sfFormData.append('macro', taskInfo.name);
            fetch('/executar-salesforce', { method: 'POST', body: sfFormData })
                .then(response => response.json())
                .then(data => handleTaskResult(data, isSchedulerJob))
                .catch(error => handleFetchError(error, isSchedulerJob));
        }
    }
    
    function autoLoginAndExecute(taskInfo, credentials, isSchedulerJob = false) {
        const systemType = taskInfo.type;
        let endpoint;
        if (systemType === 'sap') endpoint = '/login-sap';
        else if (systemType === 'bw') endpoint = '/login-bw-hana';
        else if (systemType === 'salesforce') endpoint = '/login-salesforce';
        
        setProcessing(`Autenticando com conexão salva ${systemType.toUpperCase()}...`, isSchedulerJob);

        const formData = new URLSearchParams();
        formData.append('usuario', credentials.user);
        formData.append('senha', credentials.pass);

        fetch(endpoint, { method: 'POST', body: formData })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                if (systemType === 'sap') {
                    isSapLoggedIn = true;
                    isBwLoggedIn = false;
                    isSalesforceLoggedIn = false;
                } else if (systemType === 'bw') {
                    isBwLoggedIn = true;
                    isSapLoggedIn = false;
                    isSalesforceLoggedIn = false;
                } else if (systemType === 'salesforce') {
                    isSalesforceLoggedIn = true;
                    isSapLoggedIn = false;
                    isBwLoggedIn = false;
                }
                
                if (!isSchedulerJob) {
                    updateUiState();
                }
                
                executeTask(taskInfo, isSchedulerJob);
            } else {
                if (!isSchedulerJob) {
                    enableUI();
                    showStatus('Falha na conexão salva. Faça o login manual.', 'error', true);
                    openLoginModal(taskInfo);
                } else {
                    const job = jobQueue.find(j => j.status === 'running');
                    if (job) {
                        job.status = 'failed';
                        jobHistory.unshift(job);
                        jobHistory = jobHistory.slice(0, 4); 
                        jobQueue = jobQueue.filter(j => j.id !== job.id);
                        renderJobQueue();
                        renderJobHistory();
                    }
                    isSchedulerRunning = false;
                }
            }
        })
        .catch(error => {
            handleFetchError(error, isSchedulerJob);
            if (!isSchedulerJob) {
                openLoginModal(taskInfo);
            }
        });
    }

    // --- Funções de UI (Seleção de Sistema e Logout) ---

    function handleLogout() {
        let endpoint = '';
        if (isSapLoggedIn) {
            endpoint = '/logout-sap';
            setProcessing("Realizando logout do SAP...");
        } else if (isBwLoggedIn) {
            endpoint = '/logout-bw-hana';
            setProcessing("Realizando logout do BW...");
        } else if (isSalesforceLoggedIn) {
            endpoint = '/logout-salesforce';
            setProcessing("Realizando logout do Salesforce...");
        } else {
            isSapLoggedIn = false;
            isBwLoggedIn = false;
            isSalesforceLoggedIn = false;
            updateUiState();
            return;
        }

        fetch(endpoint, { method: 'POST' })
            .then(response => response.json())
            .then(data => {
                enableUI();
                showStatus(data.mensagem, 'success');
            })
            .catch(error => {
                handleFetchError(error, false); 
                showStatus("Logout forçado localmente após erro de comunicação.", 'error');
            })
            .finally(() => {
                isSapLoggedIn = false;
                isBwLoggedIn = false;
                isSalesforceLoggedIn = false;
                updateUiState();
            });
    }

    function updateUiState() {
        let showLogout = false;
        
        if (isSapLoggedIn && !savedConnections.sap) {
            showLogout = true;
        }
        else if (isBwLoggedIn && !savedConnections.bw) {
            showLogout = true;
        }
        else if (isSalesforceLoggedIn && !savedConnections.salesforce) {
            showLogout = true;
        } 

        if (showLogout) {
            logoutBtn.classList.remove('hidden');
        } else {
            logoutBtn.classList.add('hidden');
        }
        
        if (isSapLoggedIn) {
            const r = document.querySelector('input[name="login_system"][value="sap"]');
            if (r) r.checked = true;
        } else if (isBwLoggedIn) {
            const r = document.querySelector('input[name="login_system"][value="bw"]');
            if (r) r.checked = true;
        } else if (isSalesforceLoggedIn) {
            const r = document.querySelector('input[name="login_system"][value="salesforce"]');
            if (r) r.checked = true;
        }
        
        const selectedSystem = document.querySelector('input[name="login_system"]:checked')?.value;
        sapTasksSection?.classList.add('hidden');
        bwTasksSection?.classList.add('hidden');
        salesforceTasksSection?.classList.add('hidden');
        if (selectedSystem === 'sap') {
            sapTasksSection?.classList.remove('hidden');
        } else if (selectedSystem === 'bw') {
            bwTasksSection?.classList.remove('hidden');
        } else if (selectedSystem === 'salesforce') {
            salesforceTasksSection?.classList.remove('hidden');
        }
    }

    function handleSystemChange(isUpdate = false) {
        if (isUpdate) {
            return;
        }

        const selectedSystem = document.querySelector('input[name="login_system"]:checked')?.value;
        if (!selectedSystem) return;

        const showCorrectTasks = () => {
            sapTasksSection?.classList.add('hidden');
            bwTasksSection?.classList.add('hidden');
            salesforceTasksSection?.classList.add('hidden');
            if (selectedSystem === 'sap') {
                sapTasksSection?.classList.remove('hidden');
            } else if (selectedSystem === 'bw') {
                bwTasksSection?.classList.remove('hidden');
            } else if (selectedSystem === 'salesforce') {
                salesforceTasksSection?.classList.remove('hidden');
            }
        };

        const needsLogout = 
            (selectedSystem === 'sap' && (isBwLoggedIn || isSalesforceLoggedIn)) || 
            (selectedSystem === 'bw' && (isSapLoggedIn || isSalesforceLoggedIn)) ||
            (selectedSystem === 'salesforce' && (isSapLoggedIn || isBwLoggedIn));
        
        const hasAnySavedConnection = savedConnections.sap || savedConnections.bw || savedConnections.salesforce;

        if (needsLogout) {
            if (hasAnySavedConnection) {
                handleLogout(); 
            } else {
                openConfirmModal(
                    () => {
                        handleLogout();
                    },
                    () => {
                        const loggedInSystem = isSapLoggedIn ? 'sap' : (isBwLoggedIn ? 'bw' : 'salesforce');
                        document.querySelector(`input[name="login_system"][value="${loggedInSystem}"]`).checked = true;
                    }
                );
            }
        } else {
            showCorrectTasks();
        }
    }
    
    // --- Funções do Agendador (Redesign) ---

    // 1. Injeta o HTML do modal do agendador (MODIFICADO)
    // Em: automacao.js

// Em: automacao.js

// 1. Função para coletar todas as tarefas (Chamar apenas uma vez na inicialização do script)
function collectAllTasks() {
    schedulerAllTasks = [];
    
    // SAP Tasks
    const sapTasks = document.querySelectorAll('#sap-tasks-section .task-button');
    sapTasks.forEach(task => {
        const taskName = task.dataset.taskName;
        if (taskName) {
            schedulerAllTasks.push({ name: taskName, type: 'sap' });
        }
    });
    
    // BW Task
    const bwTask = document.getElementById('bw-extract-btn');
    if (bwTask) {
        schedulerAllTasks.push({ name: 'Relatório Peças', type: 'bw' });
    }

    // Salesforce Tasks
    const salesforceTasks = document.querySelectorAll('#salesforce-tasks-section .task-button');
    salesforceTasks.forEach(task => {
        const taskName = task.dataset.taskName;
        if (taskName) {
            schedulerAllTasks.push({ name: taskName, type: 'salesforce' });
        }
    });

    // Opcional: Selecionar o primeiro item na inicialização se a lista não estiver vazia
    if (schedulerAllTasks.length > 0) {
        const firstTaskValue = `${schedulerAllTasks[0].type}|${schedulerAllTasks[0].name}`;
        // Pode ser necessário ajustar como você define o input checked para o primeiro item.
        // Seus botões de rádio serão re-renderizados em renderPaginatedTasks.
    }
}

// Em: automacao.js

// 2. Função principal que renderiza a seleção de tarefas com paginação
function renderPaginatedTasks() {
    const container = document.getElementById('scheduler-tasks-container');
    if (!container) return;

    if (schedulerAllTasks.length === 0) {
        collectAllTasks();
        if (schedulerAllTasks.length === 0) return;
    }
    
    container.innerHTML = '';
    
    const totalTasks = schedulerAllTasks.length;
    const startIndex = currentTaskPage * TASKS_PER_PAGE;
    const endIndex = startIndex + TASKS_PER_PAGE;
    const tasksToRender = schedulerAllTasks.slice(startIndex, endIndex);
    
    const totalPages = Math.ceil(totalTasks / TASKS_PER_PAGE);

    tasksToRender.forEach((task, index) => {
        const actualIndex = startIndex + index;
        const taskValue = `${task.type}|${task.name}`;
        const isChecked = actualIndex === 0 ? 'checked' : ''; 
        
        // --- INÍCIO DA MODIFICAÇÃO (Req 2 e 3) ---
        let imageHtml = '';
        if (task.type === 'sap') {
            // (Req 2: Usar sapblack_logo.png)
            // (Req 3: Remover _logo extra)
            imageHtml = `
                <img src="/static/icones/sap_logo.png" alt="SAP Logo" class="scheduler-task-system-image logo-light">
                <img src="/static/icones/sapblack_logo.png" alt="SAP Logo" class="scheduler-task-system-image logo-dark">
            `;
        } else { // 'bw'
            imageHtml = `<img src="/static/icones/bwhanashort_logo.png" alt="BW Logo" class="scheduler-task-system-image">`;
        }
        
        const label = document.createElement('label');
        label.className = `scheduler-task-label ${task.type}`;
        label.innerHTML = `
            <input type="radio" name="scheduler_task" value="${taskValue}" ${isChecked}>
            ${imageHtml}
            <span class="task-name">${task.name}</span>
        `;
        // --- FIM DA MODIFICAÇÃO ---
        container.appendChild(label);
    });

    // (O restante da função continua igual... placeholders e paginação)
    const tasksRenderedCount = tasksToRender.length;
    const placeholdersNeeded = TASKS_PER_PAGE - tasksRenderedCount;

    for (let i = 0; i < placeholdersNeeded; i++) {
        const placeholder = document.createElement('div');
        placeholder.className = 'scheduler-task-placeholder'; 
        container.appendChild(placeholder);
    }
    
    if (totalPages > 1) {
        const paginationDiv = document.createElement('div');
        paginationDiv.className = 'task-pagination-controls';
        
        const prevButton = `<button class="pagination-btn" onclick="handleTaskPagination(-1)" ${currentTaskPage === 0 ? 'disabled' : ''} aria-label="Página anterior">
            <i class="fas fa-chevron-left"></i>
        </button>`;
        const pageInfo = `<span class="page-info">${currentTaskPage + 1} de ${totalPages}</span>`;
        const nextButton = `<button class="pagination-btn" onclick="handleTaskPagination(1)" ${currentTaskPage >= totalPages - 1 ? 'disabled' : ''} aria-label="Próxima página">
            <i class="fas fa-chevron-right"></i>
        </button>`;

        paginationDiv.innerHTML = prevButton + pageInfo + nextButton;
        container.appendChild(paginationDiv);
    }
}

// 3. Função para mudar a página (Chamar via click no HTML)
function handleTaskPagination(direction) {
    const totalTasks = schedulerAllTasks.length;
    const totalPages = Math.ceil(totalTasks / TASKS_PER_PAGE);
    
    let newPage = currentTaskPage + direction;
    
    if (newPage >= 0 && newPage < totalPages) {
        currentTaskPage = newPage;
        renderPaginatedTasks(); // Re-renderiza a lista de botões
    }
}

// === NOVO: EXPOR FUNÇÃO PARA O ESCOPO GLOBAL ===
window.handleTaskPagination = handleTaskPagination;

    // Em: automacao.js

// Em: automacao.js

// 3. Função de renderização genérica (para Fila e Histórico) - CORRIGIDA COM ANO
function renderJobList(listElement, jobs, showRemoveButton) {
    listElement.innerHTML = '';
    
    if (jobs.length === 0) {
        const message = listElement.id.includes('history') ? 'Não há histórico.' : 'Não há tarefas.';
        listElement.innerHTML = `<li class="queue-item empty">${message}</li>`;
        return;
    }
    
    const statusMap = {
        'pending': { icon: 'fa-hourglass-start', text: 'Pendente' },
        'running': { icon: 'fa-circle-notch fa-spin', text: 'Rodando' },
        'done': { icon: 'fa-check-circle', text: 'Concluído' },
        'failed': { icon: 'fa-times-circle', text: 'Falhou' }
    };
    
    jobs.forEach((job) => {
        const li = document.createElement('li');
        li.className = `queue-item ${job.status}`;
        
        const jobDate = new Date(job.startTime).toLocaleString('pt-BR', {
            day: '2-digit', month: '2-digit', year: 'numeric'
        });
        const jobTime = new Date(job.startTime).toLocaleString('pt-BR', {
            hour: '2-digit', minute: '2-digit'
        });
        
        const statusInfo = statusMap[job.status] || { icon: 'fa-question-circle', text: 'Desconhecido' };
        let actionItemHtml = '';
        const jobCreator = job.creator || null; 
        const currentUser = currentHubUser || null;
        const creatorName = (jobCreator || "Sistema").toUpperCase(); 

        if (showRemoveButton) { 
            if (jobCreator === currentUser) {
                actionItemHtml = `<button class="queue-item-remove" data-job-id="${job.id}" title="Remover Tarefa" aria-label="Remover ${job.taskInfo.name} da fila">&times;</button>`;
            } else {
                actionItemHtml = `<span class="queue-item-creator" title="Agendado por ${creatorName}">${creatorName}</span>`;
            }
        } else {
            actionItemHtml = `<span class="queue-item-creator" title="Agendado por ${creatorName}">${creatorName}</span>`;
        }
        
        // --- INÍCIO DA MODIFICAÇÃO (Req 2 e 3) ---
        let taskIconHtml = '';
        if (job.taskInfo.type === 'sap') {
            // (Req 2: Usar sapblack_logo.png)
            // (Req 3: Remover _logo extra)
            taskIconHtml = `
                <img src="/static/icones/sap_logo.png" class="queue-item-task-icon logo-light" alt="SAP logo">
                <img src="/static/icones/sapblack_logo.png" class="queue-item-task-icon logo-dark" alt="SAP logo">
            `;
        } else { // 'bw'
            taskIconHtml = `<img src="/static/icones/bwhanashort_logo.png" class="queue-item-task-icon" alt="BW logo">`;
        }
        // --- FIM DA MODIFICAÇÃO ---

        li.innerHTML = `
            <i class="fas ${statusInfo.icon} queue-item-icon" title="${statusInfo.text}"></i>
            <div class="queue-item-details">
                <strong>${taskIconHtml}${job.taskInfo.name}</strong>
                <em class="queue-item-datetime">Data: ${jobDate}</em>
                <em class="queue-item-datetime">Hora: ${jobTime}</em>
            </div>
            ${actionItemHtml}
        `;
        listElement.appendChild(li);
    });
    
    if (showRemoveButton) {
        listElement.querySelectorAll('.queue-item-remove').forEach(btn => {
            btn.addEventListener('click', () => {
                const jobId = btn.dataset.jobId;
                jobQueue = jobQueue.filter(job => job.id !== jobId);
                renderJobQueue();
                saveScheduleToServer(); 
            });
        });
    }
}

    // 4. Renderiza a fila na UI
    function renderJobQueue() {
        const list = document.getElementById('scheduler-queue-list');
        if (!list) return;
        renderJobList(list, jobQueue, true);
    }
    
    // 5. Renderiza o Histórico
    function renderJobHistory() {
        const list = document.getElementById('scheduler-history-list');
        if (!list) return;
        renderJobList(list, jobHistory, false);
    }

    // 6. Adiciona uma tarefa à fila (MODIFICADO)
function addJobToQueue() {
    const selectedRadio = document.querySelector('input[name="scheduler_task"]:checked');
    const dateInput = document.getElementById('scheduler-date');
    const timeInput = document.getElementById('scheduler-time');
    
    if (!selectedRadio) {
        alert('Por favor, selecione uma tarefa.');
        return;
    }
    
    const [type, name] = selectedRadio.value.split('|');
    const dateValue = dateInput.value; // "DD/MM/AAAA"
    const timeValue = timeInput.value; // "HH:MM"
    
    // --- NOVO: Validação e Conversão de Data/Hora ---
    if (!/^\d{2}\/\d{2}\/\d{4}$/.test(dateValue) || !/^\d{2}:\d{2}$/.test(timeValue)) {
        alert('Formato inválido. Use DD/MM/AAAA e HH:MM.');
        return;
    }

    const [day, month, year] = dateValue.split('/');
    const [hour, minute] = timeValue.split(':');
    
    // Converte para YYYY-MM-DDTHH:MM (formato ISO 8601) para o new Date()
    const isoDateString = `${year}-${month}-${day}T${hour}:${minute}`;
    const startTime = new Date(isoDateString).getTime();
    // --------------------------------------------------

    // --- REQ 2: Verificação de Duplicatas ---
    const existingJob = jobQueue.find(job => job.startTime === startTime && job.taskInfo.type === type);
    if (existingJob) {
        alert("Já existe uma tarefa desta plataforma agendada para esta data e hora.");
        return;
    }
    // --- Fim da Verificação ---

    const oneMinuteFromNow = Date.now() + 60000;
    
    if (isNaN(startTime) || startTime < oneMinuteFromNow) {
        alert('Data/hora inválida ou no passado. Selecione pelo menos 1 minuto de antecedência.');
        return;
    }
    
    const newJob = {
        id: `job_${Date.now()}`,
        taskInfo: { name: name, type: type },
        startTime: startTime,
        status: 'pending',
        creator: currentHubUser || null // <-- REQ 1: Salva o criador (null se deslogado) 
    };
    
    jobQueue.push(newJob);
    jobQueue.sort((a, b) => a.startTime - b.startTime);
    renderJobQueue();
    
    dateInput.value = '';
    timeInput.value = '';
    
    saveScheduleToServer(); // <-- ADICIONE ESTA LINHA
    
    if (!schedulerInterval) {
        startSchedulerMotor();
    }
}

    // 7. O "motor" que verifica a fila
    function startSchedulerMotor() {
        if (schedulerInterval) {
            clearInterval(schedulerInterval);
        }
        
        schedulerInterval = setInterval(() => {
            checkJobQueue();
        }, 5000); 
    }

    // 8. Lógica de verificação da fila (mantida)
    function checkJobQueue() {
        if (isSchedulerRunning || jobQueue.length === 0) {
            return; 
        }
        
        const now = Date.now();
        const jobToRun = jobQueue.find(job => job.status === 'pending' && job.startTime <= now);
        
        if (jobToRun) {
            const needsSap = jobToRun.taskInfo.type === 'sap';
            const needsBw = jobToRun.taskInfo.type === 'bw';
            
            const sapConnection = savedConnections.sap;
            const bwConnection = savedConnections.bw;

            let loginActive = (needsSap && isSapLoggedIn) || (needsBw && isBwLoggedIn);
            let connectionAvailable = (needsSap && sapConnection) || (needsBw && bwConnection);

            if (!loginActive && !connectionAvailable) {
                jobToRun.status = 'failed';
                showStatus(`Agendador: '${jobToRun.taskInfo.name}' falhou. Motivo: Login necessário não estava ativo nem salvo.`, 'error', true);
                
                jobHistory.unshift(jobToRun);
                jobHistory = jobHistory.slice(0, 4);
                jobQueue = jobQueue.filter(j => j.id !== jobToRun.id);
                
                renderJobQueue();
                renderJobHistory();
                saveScheduleToServer(); // <-- ADICIONE ESTA LINHA
                return;
            }

            isSchedulerRunning = true;
            jobToRun.status = 'running';
            showStatus(`Agendador: Iniciando tarefa '${jobToRun.taskInfo.name}'...`, 'processing', true);
            renderJobQueue();

            if (loginActive) {
                executeTask(jobToRun.taskInfo, true);
            } else {
                const credentials = needsSap ? sapConnection : bwConnection;
                autoLoginAndExecute(jobToRun.taskInfo, credentials, true);
            }
        }
    }

    // 9. Abre o modal do agendador (MODIFICADO)
function openSchedulerModal() {
    let modal = document.getElementById('scheduler-modal-overlay');
    
    if (!modal) {
        injectSchedulerHTML();
        modal = document.getElementById('scheduler-modal-overlay');
        // Adiciona listeners aos novos elementos DEPOIS de injetar
        document.getElementById('scheduler-close-btn').addEventListener('click', closeSchedulerModal);
        document.getElementById('scheduler-add-btn').addEventListener('click', addJobToQueue);
        
        // --- NOVO: Configura as máscaras na primeira abertura ---
        setupInputMasks(); 
        setupSchedulerTabs();
    }

    if (modal && !modal.dataset.overlayClickBound) {
        modal.addEventListener('mousedown', (event) => {
            if (event.target === modal) {
                closeSchedulerModal();
            }
        });
        modal.dataset.overlayClickBound = 'true';
    }
    
    // Sempre popula e renderiza ao abrir
    renderPaginatedTasks(); 
    renderJobQueue();
    renderJobHistory();
    document.getElementById('scheduler-modal-overlay').classList.add('visible');
    const firstTaskRadio = document.querySelector('input[name="scheduler_task"]');
    if (firstTaskRadio) {
        firstTaskRadio.focus();
    }
}

    // 10. Fecha o modal do agendador
    function closeSchedulerModal() {
        document.getElementById('scheduler-modal-overlay').classList.remove('visible');
    }
    
    // NOVO: Função para salvar a fila e o histórico no servidor
function saveScheduleToServer() {
    // O servidor sabe quem é o usuário pela sessão
    fetch('/api/scheduler/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            queue: jobQueue,
            history: jobHistory
        })
    }).catch(err => console.error("Falha ao salvar agendamento:", err));
}

// NOVO: Função para aplicar máscaras de input
// NOVO: Função para aplicar máscaras de input
function setupInputMasks() {
    const dateInput = document.getElementById('scheduler-date');
    const timeInput = document.getElementById('scheduler-time');
    
    // --- NOVOS SELETORES ---
    const calendarIcon = document.getElementById('scheduler-calendar-icon');
    const clockIcon = document.getElementById('scheduler-clock-icon');
    const dateNative = document.getElementById('scheduler-date-native');
    const timeNative = document.getElementById('scheduler-time-native');

    if (!dateInput.maskApplied) { // Evita adicionar o listener múltiplas vezes
        dateInput.maskApplied = true;
        
        // Listener da MÁSCARA (para digitação)
        dateInput.addEventListener('input', (e) => {
            let v = e.target.value.replace(/\D/g, ''); // Remove não-dígitos
            if (v.length > 8) v = v.slice(0, 8);
            if (v.length > 4) {
                v = `${v.slice(0, 2)}/${v.slice(2, 4)}/${v.slice(4)}`;
            } else if (v.length > 2) {
                v = `${v.slice(0, 2)}/${v.slice(2)}`;
            }
            e.target.value = v;
        });

        // --- Listener do ÍCONE (clica no input nativo) ---
        calendarIcon.addEventListener('click', () => {
            try {
                dateNative.showPicker(); // Método moderno para abrir o seletor
            } catch (error) {
                dateNative.click(); // Fallback para navegadores mais antigos
            }
        });

        // --- Listener do INPUT NATIVO (formata e preenche o input visível) ---
        dateNative.addEventListener('change', (e) => {
            if (!e.target.value) return; // Se o usuário cancelar
            // Valor (ex: 2025-12-01)
            const [year, month, day] = e.target.value.split('-');
            // Formata (ex: 01/12/2025)
            dateInput.value = `${day}/${month}/${year}`;
        });
    }

    if (!timeInput.maskApplied) {
        timeInput.maskApplied = true;
        
        // Listener da MÁSCARA (para digitação)
        timeInput.addEventListener('input', (e) => {
            let v = e.target.value.replace(/\D/g, '');
            if (v.length > 4) v = v.slice(0, 4);
            if (v.length > 2) {
                v = `${v.slice(0, 2)}:${v.slice(2)}`;
            }
            e.target.value = v;
        });

        // --- Listener do ÍCONE ---
        clockIcon.addEventListener('click', () => {
             try {
                timeNative.showPicker(); // Método moderno
            } catch (error) {
                timeNative.click(); // Fallback
            }
        });
        
        // --- Listener do INPUT NATIVO ---
        timeNative.addEventListener('change', (e) => {
            if (!e.target.value) return; // Se o usuário cancelar
            timeInput.value = e.target.value; // (Formato HH:MM já é o correto)
        });
    }
}

function setupSchedulerTabs() {
    const modal = document.getElementById('scheduler-modal-overlay');
    if (!modal) return;

    const tabButtons = modal.querySelectorAll('.scheduler-tab');
    const tabPanels = modal.querySelectorAll('.scheduler-tab-panel');

    const activateTab = (target) => {
        tabButtons.forEach(button => {
            button.classList.toggle('active', button.dataset.tab === target);
        });

        tabPanels.forEach(panel => {
            const isTarget = panel.dataset.tab === target;
            panel.classList.toggle('active', isTarget);
            panel.toggleAttribute('hidden', !isTarget);
        });
    };

    tabButtons.forEach(button => {
        button.addEventListener('click', () => activateTab(button.dataset.tab));
    });

    // Ativa aba padrão
    const defaultTab = modal.querySelector('.scheduler-tab[data-tab="tasks"]');
    if (defaultTab) {
        defaultTab.click();
    }
}

// NOVO: Função para processar a fila carregada (verifica tarefas perdidas)
function processLoadedSchedule(data) {
    const now = Date.now();
    const validQueue = [];
    let loadedHistory = data.history || [];
    
    (data.queue || []).forEach(job => {
        if (job.startTime < now) {
            // Se a tarefa já passou, marca como falha e move para o histórico
            job.status = 'failed'; 
            loadedHistory.unshift(job); // Adiciona no início do histórico
        } else {
            validQueue.push(job); // Mantém na fila
        }
    });

    // Atualiza as variáveis globais
    jobQueue = validQueue.sort((a, b) => a.startTime - b.startTime);
    jobHistory = loadedHistory.slice(0, 4); // Limita o histórico
    
    // Renderiza as listas atualizadas
    renderJobQueue(); 
    renderJobHistory();
    
    // Salva de volta no servidor caso tarefas tenham sido movidas
    if (data.queue.length !== validQueue.length) {
        saveScheduleToServer();
    }
}

    // --- Adiciona Listeners de Eventos (mantidos) ---

    // 1. Seleção de Sistema (Rádios)
    systemRadios.forEach(radio => {
        radio.addEventListener('change', () => handleSystemChange(false));
    });

    // 2. Botões de Tarefa (SAP e BW)
    allTaskButtons.forEach(button => {
        const taskName = button.getAttribute('data-task-name');
        const taskType = button.getAttribute('data-task-type');

        button.addEventListener('click', (e) => {
            if (button.classList.contains('disabled')) return;

            const info = {
                name: taskName || 'Relatório Peças',
                type: taskType
            };

            if (info.type === 'sap' && savedConnections.sap) {
                autoLoginAndExecute(info, savedConnections.sap, false);
                return;
            }
            if (info.type === 'bw' && savedConnections.bw) {
                autoLoginAndExecute(info, savedConnections.bw, false);
                return;
            }

            if ((info.type === 'sap' && isSapLoggedIn) || (info.type === 'bw' && isBwLoggedIn)) {
                executeTask(info, false); 
            } else {
                openLoginModal(info);
            }
        });

        // --- INÍCIO DA MODIFICAÇÃO (Req 1: Adicionar Listeners de Preview) ---
        button.addEventListener('mouseover', () => {
            showPreview(button);
        });

        button.addEventListener('mouseout', () => {
            hidePreview();
        });
        // --- FIM DA MODIFICAÇÃO ---
    });

    // 3. Botões do Modal de Login
    modalExecuteBtn.addEventListener('click', () => handleLogin(false));
    if (modalSaveConnBtn) {
        modalSaveConnBtn.addEventListener('click', () => handleLogin(true));
    }
    
    modalLoginCloseBtn.addEventListener('click', closeLoginModal);
    modalPass.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') handleLogin(false);
    });
    
    // 4. Botão de Logout
    logoutBtn.addEventListener('click', () => {
        openConfirmModal(
            () => handleLogout(), 
            () => {} 
        );
    });

    // 5. Download Icon Base Mãe
    const baseMaeDownload = document.querySelector('.task-button-download');
    if (baseMaeDownload) {
        baseMaeDownload.addEventListener('click', (e) => {
            e.stopPropagation(); 
            if (baseMaeDownload.classList.contains('inactive')) {
                e.preventDefault();
                alert('Arquivo para download não encontrado. Execute a automação primeiro.');
            }
        });
    }

    // 6. Listener do botão Agendador
    if (schedulerBtn) {
        schedulerBtn.addEventListener('click', openSchedulerModal);
    }

    // --- NOVO: Listener para fechar o seletor de hora nativo ---
    const schedulerTimeInput = document.getElementById('scheduler-time');
    if (schedulerTimeInput) {
        schedulerTimeInput.addEventListener('change', () => {
            // Força a perda de foco quando o valor é alterado (ou seja, quando o usuário seleciona)
            schedulerTimeInput.blur();
        });
    }
    // -------------------------------------------------------------

    /// --- Função de Inicialização ---
function initialize() {
    currentHubUser = localStorage.getItem('hubUsername') || null;
    // 1. Coleta as tarefas (síncrono, necessário para o agendador)
    collectAllTasks(); 
    
    // 2. Busca conexões salvas
    fetch('/api/hub/get-connections')
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                savedConnections = data.connections;
                console.log("Conexões salvas carregadas:", savedConnections);
            } else {
                console.log("Nenhuma conexão salva encontrada (usuário não logado no Hub?).");
            }
        })
        .catch(err => {
            console.error("Erro ao buscar conexões:", err);
        })
        .finally(() => {
            // 3. Busca o agendamento salvo
            fetch('/api/scheduler/load')
                .then(res => res.json())
                .then(data => {
                    // 4. Processa a fila (move tarefas passadas para o histórico)
                    processLoadedSchedule(data);
                })
                .catch(err => console.error("Falha ao carregar agendamento:", err))
                .finally(() => {
                    // 5. Inicia a UI e o motor do agendador (APENAS após tudo carregar)
                    updateUiState();
                    startSchedulerMotor(); 
                });
        });
}

    // --- Estado Inicial ---
    initialize();

});

    const pageKey = window.location.pathname.replace('/', '').split('?')[0]; // Extrai "automacao", "dashboards", ou "drive"
    const currentTab = document.querySelector(`.main-nav-tabs .nav-tab[data-page="${pageKey}"]`);
    if (currentTab) {
        currentTab.classList.add('active');
    }

    const automacaoSearchInput = document.getElementById('automacao-search-input');
    if (automacaoSearchInput) {
        function handleAutomationSearch() {
            const searchTerm = automacaoSearchInput.value.toLowerCase();
            const sapContainer = document.querySelector('#sap-tasks-section .button-container');
            const bwContainer = document.querySelector('#bw-tasks-section .button-container');
            const salesforceContainer = document.querySelector('#salesforce-tasks-section .button-container');

            // Seleciona todos os containers de botões (incluindo grupos), ignorando os que não existem
            const allTaskElements = [
                ...(sapContainer ? sapContainer.querySelectorAll('.task-button, .task-button-group') : []),
                ...(bwContainer ? bwContainer.querySelectorAll('.task-button, .task-button-group') : []),
                ...(salesforceContainer ? salesforceContainer.querySelectorAll('.task-button, .task-button-group') : []),
            ];

            allTaskElements.forEach(element => {
                // Encontra o botão real dentro do grupo ou o próprio elemento
                const button = element.querySelector('.task-button') || element;
                const taskName = button.dataset.taskName.toLowerCase();

                if (taskName.includes(searchTerm)) {
                    element.style.display = 'flex';
                } else {
                    element.style.display = 'none';
                }
            });
        }

        automacaoSearchInput.addEventListener('keyup', handleAutomationSearch);
    }