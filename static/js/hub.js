document.addEventListener('DOMContentLoaded', () => {
    
    // --- Seletores de Elementos ---
    const searchBar = document.getElementById('search-bar');
    const cards = document.querySelectorAll('.hub-card');
    const quickLinksSection = document.getElementById('quick-links-section');
    const quickLinksContainer = document.getElementById('quick-links-container');
    const hubSubtitle = document.querySelector('.hub-subtitle'); // <-- ADICIONE ESTA LINHA
    
    // --- CÓDIGO ALTERADO: Seletores da Busca Universal ---
    const hubCardsContainer = document.getElementById('hub-cards-container');
    const searchDropdown = document.getElementById('search-dropdown-results')
    const searchDropdownBody = document.getElementById('search-dropdown-body');

    // --- Seletores do Header e Acesso ---
    const accessBtn = document.getElementById('access-btn');
    const profileImgThumb = document.getElementById('profile-img'); // Imagem no header
    const defaultProfileUrl = "/static/icones/default_profile.png"; // Imagem padrão
    const accessDropdown = document.getElementById('access-dropdown');
    
    let currentHubUser = null;
    let currentHubArea = null; // Área ativa no hub (pode mudar ao trocar de área)
    let currentHubOriginArea = null; // Área de origem do usuário (definida pelo admin, estática)
    let currentHubRole = null;
    let currentHubDisplayName = null; // (Req 1) Armazena o nome de usuário globalmente
    let currentHubAllowedAreas = []; // Áreas liberadas para o usuário
    let currentProfileUrl = defaultProfileUrl; // URL da imagem atual
    let globalCmsDashboards = {}; // Armazena o JSON de dashboards
    let globalCmsAutomations = {}; // Armazena o JSON de automações
    let globalCmsUsers = []; // <-- ADICIONE ESTA LINHA
    let cropper = null; // Instância do Cropper.js
    let selectedFile = null; // Arquivo original selecionado
    let currentUploadExtension = null; // Extensão do arquivo original
    const translate = (key, fallback) => (window.hubI18n && typeof hubI18n.t === 'function')
        ? hubI18n.t(key, fallback)
        : (fallback || key);
    const isHubPage = document.body.classList.contains('hub-page');

     // --- CÓDIGO ADICIONADO: Índice de Busca Universal ---
    let searchableIndex = [];

    // --- Seletores dos Modais ---
    const settingsBtn = document.getElementById('settings-btn');
    const settingsOverlay = document.getElementById('settings-overlay');
    const aboutBtn = document.getElementById('about-btn');
    const aboutOverlay = document.getElementById('about-overlay');
    const aboutFooterCloseBtn = document.getElementById('about-footer-close');
    const countOptions = document.querySelectorAll('.count-selector .setting-option');
    const settingsFooterCloseBtn = document.getElementById('settings-footer-close');
    const settingsLanguageSelect = document.getElementById('settings-language-select');
    const settingsLanguageHint = document.getElementById('settings-language-hint');
    const settingsLanguageStatus = document.getElementById('settings-language-status');
    let settingsLanguageStatusTimer = null;
    
    // Modal de Login do Hub
    const hubLoginOverlay = document.getElementById('hub-login-overlay');
    const hubLoginCloseBtn = document.getElementById('hub-login-close-btn');
    const hubLoginSubmitBtn = document.getElementById('hub-login-submit-btn');
    const hubUserInput = document.getElementById('hub-user');
    const hubPassInput = document.getElementById('hub-pass');
    const hubLoginError = document.getElementById('hub-login-error');

    // Modal de Conexões
    const connectionsOverlay = document.getElementById('connections-overlay');
    const connectionsFooterCloseBtn = document.getElementById('connections-footer-close');
    const connectionsListContainer = document.getElementById('connections-list-container');
    
    // Modal de Perfil
    const profileOverlay = document.getElementById('profile-overlay');
    const profileCloseBtn = document.getElementById('profile-close-btn');
    const profilePreviewImg = document.getElementById('profile-preview-img');
    const profileUploadForm = document.getElementById('profile-upload-form');
    const profileFileInput = document.getElementById('profile-file-input');
    const profileRemoveBtn = document.getElementById('profile-remove-btn');
    const profileUsernameDisplay = document.querySelector('.profile-username-display');
    const profileAreaDisplay = document.getElementById('profile-area-display');
    const profileRoleDisplay = document.getElementById('profile-role-display');
    const profileUploadStatus = document.getElementById('profile-upload-status');
    const editNameOverlay = document.getElementById('edit-name-overlay');
    const editNameCloseBtn = document.getElementById('edit-name-close-btn');
    const editNameInput = document.getElementById('edit-name-input');
    const editNameSaveBtn = document.getElementById('edit-name-save-btn');

    // Modal de Recorte
    const cropperOverlay = document.getElementById('cropper-overlay');
    const cropperCloseBtn = document.getElementById('cropper-close-btn');
    const cropperImage = document.getElementById('cropper-image');
    const cropperSaveBtn = document.getElementById('cropper-save-btn');
    
    // Modal de Registro
    const registerOverlay = document.getElementById('register-overlay');
    const registerCloseBtn = document.getElementById('register-close-btn');
    const tabRegister = document.getElementById('tab-register');
    const tabConsult = document.getElementById('tab-consult');
    const registerTabContent = document.getElementById('register-tab-content');
    const consultTabContent = document.getElementById('consult-tab-content');
    
    // Form de Registro
    const registerForm = document.getElementById('register-form');
    const registerUser = document.getElementById('register-user');
    const registerArea = document.getElementById('register-area');
    const registerRole = document.getElementById('register-role');
    const registerSubmitBtn = document.getElementById('register-submit-btn');
    const registerStatus = document.getElementById('register-status');
    const registerTokenDisplay = document.getElementById('register-token-display');
    const registerFieldsWrapper = document.getElementById('register-fields');
    const registerQrImage = document.getElementById('register-qr-image');
    const registerCopyCodeBtn = document.getElementById('register-copy-code');
    const registerCodeChars = document.querySelectorAll('#register-code-display .access-code-char');
    const tokenGenerated = document.getElementById('token-generated');
    
    // Form de Consulta
    const consultToken = document.getElementById('consult-token');
    const consultTokenBtn = document.getElementById('consult-token-btn');
    const consultCodeInputs = Array.from(document.querySelectorAll('.consult-code-input'));
    const consultStatusError = document.getElementById('consult-status-error');
    const consultStatusWrapper = document.getElementById('consult-status-wrapper');
    const consultProgressWrapper = document.getElementById('consult-progress');
    const consultProgressRequestedItem = document.querySelector('[data-progress-step="requested"]');
    const consultProgressAwaitingItem = document.querySelector('[data-progress-step="awaiting"]');
    const consultProgressSecondLabel = document.getElementById('consult-progress-second-label');
    const consultAccessInfo = document.getElementById('consult-access-info');
    const consultAccessUsername = document.getElementById('consult-access-username');
    const consultAccessPassword = document.getElementById('consult-access-password');
    const consultRejectedInfo = document.getElementById('consult-rejected-info');
    const consultRejectedText = document.getElementById('consult-rejected-text');

    // Modal de Admin
    const adminOverlay = document.getElementById('admin-overlay');
    const adminCloseBtn = document.getElementById('admin-close-btn');
    
    // Abas e Painéis Admin
    const tabAdminRequests = document.getElementById('tab-admin-requests');
    const tabAdminUsers = document.getElementById('tab-admin-users');
    const tabAdminDashboards = document.getElementById('tab-admin-dashboards');
    const tabAdminAutomations = document.getElementById('tab-admin-automations');
    
    const adminRequestsTab = document.getElementById('admin-requests-tab');
    const adminUsersTab = document.getElementById('admin-users-tab');
    const adminDashboardsTab = document.getElementById('admin-dashboards-tab');
    const adminAutomationsTab = document.getElementById('admin-automations-tab');

    // Contêineres de Lista Admin
    const adminListContainer = document.getElementById('admin-list-container');
    const adminUserListContainer = document.getElementById('admin-user-list-container');
    const adminDashboardsList = document.getElementById('admin-dashboards-list');
    const adminAutomationsList = document.getElementById('admin-automations-list');
    const adminRequestsBadge = document.getElementById('admin-requests-count');
    const adminActiveTitle = document.getElementById('admin-active-title');
    const adminActiveDescription = document.getElementById('admin-active-description');
    const adminSearchContainerElement = document.getElementById('admin-search-container');
    const adminSearchInputField = document.getElementById('admin-search-input');

    const overlayIdsToDetach = [
        'settings-overlay',
        'about-overlay',
        'hub-login-overlay',
        'connections-overlay',
        'profile-overlay',
        'cropper-overlay',
        'edit-name-overlay',
        'register-overlay',
        'admin-overlay'
    ];
    overlayIdsToDetach.forEach(id => {
        const overlayEl = document.getElementById(id);
        if (overlayEl && overlayEl.parentElement !== document.body) {
            document.body.appendChild(overlayEl);
        }
    });
    
    // Botões de Adicionar Admin
    const adminAddAutomationBtn = document.getElementById('admin-add-automation-btn');
    const adminAddDashboardBtn = document.getElementById('admin-add-dashboard-btn');
    let pendingRequestsCount = 0;
    let registerCopyFeedbackTimer = null;
    const ACCESS_CODE_LENGTH = 6;
    let consultFetchInProgress = false;

    if (adminSearchInputField) {
        adminSearchInputField.addEventListener('keyup', handleAdminSearch);
    }

    // --- CÓDIGO ADICIONADO: Função para construir o Índice de Busca ---
    function buildSearchIndex() {
        if (!isHubPage || !searchDropdownBody) return;
        searchableIndex = []; // Reseta o índice


        // 1. Adiciona os Cards Principais
        cards.forEach(card => {
            if (!card.classList.contains('disabled')) { // Só adiciona cards clicáveis
                searchableIndex.push({
                    type: 'card',
                    name: card.querySelector('h2').textContent,
                    description: card.querySelector('p').textContent,
                    href: card.href,
                    icon: card.querySelector('i.fas').className,
                    origin: card.querySelector('h2').textContent.trim()
                });
            }
        });


        // 2. Adiciona os Dashboards do JSON
    for (const systemKey in globalCmsDashboards) {
            const system = globalCmsDashboards[systemKey];
            for (const areaKey in system.areas) {
                const area = system.areas[areaKey];
                area.items.forEach(item => {
                    searchableIndex.push({
                        type: 'item',
                        name: item.name,
                        description: item.text,
                        tags: (item.tags || '').replace(/,/g, ' '), // Substitui vírgulas por espaços
                        href: `/dashboards?open=${item.id}`,
                        icon: 'fas fa-chart-pie', // Ícone padrão de dashboard
                        origin: 'Dashboards'
                    });
                });
            }
        }
       
        // 3. Adiciona as Automações do JSON
        for (const autoName in globalCmsAutomations) {
            const auto = globalCmsAutomations[autoName];
            searchableIndex.push({
                type: 'item',
                name: autoName,
                description: `Automação ${auto.type.toUpperCase()}: ${auto.macro || ''}`,
                tags: `${auto.type} automação`,
                href: `/automacao?open=${autoName.replace(/\s+/g, '-')}`, // URL amigável
                icon: 'fas fa-robot', // Ícone de automação
                origin: 'Automações'
            });
        }
    }
    // --- FIM DA ADIÇÃO ---

    function applySavedAutomationOrder() {
        if (!isHubPage || !currentHubUser) return;
        const savedOrder = sessionStorage.getItem(`sortedAutomations_${currentHubUser}`);
        if (!savedOrder) return;
        try {
            const savedObj = JSON.parse(savedOrder);
            const savedKeys = Object.keys(savedObj);
            if (savedKeys.length === 0) return;

            const reordered = {};
            savedKeys.forEach(key => {
                if (globalCmsAutomations[key]) {
                    reordered[key] = globalCmsAutomations[key];
                }
            });
            Object.keys(globalCmsAutomations).forEach(key => {
                if (!reordered[key]) {
                    reordered[key] = globalCmsAutomations[key];
                }
            });

            globalCmsAutomations = reordered;
        } catch (err) {
            console.error('Falha ao aplicar ordem salva das automações', err);
            sessionStorage.removeItem(`sortedAutomations_${currentHubUser}`);
        }
    }


    // --- CÓDIGO NOVO: Função para carregar os dados do CMS e construir o índice ---
    function loadSearchData() {
        if (!isHubPage) return;
        // 1. Verifica se os dados já foram carregados (pelo sessionStorage, por exemplo)
        const automationsLoaded = Object.keys(globalCmsAutomations).length > 0;
        const dashboardsLoaded = Object.keys(globalCmsDashboards).length > 0;


        if (automationsLoaded && dashboardsLoaded) {
            // Se os dados já estão aqui (do cache da sessão), apenas construa o índice
            applySavedAutomationOrder();
            buildSearchIndex();
        } else {
            // Usa o endpoint público (filtrado por área do usuário, ou admin para todos)
            const cmsEndpoint = currentHubUser && currentHubUser.toLowerCase() === 'admin'
                ? '/api/admin/get-cms-data'
                : '/api/hub/get-cms-data';
            fetch(cmsEndpoint)
            .then(response => response.json())
            .then(data => {
                if (data.status === 'sucesso') {
                    globalCmsDashboards = data.dashboards;
                    globalCmsAutomations = data.automations;
                    applySavedAutomationOrder();
                }
            })
            .catch(err => {
                console.error("Erro ao carregar dados de busca do CMS:", err);
            })
            .finally(() => {
                // Concluindo, com ou sem erro, construa o índice com o que tiver
                buildSearchIndex();
            });
        }
    }

    // --- 1. LÓGICA DO SELETOR DE TEMA ---
    
    function applyTheme(theme) {
        applyGlobalTheme(theme); 
        document.querySelectorAll('.theme-option').forEach(opt => {
            opt.classList.toggle('active', opt.dataset.theme === theme);
        });
        localStorage.setItem('hubTheme', theme);
    }

    function initializeThemeOptions(scope = document) {
        const options = scope.querySelectorAll('.theme-option');
        if (!options.length) return;
        const savedTheme = localStorage.getItem('hubTheme') || 'light';
        options.forEach(option => {
            option.classList.toggle('active', option.dataset.theme === savedTheme);
            if (!option.dataset.themeBound) {
                option.dataset.themeBound = 'true';
                option.addEventListener('click', () => {
                    applyTheme(option.dataset.theme);
                });
            }
        });
    }

    function getCurrentLanguagePreference() {
        try {
            if (window.hubI18n && typeof hubI18n.getLanguage === 'function') {
                return window.hubI18n.getLanguage();
            }
        } catch (error) {
            console.warn('Não foi possível obter o idioma atual.', error);
        }
        return document.documentElement?.dataset?.hubLang || document.documentElement?.lang || 'pt';
    }

    function updateSettingsLanguageStatus(variant, messageKey) {
        if (!settingsLanguageStatus) return;
        const fallbackMessages = {
            'profile.language.statusApplying': 'Aplicando idioma...',
            'profile.language.statusSaved': 'Idioma atualizado.',
            'profile.language.statusError': 'Não foi possível aplicar o idioma.'
        };
        const message = messageKey ? translate(messageKey, fallbackMessages[messageKey]) : '';
        let className = 'hub-form-status';
        if (variant === 'success' || variant === 'error' || variant === 'info') {
            className += ` ${variant}`;
        }
        settingsLanguageStatus.textContent = message;
        settingsLanguageStatus.className = `${className} ${message ? 'visible' : 'hidden'}`.trim();
        clearTimeout(settingsLanguageStatusTimer);
        if (message) {
            settingsLanguageStatusTimer = setTimeout(() => {
                settingsLanguageStatus.className = 'hub-form-status hidden';
            }, 3000);
        }
    }

    function applySettingsLanguagePreference(lang) {
        const applyPromise = (window.hubI18n && typeof hubI18n.setLanguage === 'function')
            ? hubI18n.setLanguage(lang, { persist: true })
            : Promise.resolve(lang);

        applyPromise
            .then(() => {
                refreshHubSettingsLanguageUI(lang);
                broadcastLanguageChange(lang);
            })
            .catch(() => {
                updateSettingsLanguageStatus('error', 'profile.language.statusError');
            });
    }

    function applyCountSetting(count, context = document) {
        const numericCount = parseInt(count) || 4;
        const contextsToUpdate = new Set([document]);
        if (context && context !== document) {
            contextsToUpdate.add(context);
        }

        contextsToUpdate.forEach(ctx => {
            if (!ctx || typeof ctx.querySelectorAll !== 'function') return;
            const optionsToUpdate = ctx.querySelectorAll('.count-selector .setting-option');
            optionsToUpdate.forEach(opt => {
                opt.classList.toggle('active', opt.dataset.count == numericCount);
            });
        });

        localStorage.setItem('hubItemCount', numericCount);
        renderQuickLinks();
    }

    function getLanguageDisplayName(lang) {
        if (!lang) return translate('settings.language.unknown', 'Idioma indefinido');
        const normalized = lang.toLowerCase();
        if (normalized === 'en') {
            return translate('profile.language.option.en', 'Inglês');
        }
        return translate('profile.language.option.pt', 'Português');
    }

    function refreshHubSettingsLanguageUI(preferredLang = getCurrentLanguagePreference()) {
        if (settingsLanguageSelect) {
            settingsLanguageSelect.value = preferredLang;
        }
        if (settingsLanguageHint) {
            const hintLabel = translate('settings.language.currentHint', 'Idioma atual');
            const languageLabel = getLanguageDisplayName(preferredLang);
            settingsLanguageHint.textContent = `${hintLabel}: ${languageLabel}`;
        }
    }

    function broadcastLanguageChange(lang) {
        document.dispatchEvent(new CustomEvent('hubLanguageChanged', {
            detail: { lang }
        }));
    }

    function clearRecents() {
        localStorage.removeItem(getStorageKey('recentDashboards'));
        localStorage.removeItem(getStorageKey('pinnedDashboards'));
        renderQuickLinks();
    }

    function updateAreaDropdown(systemSelect, areaSelect) {
    const selectedSystem = systemSelect.value;
    areaSelect.innerHTML = ''; // Limpa opções antigas

    // Busca as áreas do sistema selecionado no objeto global
    if (globalCmsDashboards[selectedSystem] && globalCmsDashboards[selectedSystem].areas) {
        const areas = globalCmsDashboards[selectedSystem].areas;
        
        // Cria as novas <option> para as áreas
        Object.keys(areas).forEach(areaKey => {
            const option = document.createElement('option');
            option.value = areaKey;
            option.textContent = areas[areaKey].name;
            areaSelect.appendChild(option);
        });
    }
}
    
    initializeThemeOptions(document);

    if (isHubPage) {
        applyCountSetting(localStorage.getItem('hubItemCount') || 4);
        refreshHubSettingsLanguageUI();

        if (settingsBtn && settingsOverlay) {
            settingsBtn.addEventListener('click', () => { 
                refreshHubSettingsLanguageUI();
                updateSettingsLanguageStatus();
                settingsOverlay.classList.add('visible'); 
            });
            settingsOverlay.addEventListener('mousedown', (e) => { if (e.target === settingsOverlay) { settingsOverlay.classList.remove('visible'); } });
        }
        if (aboutBtn && aboutOverlay) {
            aboutBtn.addEventListener('click', () => { aboutOverlay.classList.add('visible'); });
            if (aboutFooterCloseBtn) {
                aboutFooterCloseBtn.addEventListener('click', () => { aboutOverlay.classList.remove('visible'); });
            }
            aboutOverlay.addEventListener('mousedown', (e) => { if (e.target === aboutOverlay) { aboutOverlay.classList.remove('visible'); } });
        }
        countOptions.forEach(option => { option.addEventListener('click', () => { applyCountSetting(option.dataset.count); }); });
        if (settingsFooterCloseBtn && settingsOverlay) {
            settingsFooterCloseBtn.addEventListener('click', () => { settingsOverlay.classList.remove('visible'); });
        }
        if (settingsLanguageSelect) {
            settingsLanguageSelect.addEventListener('change', () => {
                applySettingsLanguagePreference(settingsLanguageSelect.value);
            });
        }
        document.addEventListener('hubLanguageChanged', (event) => {
            const nextLang = (event && event.detail && event.detail.lang) ? event.detail.lang : getCurrentLanguagePreference();
            refreshHubSettingsLanguageUI(nextLang);
        });
    }
    
    
    // --- 2. LÓGICA DE ACESSO RÁPIDO (COM PINS) (MODIFICADO) ---

    function getStorageKey(baseKey) {
        // Usa o usuário logado ou '_guest' se estiver deslogado
        const userKey = currentHubUser || '_guest';
        return `${baseKey}_${userKey}`;
    }

    function getRecents() { 
        return JSON.parse(localStorage.getItem(getStorageKey('recentDashboards'))) || []; 
    }
    function getPinned() { 
        return JSON.parse(localStorage.getItem(getStorageKey('pinnedDashboards'))) || []; 
    }
    function savePinned(pinned) { 
        localStorage.setItem(getStorageKey('pinnedDashboards'), JSON.stringify(pinned)); 
    }
    function togglePin(item) {
        let pinned = getPinned();
        const isPinned = pinned.some(p => p.id === item.id);
        if (isPinned) {
            pinned = pinned.filter(p => p.id !== item.id);
        } else {
            pinned.unshift(item);
            pinned = pinned.slice(0, 4);
        }
        savePinned(pinned);
        renderQuickLinks();
    }

    // --- Seletor de Área ---

    // Fecha dropdown ao clicar fora (registrado uma vez)
    document.addEventListener('click', () => {
        const dropdown = document.getElementById('hub-area-dropdown');
        const btn = document.getElementById('hub-area-btn');
        if (dropdown) dropdown.classList.remove('open');
        if (btn) btn.classList.remove('open');
    });

    function getActiveHubArea(username, defaultArea) {
        try {
            return sessionStorage.getItem('activeHubArea_' + username) || defaultArea;
        } catch (e) {
            return defaultArea;
        }
    }

    function renderAreaSelector(activeArea, allowedAreas) {
        const selector = document.getElementById('hub-area-selector');
        const btn = document.getElementById('hub-area-btn');
        const label = document.getElementById('hub-area-btn-label');
        const dropdown = document.getElementById('hub-area-dropdown');
        if (!selector || !btn || !label || !dropdown) return;

        const areaIcons = {
            'Spare Parts': 'fas fa-box-open',
            'Finished Goods': 'fas fa-truck'
        };
        const defaultIcon = 'fas fa-layer-group';

        if (!allowedAreas || allowedAreas.length === 0) {
            selector.style.display = 'none';
            return;
        }

        const iconClass = areaIcons[activeArea] || defaultIcon;
        label.innerHTML = `<i class="${iconClass}"></i><span>${activeArea}</span>`;
        selector.style.display = 'block';

        if (allowedAreas.length <= 1) {
            btn.classList.add('single-area');
            dropdown.innerHTML = '';
            return;
        }

        btn.classList.remove('single-area');

        dropdown.innerHTML = allowedAreas.map(area => `
            <button class="hub-area-dropdown-item${area === activeArea ? ' active' : ''}" data-area="${area}" type="button">
                <i class="${areaIcons[area] || defaultIcon}"></i>
                <span>${area}</span>
            </button>
        `).join('');

        dropdown.querySelectorAll('.hub-area-dropdown-item').forEach(item => {
            item.addEventListener('click', (e) => {
                e.stopPropagation();
                dropdown.classList.remove('open');
                btn.classList.remove('open');
                if (item.dataset.area !== currentHubArea) {
                    switchHubArea(item.dataset.area);
                }
            });
        });

        btn.onclick = (e) => {
            e.stopPropagation();
            const isOpen = dropdown.classList.toggle('open');
            btn.classList.toggle('open', isOpen);
        };
    }

    function switchHubArea(newArea) {
        if (!currentHubUser) return;
        fetch('/api/hub/set-active-area', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ area: newArea })
        })
        .then(r => r.json())
        .then(data => {
            if (data.status === 'sucesso') {
                try {
                    sessionStorage.setItem('activeHubArea_' + currentHubUser, newArea);
                } catch (e) { /* ignora */ }
                currentHubArea = newArea;
                renderAreaSelector(newArea, currentHubAllowedAreas);
                // Atualiza os rótulos de área nas subpáginas (se houver)
                document.querySelectorAll('.hub-area-label-text').forEach(el => { el.textContent = 'Hub ' + newArea; });
                // Recarrega o conteúdo CMS filtrado
                loadSearchData();
                const cardsContainer = document.getElementById('hub-cards-container');
                if (cardsContainer) {
                    fetch('/api/hub/get-cms-data')
                        .then(r => r.json())
                        .then(cms => {
                            if (cms.automations) globalCmsAutomations = cms.automations;
                            if (cms.dashboards) globalCmsDashboards = cms.dashboards;
                        });
                }
            }
        })
        .catch(() => {});
    }

    function switchHubAreaThenNavigate(newArea, url) {
        if (!currentHubUser) { window.location.href = url; return; }
        fetch('/api/hub/set-active-area', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ area: newArea })
        })
        .then(r => r.json())
        .then(data => {
            if (data.status === 'sucesso') {
                try { sessionStorage.setItem('activeHubArea_' + currentHubUser, newArea); } catch (e) {}
            }
            window.location.href = url;
        })
        .catch(() => { window.location.href = url; });
    }

    function renderQuickLinks() {
        if (!isHubPage || !quickLinksContainer) return;
        quickLinksContainer.innerHTML = ''; 
        const recents = getRecents();
        const pinned = getPinned();
        let combined = [...pinned];
        recents.forEach(recent => {
            if (!pinned.some(p => p.id === recent.id)) {
                combined.push(recent);
            }
        });
        const savedCount = parseInt(localStorage.getItem('hubItemCount') || 4);
        const itemsToRender = combined.slice(0, savedCount);
        
        if (itemsToRender.length === 0) {
            quickLinksContainer.innerHTML = '<p class="no-recents">Nenhum item acessado recentemente.</p>';
            return;
        }
        
        itemsToRender.forEach(item => {
            const isPinned = pinned.some(p => p.id === item.id);
            const link = document.createElement('a');
            const itemArea = item.hub_area; // pode ser undefined em itens antigos
            // Só troca a área se o item EXPLICITAMENTE pertence a outra área
            if (currentHubUser && currentHubArea && itemArea && itemArea !== currentHubArea) {
                link.href = '#';
                link.addEventListener('click', (e) => {
                    e.preventDefault();
                    switchHubAreaThenNavigate(itemArea, `/dashboards?open=${item.id}`);
                });
            } else {
                link.href = `/dashboards?open=${item.id}`;
            }
            link.className = 'quick-link';
            const itemIcon = document.createElement('i');
            itemIcon.className = item.icon;
            link.appendChild(itemIcon);
            const text = document.createElement('span');
            text.textContent = item.name;
            link.appendChild(text);
            const pinIcon = document.createElement('i');
            pinIcon.className = `fas fa-thumbtack pin-icon ${isPinned ? 'pinned' : ''}`;
            pinIcon.title = isPinned ? 'Desafixar' : 'Fixar no Acesso Rápido';
            pinIcon.addEventListener('click', (e) => {
                e.preventDefault(); 
                e.stopPropagation(); 
                togglePin(item);
            });
            link.appendChild(pinIcon);
            quickLinksContainer.appendChild(link);
        });
    }
    
    renderQuickLinks();

    if (isHubPage && searchBar && searchDropdown && searchDropdownBody) {
        // --- 3. LÓGICA DA BARRA DE PESQUISA (SUBSTITUÍDA PELA VERSÃO DROPDOWN) ---
        searchBar.addEventListener('keyup', (e) => {
        const searchTerm = e.target.value.toLowerCase();

        // Limpa resultados anteriores
        searchDropdownBody.innerHTML = '';

        // Se a barra de pesquisa estiver vazia, esconde o dropdown e sai
        if (searchTerm === "") {
            searchDropdown.classList.remove('visible');
            return;
        }

        let itemsFound = 0;

        // Filtra o índice de busca universal
        searchableIndex.forEach(item => {
            const searchableText = [
                item.name.toLowerCase(),
                item.description.toLowerCase(),
                (item.tags || '').toLowerCase()
            ].join(' ');
            
            if (searchableText.includes(searchTerm)) {
                itemsFound++;
                
                // Cria o link do resultado (com a nova classe)
                const link = document.createElement('a');
                link.href = item.href;
                link.className = 'search-result-item'; // <-- Nova classe de estilo
                
                const itemIcon = document.createElement('i');
                itemIcon.className = item.icon;
                link.appendChild(itemIcon);
                
                const textWrapper = document.createElement('div');
                textWrapper.className = 'search-result-content';

                const title = document.createElement('span');
                title.className = 'search-result-title';
                title.textContent = item.name;
                textWrapper.appendChild(title);

                if (item.description) {
                    const desc = document.createElement('small');
                    desc.className = 'search-result-desc';
                    desc.textContent = item.description;
                    textWrapper.appendChild(desc);
                }

                link.appendChild(textWrapper);

                const tag = document.createElement('span');
                tag.className = 'search-result-tag';
                tag.textContent = item.origin || 'Hub';
                link.appendChild(tag);

                searchDropdownBody.appendChild(link);
            }
        });

        // Mostra mensagem se nenhum resultado for encontrado
        if (itemsFound === 0) {
            searchDropdownBody.innerHTML = '<p class="no-results">Nenhum resultado encontrado.</p>';
        }

        // Mostra o dropdown
        searchDropdown.classList.add('visible');
    });

    // Atalho de teclado Ctrl/Cmd + K para focar na busca
        document.addEventListener('keydown', (event) => {
            if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
                event.preventDefault();
                searchBar.focus();
                searchBar.select();
            }
        });
    }

    // ========================================================
    // ===== 4. LÓGICA (ACESSO, LOGIN HUB, PERFIL) ATUALIZADA =====
    // ========================================================

    /**
     * ATUALIZADO: Atualiza a UI do dropdown, a IMAGEM do botão e a largura mínima.
     */
    function updateAccessDropdown(username = null, profileImageUrl = null, area = null, role = null, displayName = null) {
    currentHubUser = username;
    currentHubOriginArea = area; // Área de origem (do users.json), não altera currentHubArea
    currentHubRole = role;
    currentHubDisplayName = displayName; 
    accessDropdown.innerHTML = ''; // Limpa o conteúdo
    
    currentProfileUrl = profileImageUrl || defaultProfileUrl;
    profileImgThumb.src = currentProfileUrl;
    
    if (username) {
        accessDropdown.style.minWidth = '200px';
    } else {
        accessDropdown.style.minWidth = '180px';
    }

    if (username) {
        let adminButton = '';
        if (username.toLowerCase() === 'admin') {
            adminButton = `<button class="access-dropdown-item" id="access-admin-btn"><i class="fas fa-user-shield"></i>Administração</button>`;
        }

        accessDropdown.innerHTML = `
            <button class="access-dropdown-item" id="access-profile-btn">
                <i class="fas fa-user"></i>Minha Conta
            </button>
            ${adminButton} 
            <button class="access-dropdown-item" id="access-connections-btn">
                <i class="fas fa-plug"></i>Minhas Conexões
            </button>
            <button class="access-dropdown-item danger" id="access-logout-btn">
                <i class="fas fa-sign-out-alt"></i>Deslogar
            </button>
        `;

        // --- CORREÇÃO: Anexa os listeners AQUI, depois de criar os botões ---
        document.getElementById('access-profile-btn').addEventListener('click', () => {
            openProfileModal(username, currentHubDisplayName, currentProfileUrl);
        });
        document.getElementById('access-connections-btn').addEventListener('click', openConnectionsModal);
        document.getElementById('access-logout-btn').addEventListener('click', handleHubLogout);
        
        const adminBtn = document.getElementById('access-admin-btn');
        if (adminBtn) {
            adminBtn.addEventListener('click', openAdminModal);
        }
        // --- FIM DA CORREÇÃO ---
            
    } else {
        accessDropdown.innerHTML = `
            <button class="access-dropdown-item" id="access-login-btn">
                <i class="fas fa-sign-in-alt"></i>Logar
            </button>
            <button class="access-dropdown-item" id="access-register-btn">
                <i class="fas fa-user-plus"></i>Solicitar Acesso
            </button>
        `;
        
        // --- CORREÇÃO: Anexa os listeners AQUI ---
        document.getElementById('access-login-btn').addEventListener('click', openHubLoginModal);
        document.getElementById('access-register-btn').addEventListener('click', openRegisterModal);
        // --- FIM DA CORREÇÃO ---
    }
}

    // --- Lógica de Recorte (Cropper.js) ---

    function openCropperModal(imageSrc) {
        cropperOverlay.style.zIndex = '1200';
        cropperImage.src = imageSrc;
        cropperOverlay.classList.add('visible');

        // Garante que o cropper só seja inicializado quando a imagem estiver carregada
        cropperImage.onload = () => {
            if (cropper) {
                cropper.destroy();
            }
            cropper = new Cropper(cropperImage, {
                aspectRatio: 1, // Quadrado
                viewMode: 1, // Garante que o cropper caiba na tela
                movable: true,
                zoomable: true,
                scalable: false,
                rotatable: false,
                cropBoxMovable: true,
                cropBoxResizable: true,
                background: false
            });
        };
    }
    
    function closeCropperModal() {
        if (cropper) {
            cropper.destroy();
            cropper = null;
        }
        cropperOverlay.classList.remove('visible');
        cropperOverlay.style.zIndex = '';
    }

    cropperCloseBtn.addEventListener('click', closeCropperModal);
    document.getElementById('cropper-close-footer-btn').addEventListener('click', closeCropperModal);
    cropperOverlay.addEventListener('mousedown', (e) => {
        if (e.target === cropperOverlay) closeCropperModal();
    });

    cropperSaveBtn.addEventListener('click', () => {
        if (!cropper || !currentHubUser || !selectedFile) return;

        // 1. Recorta a imagem para um Blob (tipo de arquivo)
        const croppedCanvas = cropper.getCroppedCanvas({
            width: 256,
            height: 256,
        });

        croppedCanvas.toBlob((blob) => {
            if (!blob) {
                profileUploadStatus.textContent = "Erro ao processar imagem.";
                return;
            }
            
            closeCropperModal();
            profileUploadStatus.textContent = "Recorte concluído. Enviando imagem...";

            // 2. Prepara para enviar o Blob como um FormData
            const formData = new FormData();
            // Renomeia o blob com o nome do arquivo original para manter a extensão
            formData.append('file', blob, `${currentHubUser}.${currentUploadExtension}`);

            // 3. Executa o upload
            uploadCroppedImage(formData);
        }, `image/${currentUploadExtension === 'png' ? 'png' : 'jpeg'}`, 0.9);
    });

    // Em: hub.js

function uploadCroppedImage(formData) {
    fetch('/api/profile/upload', {
        method: 'POST',
        body: formData,
    })
    .then(response => response.json())
    .then(data => {
        // --- INÍCIO DA MODIFICAÇÃO (Req 1 & 2) ---
        if (data.status === 'sucesso') {
            // 1. Atualiza a URL da imagem global
            currentProfileUrl = data.url; 
            
            // 2. Chama o update do Header com os dados corretos
            updateAccessDropdown(currentHubUser, currentProfileUrl, currentHubArea, currentHubRole, currentHubDisplayName);
            
            // 3. Atualiza a imagem DENTRO do modal
            const profileImgInModal = document.getElementById('new-profile-avatar-img');
            if (profileImgInModal) {
                profileImgInModal.src = currentProfileUrl;
            }
            
            // 3. (Req: Habilita o botão de remover foto)
            const removerBtn = document.getElementById('profile-edit-dropdown-remover');
            if (removerBtn) {
                removerBtn.style.display = 'flex';
                removerBtn.disabled = false;
            }

        } else {
            alert(data.mensagem || "Erro ao fazer upload.");
        }
    })
    .catch(() => {
        alert("Erro de comunicação com o servidor.");
    })
    .finally(() => {
        // (Req: NÃO reabre o modal de perfil, pois ele já está aberto)
        // A função openProfileModal() foi REMOVIDA daqui.
    });
}

// (Req 1) NOVO: Handler para salvar o nome de usuário
    function handleUpdateDetails(username) {
        const btn = document.getElementById('profile-details-save-btn');
        const input = document.getElementById('profile-display-name-input');
        if (!btn || !input) return;

        const newDisplayName = input.value.trim() || null;
        btn.disabled = true;

        fetch('/api/profile/update-details', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ display_name: newDisplayName })
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                // Atualiza o dropdown do header
                updateAccessDropdown(username, currentProfileUrl, currentHubArea, currentHubRole, data.display_name);
            }
            alert(data.mensagem); // Mostra "Sucesso" ou "Erro"
        })
        .finally(() => {
            btn.disabled = false;
        });
    }

    // (Req 2) NOVO: Handler para alterar a senha
    function handleChangePassword() {
    // (Req 1: CORREÇÃO DO BUG)
    const modal = document.getElementById('profile-overlay');
    const btn = modal.querySelector('#profile-save-btn'); // O botão do footer
    const currentPass = modal.querySelector('#profile-current-pass');
    const newPass = modal.querySelector('#profile-new-pass');
    const confirmPass = modal.querySelector('#profile-confirm-pass');
    const statusEl = modal.querySelector('#profile-password-status');
    
    if (!btn || !currentPass || !newPass || !confirmPass || !statusEl) {
        console.error("Erro: Elementos do formulário de senha não encontrados.");
        return;
    }
    
    statusEl.className = 'hub-form-status hidden';

    if (newPass.value !== confirmPass.value) {
        statusEl.textContent = "A nova senha e a confirmação não correspondem.";
        statusEl.className = 'hub-form-status error visible';
        return;
    }
    if (newPass.value.length < 4) {
        statusEl.textContent = "A nova senha deve ter pelo menos 4 caracteres.";
        statusEl.className = 'hub-form-status error visible';
        return;
    }
    
    // (Req 2: Limite de 16 caracteres)
    if (newPass.value.length > 16) {
        statusEl.textContent = "A nova senha não pode ter mais de 16 caracteres.";
        statusEl.className = 'hub-form-status error visible';
        return;
    }

    btn.disabled = true;
    fetch('/api/profile/change-password', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            current_pass: currentPass.value,
            new_pass: newPass.value
        })
    })
    .then(response => response.json())
    .then(data => {
        statusEl.textContent = data.mensagem; 
        
        if (data.status === 'sucesso') {
            statusEl.className = 'hub-form-status success visible';
            currentPass.value = '';
            newPass.value = '';
            confirmPass.value = '';
        } else {
            statusEl.className = 'hub-form-status error visible';
        }
    })
    .finally(() => {
        btn.disabled = false;
    });
}

    // (Req 3 & 4) NOVO: Helper para renderizar listas no modal
    function renderProfileActivityList(container, items, type) {
        container.innerHTML = '';
        if (!items || items.length === 0) {
            const message = (type === 'tasks') ? 'Nenhuma atividade recente.' : 'Nenhum dashboard acessado.';
            container.innerHTML = `<li class="no-activity">${message}</li>`;
            return;
        }

        items.forEach(item => {
            const li = document.createElement('li');
            
            if (type === 'tasks') {
                // (Req 3) Renderiza Tarefas
                li.className = 'profile-activity-item';
                const jobDate = new Date(item.startTime).toLocaleString('pt-BR', {
                    day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit'
                });
                let icon = 'fa-clock';
                if (item.status === 'done') icon = 'fa-check-circle';
                if (item.status === 'failed') icon = 'fa-times-circle';
                if (item.status === 'pending') icon = 'fa-hourglass-start';
                
                const statusMap = { 'done': 'Concluído', 'pending': 'Pendente', 'running': 'Rodando' };
                const statusText = statusMap[item.status] || '';
                const statusDisplay = statusText ? ` (${statusText})` : '';

                li.innerHTML = `
                    <i class="fas ${icon}"></i>
                    <div class="profile-activity-details">
                        <strong>${item.taskInfo.name}</strong>
                        <span>${jobDate}${statusDisplay}</span>
                    </div>
                `;
            } else {
                // (Req 4) Renderiza Dashboards
                li.className = 'profile-activity-item'; // (Classe base para estilo)
                const link = document.createElement('a'); // (Transforma em link)
                link.href = item.href;
                link.className = 'profile-dashboard-item';
                
                // --- INÍCIO DA MODIFICAÇÃO (Req 1: Mostrar Contagem) ---
                const countHtml = item.count ? `<span>(${item.count} ${item.count > 1 ? 'acessos' : 'acesso'})</span>` : '';
                // --- FIM DA MODIFICAÇÃO ---

                link.innerHTML = `
                    <i class="${item.icon || 'fas fa-chart-pie'}"></i>
                    <div class="profile-activity-details">
                        <strong>${item.name}</strong>
                        ${countHtml}
                    </div>
                `;
                // Anexa o <a> ao <li>
                li.appendChild(link);
            }
            container.appendChild(li);
        });
    }

    // --- Lógica do Modal de Perfil (Input de Arquivo) ---

    function openProfileModal(username, displayName, currentImageUrl) {
    accessDropdown.classList.remove('visible');
    
    const modal = profileOverlay.querySelector('.settings-modal');
    if (!modal) return; 
    
    // --- 1. Mapeamento de Dados ---
    const nameToDisplay = displayName || username;
    const usernameHandle = username; 
    const location = currentHubOriginArea || 'N/A'; 
    const role = currentHubRole || 'N/A'; 
    
    let accessTagsHtml = '';
    if (role === 'Executor' || role === 'Admin') {
        accessTagsHtml = `
            <span class="profile-tag">Automações</span>
            <span class="profile-tag">Dashboards</span>
        `;
    } else if (role === 'Analista') {
        accessTagsHtml = `
            <span class="profile-tag">Dashboards</span>
        `;
    }

    // --- 2. Injeção do Novo HTML (Abas "Experience" e "About" removidas) ---
    modal.innerHTML = `
<div class="new-profile-modal">
    <div class="profile-header-card">
        <div class="profile-avatar-wrapper">
            <img src="${currentImageUrl}" alt="Foto de Perfil" class="profile-avatar" id="new-profile-avatar-img" title="Foto de Perfil">
            <input type="file" id="profile-file-input" name="file" accept="image/png, image/jpeg" class="visually-hidden">
            <button id="profile-edit-btn" class="profile-camera-btn" title="Alterar foto">
                <i class="fas fa-camera"></i>
            </button>
            <div id="profile-edit-dropdown" class="profile-edit-dropdown">
                <button id="profile-edit-dropdown-alterar" class="access-dropdown-item">
                    <i class="fas fa-upload"></i>Alterar Foto
                </button>
                <button id="profile-edit-dropdown-remover" class="access-dropdown-item danger">
                    <i class="fas fa-trash-alt"></i>Remover Foto
                </button>
            </div>
        </div>
        
        <div class="profile-user-info">
            <h2>${nameToDisplay} <i class="fas fa-check-circle" style="color: #007bff; font-size: 1rem;" title="Usuário Verificado"></i></h2>
            <div class="profile-user-meta">
                <span><i class="fas fa-user-tag"></i> ${role}</span>
                <span><i class="fas fa-briefcase"></i> ${location}</span>
            </div>
        </div>
        
        <div class="profile-header-actions">
            <button class="profile-header-btn" id="profile-signout-btn" title="Deslogar">
                <i class="fas fa-sign-out-alt"></i> Deslogar
            </button>
        </div>
        
        <div class="profile-tags">
            ${accessTagsHtml}
        </div>
    </div>

    <nav class="profile-nav-tabs">
        <a href="#" class="profile-nav-link active" data-tab="tab-profile">
            <i class="fas fa-user-circle"></i> Perfil
        </a>
        <a href="#" class="profile-nav-link" data-tab="tab-security">
            <i class="fas fa-lock"></i> Segurança
        </a>
        <a href="#" class="profile-nav-link" data-tab="tab-activity">
            <i class="fas fa-history"></i> Atividade
        </a>
        <a href="#" class="profile-nav-link" data-tab="tab-settings">
            <i class="fas fa-cog"></i> Configurações
        </a>
    </nav>

    <div class="profile-form-section">
    
        <div class="profile-tab-pane active" id="tab-profile">
        <p id="profile-name-status" class="hub-form-status hidden" style="margin-top: -15px;"></p>
            <div class="form-row"> 
                <div class="form-group">
                    <label for="profile-full-name">Nome de Exibição</label>
                    <input type="text" id="profile-full-name" class="profile-input" value="${displayName || ''}" placeholder="Digite um nome..." maxlength="16">
                </div>
                <div class="form-group">
                    <label for="profile-username">Login de Funcionário</label>
                    <input type="text" id="profile-username" class="profile-input" value="${usernameHandle.toUpperCase()}" readonly disabled>
                    <i class="fas fa-check-circle input-check-icon" title="Nome de funcionário verificado"></i>
                </div>
            </div>
        </div>
        
        <div class="profile-tab-pane" id="tab-security">
            </div>

        <div class="profile-tab-pane" id="tab-activity">
            </div>

        <div class="profile-tab-pane" id="tab-settings">
            </div>
    </div>

    <div class="profile-form-footer">
        <button class="button btn-cancel" id="profile-discard-btn">Descartar</button>
        <button class="button btn-save" id="profile-save-btn">Salvar Alterações</button>
    </div>
</div>
    `;

    // --- 3. Re-injeção de Conteúdo Antigo e Listeners ---
    
    // (HTML de Segurança - SEM o botão)
    const securityHTML = `
        <h4>Alterar Senha</h4>
        <p class="profile-section-subtitle">Insira sua senha atual e uma nova senha.</p>
        <p id="profile-password-status" class="hub-form-status hidden"></p>
        <form id="password-change-form" class="password-change-form form-row">
            <div class="form-group">
                <label for="profile-current-pass">Senha Atual:</label>
                <div class="password-input-wrapper">
                    <input type="password" id="profile-current-pass" class="profile-input">
                    <button type="button" class="password-toggle-btn" data-target="profile-current-pass" aria-label="${translate('actions.showPassword', 'Mostrar senha')}" title="${translate('actions.showPassword', 'Mostrar senha')}">
                        <i class="fas fa-eye"></i>
                    </button>
                </div>
            </div>
            <div class="form-group">
                <label for="profile-new-pass">Nova Senha:</label>
                <div class="password-input-wrapper">
                    <input type="password" id="profile-new-pass" class="profile-input" maxlength="16">
                    <button type="button" class="password-toggle-btn" data-target="profile-new-pass" aria-label="${translate('actions.showPassword', 'Mostrar senha')}" title="${translate('actions.showPassword', 'Mostrar senha')}">
                        <i class="fas fa-eye"></i>
                    </button>
                </div>
            </div>
            <div class="form-group">
                <label for="profile-confirm-pass">Confirmar Nova Senha:</label>
                <div class="password-input-wrapper">
                    <input type="password" id="profile-confirm-pass" class="profile-input" maxlength="16">
                    <button type="button" class="password-toggle-btn" data-target="profile-confirm-pass" aria-label="${translate('actions.showPassword', 'Mostrar senha')}" title="${translate('actions.showPassword', 'Mostrar senha')}">
                        <i class="fas fa-eye"></i>
                    </button>
                </div>
            </div>
            </form>
    `;
    
    // (Req 3: HTML de Atividade - SEM Dashboard Mais Acessado)
    const activityHTML = `
        <h4>Agendamentos</h4>
        <p class="profile-section-subtitle">Verifique as últimas tarefas agendadas.</p>
        <ul id="profile-activity-list" class="profile-activity-list new-design">
            <li class="no-activity">Carregando...</li>
        </ul>
        `;

    // (HTML de Configurações - SEM o botão)
    const settingsHTML = `
        <div class="setting-group form-group" style="padding: 0;">
            <label>${translate('settings.cards.label', 'Quantidade de Cards')}</label>
            <div class="count-selector">
                <div class="setting-option card-count-option" data-count="4">
                    <div class="count-icon grid-4" aria-hidden="true">
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                    </div>
                    <span class="count-label">${translate('settings.cards.option4', '4 Espaços')}</span>
                </div>
                <div class="setting-option card-count-option" data-count="6">
                    <div class="count-icon grid-6" aria-hidden="true">
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                    </div>
                    <span class="count-label">${translate('settings.cards.option6', '6 Espaços')}</span>
                </div>
                <div class="setting-option card-count-option" data-count="8">
                    <div class="count-icon grid-8" aria-hidden="true">
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                        <span class="count-square"></span>
                    </div>
                    <span class="count-label">${translate('settings.cards.option8', '8 Espaços')}</span>
                </div>
            </div>
            </div>
        <div class="setting-group form-group" style="padding: 0;">
            <label>${translate('settings.appearance', 'Aparência')}</label>
            <div class="theme-selector theme-selector-compact">
                <div class="theme-option" data-theme="light" title="${translate('settings.theme.lightTitle', 'Modo Claro')}">
                    <i class="fas fa-sun"></i>
                    <span>${translate('settings.theme.light', 'Claro')}</span>
                </div>
                <div class="theme-option" data-theme="dark" title="${translate('settings.theme.darkTitle', 'Modo Escuro')}">
                    <i class="fas fa-moon"></i>
                    <span>${translate('settings.theme.dark', 'Escuro')}</span>
                </div>
                <div class="theme-option" data-theme="system" title="${translate('settings.theme.systemTitle', 'Padrão do Sistema')}">
                    <i class="fas fa-desktop"></i>
                    <span>${translate('settings.theme.system', 'Sistema')}</span>
                </div>
            </div>
        </div>
        <div class="setting-group form-group language-settings" style="padding: 0;">
            <label>${translate('profile.language.label', 'Idioma')}</label>
            <p class="helper-text language-inline-hint">${translate('profile.language.selectLabel', 'Selecione o idioma de exibição.')}</p>
            <div class="language-selector">
                <select id="profile-language-select" class="profile-input" aria-label="${translate('profile.language.label', 'Idioma')}">
                    <option value="pt">${translate('profile.language.option.pt', 'Português')}</option>
                    <option value="en">${translate('profile.language.option.en', 'Inglês')}</option>
                </select>
            </div>
            <p id="profile-language-status" class="hub-form-status hidden" aria-live="polite"></p>
        </div>
    `;
    
    modal.querySelector('#tab-security').innerHTML = securityHTML;
    modal.querySelector('#tab-activity').innerHTML = activityHTML;
    modal.querySelector('#tab-settings').innerHTML = settingsHTML; 
    initializeThemeOptions(modal);

    let profileLanguageSelect = modal.querySelector('#profile-language-select');
    const profileLanguageStatus = modal.querySelector('#profile-language-status');
    let pendingLanguagePreference = (window.hubI18n && typeof hubI18n.getLanguage === 'function') ? hubI18n.getLanguage() : 'pt';
    let languageStatusTimeout = null;

    const profileTabState = {
        initialDisplayName: (displayName || '').trim(),
        pendingDisplayName: (displayName || '').trim(),
        dirty: false
    };
    const settingsTabState = {
        initialLanguage: pendingLanguagePreference,
        pendingLanguage: pendingLanguagePreference,
        dirty: false
    };
    const securityTabState = { dirty: false };
    const securityInputs = {
        current: modal.querySelector('#profile-current-pass'),
        newPass: modal.querySelector('#profile-new-pass'),
        confirm: modal.querySelector('#profile-confirm-pass')
    };
    const passwordStatusEl = modal.querySelector('#profile-password-status');
    const passwordToggleButtons = modal.querySelectorAll('.password-toggle-btn');

    const getPasswordToggleLabel = (visible) => visible
        ? translate('actions.hidePassword', 'Ocultar senha')
        : translate('actions.showPassword', 'Mostrar senha');

    passwordToggleButtons.forEach(btn => {
        const targetId = btn.dataset.target;
        const input = targetId ? modal.querySelector(`#${targetId}`) : null;
        if (!input) {
            btn.disabled = true;
            return;
        }
        btn.setAttribute('aria-pressed', 'false');
        btn.addEventListener('click', () => {
            const willShow = input.type === 'password';
            input.type = willShow ? 'text' : 'password';
            btn.setAttribute('aria-pressed', willShow ? 'true' : 'false');
            const icon = btn.querySelector('i');
            if (icon) {
                icon.className = willShow ? 'fas fa-eye-slash' : 'fas fa-eye';
            }
            const label = getPasswordToggleLabel(willShow);
            btn.setAttribute('aria-label', label);
            btn.setAttribute('title', label);
        });
    });

    const closeProfileModal = () => profileOverlay.classList.remove('visible');
    const getActiveProfileTabId = () => {
        const activeLink = modal.querySelector('.profile-nav-link.active');
        return activeLink ? activeLink.dataset.tab : 'tab-profile';
    };

    const applyFooterButtonConfig = ({ primary, secondary }) => {
        const footer = modal.querySelector('.profile-form-footer');
        if (!footer) return;
        footer.style.display = 'flex';

        if (primary) {
            const saveBtn = footer.querySelector('#profile-save-btn');
            if (saveBtn) {
                const newBtn = saveBtn.cloneNode(true);
                newBtn.textContent = primary.text ?? newBtn.textContent;
                newBtn.className = primary.className ?? newBtn.className;
                newBtn.style.display = primary.display ?? 'block';
                newBtn.disabled = !!primary.disabled;
                if (primary.onClick) {
                    newBtn.addEventListener('click', primary.onClick);
                }
                saveBtn.parentNode.replaceChild(newBtn, saveBtn);
            }
        }

        if (secondary) {
            const discardBtn = footer.querySelector('#profile-discard-btn');
            if (discardBtn) {
                const newBtn = discardBtn.cloneNode(true);
                newBtn.textContent = secondary.text ?? newBtn.textContent;
                newBtn.className = secondary.className ?? newBtn.className;
                if (secondary.display) {
                    newBtn.style.display = secondary.display;
                } else {
                    newBtn.style.display = 'block';
                }
                newBtn.disabled = !!secondary.disabled;
                if (secondary.onClick) {
                    newBtn.addEventListener('click', secondary.onClick);
                }
                discardBtn.parentNode.replaceChild(newBtn, discardBtn);
            }
        }
    };

    const setProfileDirtyFlag = (dirty) => {
        profileTabState.dirty = dirty;
        configureProfileFooterButtons();
    };

    const resetProfileDisplayNameField = () => {
        const modalInstance = profileOverlay.querySelector('.settings-modal');
        if (!modalInstance) return;
        const nameField = modalInstance.querySelector('#profile-full-name');
        const statusEl = modalInstance.querySelector('#profile-name-status');
        if (statusEl) {
            statusEl.className = 'hub-form-status hidden';
        }
        if (nameField) {
            const restoredValue = profileTabState.initialDisplayName || '';
            nameField.value = restoredValue;
            profileTabState.pendingDisplayName = restoredValue;
            nameField.dispatchEvent(new Event('input'));
        } else {
            setProfileDirtyFlag(false);
        }
    };

    const setSettingsDirtyFlag = (dirty) => {
        settingsTabState.dirty = dirty;
        configureSettingsFooterButtons();
    };

    const setSecurityDirtyFlag = (dirty) => {
        securityTabState.dirty = dirty;
        configureSecurityFooterButtons();
    };

    const clearSecurityFields = (resetStatus = true) => {
        Object.values(securityInputs).forEach(input => {
            if (input) input.value = '';
        });
        if (resetStatus && passwordStatusEl) {
            passwordStatusEl.textContent = '';
            passwordStatusEl.className = 'hub-form-status hidden';
        }
        setSecurityDirtyFlag(false);
    };

    const evaluateSecurityDirtyState = () => {
        const hasValue = Object.values(securityInputs).some(input => input && input.value.trim() !== '');
        setSecurityDirtyFlag(hasValue);
    };

    const showLanguageStatus = (variant, key, autoHide = false) => {
        if (!profileLanguageStatus) return;
        const message = key ? translate(key, '') : '';
        if (!message) {
            profileLanguageStatus.textContent = '';
            profileLanguageStatus.className = 'hub-form-status hidden';
            return;
        }
        profileLanguageStatus.textContent = message;
        const variantClass = variant === 'error' ? 'error' : variant === 'info' ? 'info' : 'success';
        profileLanguageStatus.className = `hub-form-status ${variantClass} visible`;
        if (autoHide) {
            clearTimeout(languageStatusTimeout);
            languageStatusTimeout = setTimeout(() => {
                profileLanguageStatus.className = 'hub-form-status hidden';
            }, 3000);
        }
    };

    if (profileLanguageSelect) {
        profileLanguageSelect.value = pendingLanguagePreference;
        profileLanguageSelect.addEventListener('change', () => {
            const selectedLang = profileLanguageSelect.value;
            pendingLanguagePreference = selectedLang;
            settingsTabState.pendingLanguage = selectedLang;
            showLanguageStatus(null, null);
            setSettingsDirtyFlag(selectedLang !== settingsTabState.initialLanguage);
        });
    }

    function handleProfileTabSave() {
        const modal = profileOverlay.querySelector('.settings-modal');
        if (!modal) return Promise.resolve(false);
        const statusEl = modal.querySelector('#profile-name-status');
        const nameField = modal.querySelector('#profile-full-name');
        if (!statusEl || !nameField) {
            console.error('Elementos de nome não encontrados no modal de perfil.');
            return Promise.resolve(false);
        }
        const newDisplayName = nameField.value.trim() || null;
        profileTabState.pendingDisplayName = nameField.value.trim();

        statusEl.className = 'hub-form-status hidden';

        if (newDisplayName && newDisplayName.length > 16) {
            statusEl.textContent = translate('profile.displayName.tooLong', 'O Nome de Exibição não pode ter mais de 16 caracteres.');
            statusEl.className = 'hub-form-status error visible';
            return Promise.resolve(false);
        }

        return fetch('/api/profile/update-details', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ display_name: newDisplayName })
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                currentHubDisplayName = data.display_name;
                profileTabState.initialDisplayName = (data.display_name || '').trim();
                profileTabState.pendingDisplayName = profileTabState.initialDisplayName;
                setProfileDirtyFlag(false);
                updateAccessDropdown(currentHubUser, currentProfileUrl, currentHubArea, currentHubRole, currentHubDisplayName);
                modal.querySelector('.profile-user-info h2').innerHTML = `${currentHubDisplayName || username} <i class="fas fa-check-circle" style="color: #007bff; font-size: 1rem;" title="Usuário Verificado"></i>`;

                statusEl.textContent = translate('profile.displayName.saved', 'Nome de exibição salvo.');
                statusEl.className = 'hub-form-status success visible';
                return true;
            }

            statusEl.textContent = data.mensagem;
            statusEl.className = 'hub-form-status error visible';
            return false;
        })
        .finally(() => {
            setTimeout(() => {
                if (statusEl) {
                    statusEl.className = 'hub-form-status hidden';
                }
            }, 3000);
        });
    }

    function handleSaveSettings() {
        const selectedLang = settingsTabState.pendingLanguage || pendingLanguagePreference || 'pt';
        const applyPromise = window.hubI18n && typeof hubI18n.setLanguage === 'function'
            ? hubI18n.setLanguage(selectedLang, { persist: true })
            : Promise.resolve(selectedLang);

        return applyPromise
            .then(() => {
                pendingLanguagePreference = selectedLang;
                settingsTabState.initialLanguage = selectedLang;
                setSettingsDirtyFlag(false);
                broadcastLanguageChange(selectedLang);
                if (isHubPage) {
                    refreshHubSettingsLanguageUI(selectedLang);
                }
                return true;
            })
            .catch(() => {
                showLanguageStatus('error', 'profile.language.statusError');
                return false;
            });
    }

    function handleChangePassword() {
        const modal = profileOverlay.querySelector('.settings-modal');
        if (!modal) return Promise.resolve(false);
        const btn = modal.querySelector('#profile-save-btn');
        const currentPass = securityInputs.current;
        const newPass = securityInputs.newPass;
        const confirmPass = securityInputs.confirm;

        if (!btn || !currentPass || !newPass || !confirmPass || !passwordStatusEl) {
            console.error('Erro: Elementos do formulário de senha não encontrados.');
            return Promise.resolve(false);
        }

        passwordStatusEl.className = 'hub-form-status hidden';

        if (newPass.value !== confirmPass.value) {
            passwordStatusEl.textContent = 'A nova senha e a confirmação não correspondem.';
            passwordStatusEl.className = 'hub-form-status error visible';
            return Promise.resolve(false);
        }
        if (newPass.value.length < 4) {
            passwordStatusEl.textContent = 'A nova senha deve ter pelo menos 4 caracteres.';
            passwordStatusEl.className = 'hub-form-status error visible';
            return Promise.resolve(false);
        }
        if (newPass.value.length > 16) {
            passwordStatusEl.textContent = 'A nova senha não pode ter mais de 16 caracteres.';
            passwordStatusEl.className = 'hub-form-status error visible';
            return Promise.resolve(false);
        }

        btn.disabled = true;
        return fetch('/api/profile/change-password', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                current_pass: currentPass.value,
                new_pass: newPass.value
            })
        })
        .then(response => response.json())
        .then(data => {
            passwordStatusEl.textContent = data.mensagem;
            if (data.status === 'sucesso') {
                passwordStatusEl.className = 'hub-form-status success visible';
                clearSecurityFields(false);
                return true;
            }

            passwordStatusEl.className = 'hub-form-status error visible';
            return false;
        })
        .finally(() => {
            btn.disabled = false;
        });
    }

    function configureProfileFooterButtons(force = false) {
        if (!force && getActiveProfileTabId() !== 'tab-profile') return;
        applyFooterButtonConfig({
            primary: profileTabState.dirty
                ? {
                    text: translate('actions.saveChanges', 'Salvar Alterações'),
                    className: 'button btn-save',
                    display: 'block',
                    onClick: () => handleProfileTabSave()
                }
                : {
                    text: translate('actions.close', 'Fechar'),
                    className: 'button btn-save',
                    display: 'block',
                    onClick: closeProfileModal
                },
            secondary: {
                text: translate('actions.discard', 'Descartar'),
                className: 'button btn-cancel',
                display: 'block',
                onClick: () => resetProfileDisplayNameField()
            }
        });
    }

    function configureSecurityFooterButtons(force = false) {
        if (!force && getActiveProfileTabId() !== 'tab-security') return;
        applyFooterButtonConfig({
            primary: securityTabState.dirty
                ? {
                    text: translate('actions.savePassword', 'Salvar Senha'),
                    className: 'button btn-save',
                    display: 'block',
                    onClick: () => handleChangePassword()
                }
                : {
                    text: translate('actions.close', 'Fechar'),
                    className: 'button btn-save',
                    display: 'block',
                    onClick: closeProfileModal
                },
            secondary: {
                text: translate('actions.discard', 'Descartar'),
                className: 'button btn-cancel',
                display: 'block',
                onClick: () => clearSecurityFields()
            }
        });
    }

    function configureSettingsFooterButtons(force = false) {
        if (!force && getActiveProfileTabId() !== 'tab-settings') return;
        applyFooterButtonConfig({
            primary: settingsTabState.dirty
                ? {
                    text: translate('actions.saveChanges', 'Salvar Alterações'),
                    className: 'button btn-save',
                    display: 'block',
                    onClick: () => handleSaveSettings()
                }
                : {
                    text: translate('actions.close', 'Fechar'),
                    className: 'button btn-save',
                    display: 'block',
                    onClick: closeProfileModal
                },
            secondary: {
                text: translate('actions.clearCards', 'Limpar Cards'),
                className: 'button btn-danger-outline',
                display: 'block',
                onClick: () => {
                    const confirmMessage = translate('settings.clearCardsConfirm', 'Tem certeza que deseja limpar todos os cards do Acesso Rápido?');
                    if (confirm(confirmMessage)) {
                        clearRecents();
                    }
                }
            }
        });
    }

    function configureActivityFooterButtons() {
        applyFooterButtonConfig({
            primary: {
                text: translate('actions.close', 'Fechar'),
                className: 'button btn-save',
                display: 'block',
                onClick: closeProfileModal
            },
            secondary: {
                display: 'none'
            }
        });
    }

    function updateFooterButtons(activeTabId) {
        switch (activeTabId) {
            case 'tab-profile':
                configureProfileFooterButtons(true);
                break;
            case 'tab-security':
                configureSecurityFooterButtons(true);
                break;
            case 'tab-settings':
                configureSettingsFooterButtons(true);
                break;
            default:
                configureActivityFooterButtons();
                break;
        }
    }
    
    // --- 6. Anexa os Listeners aos Elementos ---
    
    // (Listener das Abas)
    modal.querySelectorAll('.profile-nav-link').forEach(tab => {
        tab.addEventListener('click', (e) => {
            e.preventDefault();
            if (tab.classList.contains('disabled-feature')) {
                alert('Esta funcionalidade é apenas para fins de design no momento.');
                return;
            }

            modal.querySelectorAll('.profile-nav-link').forEach(t => t.classList.remove('active'));
            modal.querySelectorAll('.profile-tab-pane').forEach(p => p.classList.remove('active'));

            tab.classList.add('active');
            const tabId = tab.dataset.tab;
            modal.querySelector(`#${tabId}`).classList.add('active');

            updateFooterButtons(tabId);

            // (Req 3: Lógica de Atividade ATUALIZADA - sem dash)
            if (tabId === 'tab-activity') {
                const listEl = document.getElementById('profile-activity-list');
                fetch('/api/profile/get-activity')
                    .then(res => res.json())
                    .then(data => {
                        if (data.status === 'sucesso') {
                            renderProfileActivityList(listEl, data.activity.slice(0, 3), 'tasks');
                        }
                    });
                // (Lógica do Dashboard Mais Acessado REMOVIDA daqui)
            }
            else if (tabId === 'tab-settings') {
                if (typeof applyCountSetting === 'function') {
                    applyCountSetting(localStorage.getItem('hubItemCount') || 4, modal); 
                }
            }
        });
    });
    
    const nameInput = modal.querySelector('#profile-full-name');
    const profileNameHeading = modal.querySelector('.profile-user-info h2');
    const updateHeadingLive = () => {
        if (!profileNameHeading) return;
        const liveValue = nameInput ? nameInput.value.trim() : '';
        const displayValue = liveValue || currentHubDisplayName || nameToDisplay || username;
        profileNameHeading.innerHTML = `${displayValue} <i class="fas fa-check-circle" style="color: #007bff; font-size: 1rem;" title="Usuário Verificado"></i>`;
    };

    if (nameInput) {
        nameInput.addEventListener('input', () => {
            const statusEl = modal.querySelector('#profile-name-status');
            if (statusEl) {
                statusEl.className = 'hub-form-status hidden';
            }
            profileTabState.pendingDisplayName = nameInput.value.trim();
            setProfileDirtyFlag(profileTabState.pendingDisplayName !== profileTabState.initialDisplayName);
            updateHeadingLive();
        });
    }

    Object.values(securityInputs).forEach(input => {
        if (input) {
            input.addEventListener('input', () => {
                if (passwordStatusEl) {
                    passwordStatusEl.className = 'hub-form-status hidden';
                }
                evaluateSecurityDirtyState();
            });
        }
    });
    evaluateSecurityDirtyState();
    updateHeadingLive();

    // (Listeners do Header do Modal)
    modal.querySelector('#profile-signout-btn').addEventListener('click', handleHubLogout);
    
    // (Listeners de Upload de Foto)
    const profileEditBtn = modal.querySelector('#profile-edit-btn');
    const profileEditDropdown = modal.querySelector('#profile-edit-dropdown');
    const profileAlterarBtn = modal.querySelector('#profile-edit-dropdown-alterar');
    const profileRemoverBtn = modal.querySelector('#profile-edit-dropdown-remover');
    const newFileInput = modal.querySelector('#profile-file-input');

    profileEditBtn.addEventListener('click', () => {
        profileEditDropdown.classList.toggle('visible');
    });
    profileAlterarBtn.addEventListener('click', () => {
        newFileInput.click(); 
        profileEditDropdown.classList.remove('visible');
    });
    profileRemoverBtn.addEventListener('click', () => {
        if (!currentHubUser || profileRemoverBtn.disabled) return;
        profileRemoverBtn.disabled = true;
        profileEditDropdown.classList.remove('visible');
        handleRemoveProfileImage(username, displayName); 
    });
    if (currentImageUrl !== defaultProfileUrl) {
        profileRemoverBtn.style.display = 'flex';
        profileRemoverBtn.disabled = false;
    } else {
        profileRemoverBtn.style.display = 'none';
        profileRemoverBtn.disabled = true;
    }
    newFileInput.addEventListener('change', (e) => {
        const file = e.target.files[0];
        if (!file) return;
        const fileNameParts = file.name.split('.');
        currentUploadExtension = fileNameParts.length > 1 ? fileNameParts.pop().toLowerCase() : 'jpg';
        if (!['png', 'jpg', 'jpeg'].includes(currentUploadExtension)) {
            alert(translate('profile.image.invalidType', 'Apenas arquivos PNG ou JPG são permitidos.'));
            newFileInput.value = null; return;
        }
        selectedFile = file;
        const reader = new FileReader();
        reader.onload = (e) => openCropperModal(e.target.result);
        reader.readAsDataURL(file);
        newFileInput.value = null;
    });

    // (Listeners das Configurações Movidas)
    modal.querySelectorAll('#tab-settings .count-selector .setting-option').forEach(option => {
        option.addEventListener('click', () => {
            if (typeof applyCountSetting === 'function') {
                applyCountSetting(option.dataset.count, modal); 
            }
        });
    });

    // --- 7. Define o estado inicial do Footer ---
    updateFooterButtons('tab-profile'); // Define o estado inicial para a aba "Perfil"

    // --- 8. Abre o Modal ---
    profileOverlay.classList.add('visible');
}

function handleRemoveProfileImage(username, displayName) {
    const profilePreviewImg = document.getElementById('new-profile-avatar-img'); // (Seletor do novo modal)
    const profileRemoverBtn = document.getElementById('profile-edit-dropdown-remover'); // (Seletor do novo modal)

    fetch('/api/profile/remove-image', { method: 'POST' })
    .then(response => response.json())
    .then(data => {
        if (data.status === 'sucesso') {
            currentProfileUrl = data.default_url;
            updateAccessDropdown(username, data.default_url, currentHubArea, currentHubRole, displayName);
            
            if (profilePreviewImg) {
                profilePreviewImg.src = data.default_url;
            }
            if (profileRemoverBtn) {
                profileRemoverBtn.style.display = 'none'; // Esconde a opção
            }
        } else {
            alert(data.mensagem);
            if (profileRemoverBtn) {
                profileRemoverBtn.disabled = false; // Reabilita se falhar
            }
        }
    })
    .catch(() => {
        alert(translate('profile.image.removeCommunicationError', 'Erro de comunicação ao remover imagem.'));
        if (profileRemoverBtn) {
            profileRemoverBtn.disabled = false; // Reabilita se falhar
        }
    });
}
    
    profileCloseBtn.addEventListener('click', () => profileOverlay.classList.remove('visible'));
    profileOverlay.addEventListener('mousedown', (e) => {
        // --- INÍCIO DA MODIFICAÇÃO (Req 1: Fechar Dropdown ao Clicar Fora) ---
        
        // Elementos relevantes (podem não existir se o modal não estiver aberto)
        const dropdown = document.getElementById('profile-edit-dropdown');
        const cameraBtn = document.getElementById('profile-edit-btn');
        
        // 1. Lógica original: Fechar o modal ao clicar no fundo
        if (e.target === profileOverlay) {
            profileOverlay.classList.remove('visible');
            // Garante que o dropdown feche junto
            if (dropdown) dropdown.classList.remove('visible');
            return;
        }

        // 2. Lógica nova: Fechar o mini-dropdown ao clicar no corpo do modal
        if (dropdown && cameraBtn) {
            // Verifica se o dropdown está visível
            if (dropdown.classList.contains('visible')) {
                // Verifica se o clique NÃO foi no botão da câmera
                const isCameraClick = cameraBtn.contains(e.target);
                // Verifica se o clique NÃO foi dentro do dropdown
                const isDropdownClick = dropdown.contains(e.target);

                if (!isCameraClick && !isDropdownClick) {
                    dropdown.classList.remove('visible');
                }
            }
        }
        // --- FIM DA MODIFICAÇÃO ---
    });

    // Dispara o modal de recorte ao selecionar um arquivo
    profileFileInput.addEventListener('change', (e) => {
        const file = e.target.files[0];
        if (!file) return;

        const fileNameParts = file.name.split('.');
        currentUploadExtension = fileNameParts.length > 1 ? fileNameParts.pop().toLowerCase() : 'jpg';

        // Verifica o tipo de arquivo
        if (!['png', 'jpg', 'jpeg'].includes(currentUploadExtension)) {
            profileUploadStatus.textContent = translate('profile.image.invalidType', 'Apenas arquivos PNG ou JPG são permitidos.');
            profileFileInput.value = null; // Limpa o input
            return;
        }

        selectedFile = file;
        const reader = new FileReader();
        reader.onload = (e) => {
            // Abre o modal de recorte com a imagem selecionada
            openCropperModal(e.target.result);
        };
        reader.readAsDataURL(file);
        
        // --- CORREÇÃO (Req. 2): Reseta o input para permitir selecionar o mesmo arquivo novamente ---
        profileFileInput.value = null;
        // ----------------------------------------------------------------------------------
    });
    
    // Lógica para o novo botão "Remover Imagem"
profileRemoveBtn.addEventListener('click', () => {
    if (!currentHubUser || profileRemoveBtn.disabled) return;
    
    // --- CORREÇÃO (Req. 3): Remoção do popup de confirmação ---
    // if (!confirm("Tem certeza que deseja remover sua foto de perfil?")) return;
    
    profileUploadStatus.textContent = "Removendo imagem...";
    profileRemoveBtn.disabled = true; // Desabilita durante a remoção
    
    fetch('/api/profile/remove-image', { method: 'POST' })
    .then(response => response.json())
    .then(data => {
        if (data.status === 'sucesso') {
            profileUploadStatus.textContent = data.mensagem;
            
            // --- CORREÇÃO (Req. 4): Atualiza a URL local ---
            currentProfileUrl = data.default_url; 

            updateAccessDropdown(currentHubUser, data.default_url, currentHubArea);
            profilePreviewImg.src = data.default_url;
            profileRemoveBtn.classList.add('hidden'); // Esconde o botão
        } else {
            profileUploadStatus.textContent = data.mensagem || "Erro ao remover imagem.";
            profileRemoveBtn.disabled = false; // Reabilita se falhar
        }
    })
    .catch(() => {
    profileUploadStatus.textContent = translate('profile.image.removeCommunicationError', 'Erro de comunicação ao remover imagem.');
        profileRemoveBtn.disabled = false; // Reabilita se falhar
    });
});

function openEditNameModal(currentName) {
        editNameInput.value = currentName || '';
        editNameOverlay.classList.add('visible');
        editNameInput.focus();
    }
    
    // (Req 1) NOVO: Fecha o modal de edição de nome
    function closeEditNameModal() {
        editNameOverlay.classList.remove('visible');
    }
    
    // (Req 1) NOVO: Handler para salvar o nome (substitui handleUpdateDetails)
    function handleSaveName() {
        const newDisplayName = editNameInput.value.trim() || null;
        editNameSaveBtn.disabled = true;

        if (newDisplayName && newDisplayName.length > 16) {
            alert("Erro: O Nome de Usuário não pode ter mais de 16 caracteres.");
            editNameSaveBtn.disabled = false;
            return;
        }
    
        fetch('/api/profile/update-details', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ display_name: newDisplayName })
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                // 1. Atualiza a variável global
                currentHubDisplayName = data.display_name;
                
                // 2. Atualiza o header
                updateAccessDropdown(currentHubUser, currentProfileUrl, currentHubArea, currentHubRole, currentHubDisplayName);
                
                // 3. Atualiza o texto na aba "Perfil" (que está aberta no fundo)
                const nameSpan = document.getElementById('profile-name-span');
                if (nameSpan) {
                    nameSpan.textContent = currentHubDisplayName || 'Não definido';
                }

                // 4. Fecha o modal de edição
                closeEditNameModal();
                
                // (Não mostramos alerta de sucesso)

            } else {
                // (Mostra o alerta APENAS se for um erro)
                alert(data.mensagem);
            }
            // --- FIM DA MODIFICAÇÃO ---
        })
        .finally(() => {
            editNameSaveBtn.disabled = false;
        });
    }

    // --- Lógica de Login/Sessão (Ajustadas para imagem) ---
    function openHubLoginModal() {
        accessDropdown.classList.remove('visible');
        hubLoginError.classList.add('hidden');
        hubUserInput.value = '';
        hubPassInput.value = '';
        hubLoginOverlay.classList.add('visible');
        hubUserInput.focus();
        updateHubLoginButtonState();
    }
    function closeHubLoginModal() {
        hubLoginOverlay.classList.remove('visible');
    }
    function showHubLoginError(message) {
        hubLoginError.textContent = message;
        hubLoginError.classList.remove('hidden');
    }

    function updateHubLoginButtonState() {
        if (!hubLoginSubmitBtn) return;
        const hasText = hubUserInput.value.trim().length > 0 || hubPassInput.value.trim().length > 0;
        if (hasText) {
            hubLoginSubmitBtn.classList.remove('hidden');
        } else {
            hubLoginSubmitBtn.classList.add('hidden');
        }
    }

    function resetRegisterSuccessView() {
        registerCodeChars.forEach(char => char.textContent = '•');
        if (registerQrImage) {
            registerQrImage.removeAttribute('src');
            registerQrImage.alt = 'QR Code ainda não gerado';
        }
        if (registerCopyCodeBtn) {
            registerCopyCodeBtn.disabled = true;
            registerCopyCodeBtn.classList.remove('copied');
            const label = registerCopyCodeBtn.querySelector('span');
            if (label) label.textContent = 'Copiar';
        }
        if (tokenGenerated) {
            tokenGenerated.textContent = '';
        }
        updateRegisterSubmitButtonState();
    }

    function showRegisterSuccessCard(code) {
        if (!registerTokenDisplay) return;
        const formattedCode = (code || '').toUpperCase();
        registerTokenDisplay.classList.remove('hidden');
        if (registerFieldsWrapper) {
            registerFieldsWrapper.classList.add('hidden');
        }
        registerCodeChars.forEach((char, idx) => {
            char.textContent = formattedCode[idx] || '•';
        });
        if (tokenGenerated) {
            tokenGenerated.textContent = formattedCode;
        }
        if (registerQrImage && formattedCode) {
            const qrUrl = `https://api.qrserver.com/v1/create-qr-code/?size=180x180&data=${encodeURIComponent(formattedCode)}`;
            registerQrImage.src = qrUrl;
            registerQrImage.alt = `QR Code para o código ${formattedCode}`;
        }
        if (registerCopyCodeBtn) {
            registerCopyCodeBtn.disabled = !formattedCode;
            registerCopyCodeBtn.classList.remove('copied');
            const label = registerCopyCodeBtn.querySelector('span');
            if (label) label.textContent = 'Copiar';
        }
        updateRegisterSubmitButtonState();
    }

    function handleRegisterCodeCopy() {
        if (!registerCopyCodeBtn || !tokenGenerated) return;
        const code = tokenGenerated.textContent.trim();
        if (!code) return;

        const applyFeedback = () => {
            registerCopyCodeBtn.classList.add('copied');
            const label = registerCopyCodeBtn.querySelector('span');
            if (label) label.textContent = 'Copiado!';
            clearTimeout(registerCopyFeedbackTimer);
            registerCopyFeedbackTimer = setTimeout(() => {
                registerCopyCodeBtn.classList.remove('copied');
                if (label) label.textContent = 'Copiar';
            }, 2000);
        };

        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(code).then(applyFeedback).catch(() => {
                fallbackCopy(code, applyFeedback);
            });
        } else {
            fallbackCopy(code, applyFeedback);
        }
    }

    function fallbackCopy(text, onSuccess) {
        const tempInput = document.createElement('input');
        tempInput.value = text;
        document.body.appendChild(tempInput);
        tempInput.select();
        try {
            document.execCommand('copy');
            if (typeof onSuccess === 'function') onSuccess();
        } catch (err) {
            console.error('Não foi possível copiar o código.', err);
        }
        document.body.removeChild(tempInput);
    }

    function hideConsultAccessInfo() {
        if (consultAccessInfo) {
            consultAccessInfo.classList.add('hidden');
        }
        if (consultAccessUsername) {
            consultAccessUsername.textContent = '—';
        }
        if (consultAccessPassword) {
            consultAccessPassword.textContent = '—';
        }
    }

    function hideConsultRejectedInfo() {
        if (consultRejectedInfo) {
            consultRejectedInfo.classList.add('hidden');
        }
        if (consultRejectedText) {
            consultRejectedText.textContent = '—';
        }
    }

    function showConsultAccessInfo(usernameText, passwordText) {
        if (!consultAccessInfo) return;
        consultAccessInfo.classList.remove('hidden');
        if (consultAccessUsername) {
            consultAccessUsername.textContent = usernameText || '—';
        }
        if (consultAccessPassword) {
            consultAccessPassword.textContent = passwordText || '—';
        }
    }

    function showConsultRejectedInfo(justificationText) {
        if (!consultRejectedInfo) return;
        consultRejectedInfo.classList.remove('hidden');
        if (consultRejectedText) {
            consultRejectedText.textContent = justificationText || '—';
        }
    }

    function getConsultCodeValue() {
        return consultCodeInputs.map(input => input.value.trim().toUpperCase()).join('');
    }

    function updateConsultButtonState(codeValue) {
        if (!consultTokenBtn) return;
        const value = typeof codeValue === 'string' ? codeValue : getConsultCodeValue();
        const hasFullCode = value.length === ACCESS_CODE_LENGTH;
        const isConsultTabActive = tabConsult ? tabConsult.classList.contains('active') : false;
        consultTokenBtn.disabled = !hasFullCode || consultFetchInProgress;
        if (isConsultTabActive && hasFullCode) {
            consultTokenBtn.classList.remove('hidden');
        } else {
            consultTokenBtn.classList.add('hidden');
        }
    }

    function syncConsultCodeInputs() {
        const value = getConsultCodeValue();
        if (consultToken) {
            consultToken.value = value;
        }
        updateConsultButtonState(value);
    }

    function resetConsultCodeInputs() {
        consultCodeInputs.forEach(input => {
            input.value = '';
        });
        if (consultToken) {
            consultToken.value = '';
        }
        consultFetchInProgress = false;
        updateConsultButtonState('');
        resetConsultProgress();
    }
    
    function setConsultProgressState(item, state) {
        if (!item) return;
        item.classList.remove('is-complete', 'is-pending', 'is-rejected');
        if (state) {
            item.classList.add(state);
        }
    }

    function resetConsultProgress() {
        if (!consultProgressWrapper) return;
        consultProgressWrapper.classList.add('hidden');
        setConsultProgressState(consultProgressRequestedItem, 'is-pending');
        setConsultProgressState(consultProgressAwaitingItem, 'is-pending');
        if (consultProgressSecondLabel) {
            consultProgressSecondLabel.textContent = 'Aguardando Aprovação';
        }
    }

    function showConsultAwaitingProgress() {
        if (!consultProgressWrapper) return;
        consultProgressWrapper.classList.remove('hidden');
        setConsultProgressState(consultProgressRequestedItem, 'is-complete');
        setConsultProgressState(consultProgressAwaitingItem, 'is-pending');
        if (consultProgressSecondLabel) {
            consultProgressSecondLabel.textContent = 'Aguardando Aprovação';
        }
    }

    function hideConsultProgress() {
        if (consultProgressWrapper) {
            consultProgressWrapper.classList.add('hidden');
        }
    }
    
    function distributeConsultCodeChars(startIndex, chars) {
        if (!chars || !chars.length) return 0;
        const sanitized = chars.replace(/[^A-Z0-9]/g, '').toUpperCase();
        let inserted = 0;
        for (let i = 0; i < sanitized.length; i++) {
            const targetIndex = startIndex + i;
            if (!consultCodeInputs[targetIndex]) break;
            consultCodeInputs[targetIndex].value = sanitized[i];
            inserted++;
        }
        return inserted;
    }

    function focusConsultCodeInput(index) {
        const target = consultCodeInputs[index];
        if (target) {
            target.focus();
            target.select();
        }
    }

    function handleConsultCodeInputEvent(event, index) {
        let value = event.target.value || '';
        value = value.toUpperCase().replace(/[^A-Z0-9]/g, '');

        if (!value) {
            event.target.value = '';
            syncConsultCodeInputs();
            return;
        }

        const chars = value.split('');
        const firstChar = chars.shift();
        event.target.value = firstChar;
        const extraFilled = distributeConsultCodeChars(index + 1, chars.join(''));
        const advance = 1 + extraFilled;
        const nextIndex = index + advance;
        if (advance > 0 && nextIndex < consultCodeInputs.length) {
            focusConsultCodeInput(nextIndex);
        }
        syncConsultCodeInputs();
    }

    function handleConsultCodeKeydown(event, index) {
        const key = event.key;
        if (key === 'Backspace') {
            if (!consultCodeInputs[index].value && index > 0) {
                event.preventDefault();
                const previous = consultCodeInputs[index - 1];
                previous.value = '';
                focusConsultCodeInput(index - 1);
                syncConsultCodeInputs();
            }
        } else if (key === 'ArrowLeft' && index > 0) {
            event.preventDefault();
            focusConsultCodeInput(index - 1);
        } else if (key === 'ArrowRight' && index < consultCodeInputs.length - 1) {
            event.preventDefault();
            focusConsultCodeInput(index + 1);
        } else if (key === 'Enter' && consultTokenBtn && !consultTokenBtn.disabled) {
            event.preventDefault();
            consultTokenBtn.click();
        }
    }

    function handleConsultCodePaste(event, index) {
        const clipboardData = event.clipboardData || window.clipboardData;
        if (!clipboardData) return;
        const pasted = clipboardData.getData('text');
        if (!pasted) return;
        event.preventDefault();
        const inserted = distributeConsultCodeChars(index, pasted);
        const targetIndex = index + inserted;
        if (targetIndex < consultCodeInputs.length) {
            focusConsultCodeInput(targetIndex);
        }
        syncConsultCodeInputs();
    }
    
    function handleHubLogin() {
        const username = hubUserInput.value;
        const password = hubPassInput.value;
        
        if (!username || !password) { showHubLoginError("Preencha usuário e senha."); return; }
        const formData = new URLSearchParams();
        formData.append('username', username);
        formData.append('password', password);

        fetch('/api/hub/login', { method: 'POST', body: formData })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                // Redireciona para a mainpage após login
                window.location.href = '/';
            } else { showHubLoginError(data.mensagem || "Erro desconhecido."); }
        })
        .catch(() => showHubLoginError("Erro de comunicação com o servidor."));
    }

    function handleHubLogout() {
        // --- INÍCIO DA MODIFICAÇÃO (Limpar Ordem) ---
        if (currentHubUser) {
            sessionStorage.removeItem(`sortedAutomations_${currentHubUser}`);
            sessionStorage.removeItem(`sortedDashboards_${currentHubUser}`);
        }
        // --- FIM DA MODIFICAÇÃO ---
        fetch('/api/hub/logout', { method: 'POST' })
        .then(() => {
            window.location.href = '/';
        });
    }

    // --- Modal de Conexões (Mantidas) ---
    function openConnectionsModal() {
        accessDropdown.classList.remove('visible');
        connectionsListContainer.innerHTML = '<p class="no-connections">Carregando...</p>';
        connectionsOverlay.classList.add('visible');

        fetch('/api/hub/get-connections')
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                renderConnections(data.connections);
            } else {
                connectionsListContainer.innerHTML = '<p class="no-connections">Erro ao carregar conexões.</p>';
            }
        });
    }

    function renderConnections(connections) {
        connectionsListContainer.innerHTML = '';
        const sapConn = connections.sap;
        const bwConn = connections.bw;
        const tableauConn = connections.tableau;

        connectionsListContainer.appendChild(
            createConnectionItem('sap', '/static/icones/saplong_logo.png', 'SAP', sapConn)
        );
        connectionsListContainer.appendChild(
            createConnectionItem('bw', '/static/icones/bwhanashort_logo.png', 'BW HANA', bwConn)
        );
        connectionsListContainer.appendChild(
            createConnectionItem('salesforce', '/static/icones/salesforce_logo.png', 'Salesforce', connections.salesforce)
        );
    }

    function createConnectionItem(system, iconSrc, systemName, connectionData) {
        const item = document.createElement('li');
        item.className = 'connection-item';
        item.dataset.system = system;

        const iconClass = (system === 'salesforce') ? 'connection-icon salesforce-icon' : 'connection-icon';
        let iconHtml = '';
        if (system === 'sap') {
            iconHtml = `
                <img src="/static/icones/saplong_logo.png" alt="SAP Logo" class="${iconClass} logo-light">
                <img src="/static/icones/sapblacklong_logo.png" alt="SAP Logo" class="${iconClass} logo-dark">
            `;
        } else if (system === 'tableau') {
            iconHtml = `
                <img src="/static/icones/tableau_logo.png" alt="Tableau Logo" class="${iconClass} logo-light">
                <img src="/static/icones/tableaublack_logo.png" alt="Tableau Logo" class="${iconClass} logo-dark">
            `;
        } else if (system === 'salesforce') {
            iconHtml = `<img src="/static/icones/salesforce_logo.png" alt="Salesforce Logo" class="${iconClass}">`;
        } else {
            iconHtml = `<img src="${iconSrc}" alt="${systemName} Logo" class="${iconClass}">`;
        }

        const details = document.createElement('div');
        details.className = 'connection-details';
        details.insertAdjacentHTML('beforeend', iconHtml);

        const userText = document.createElement('span');
        userText.className = 'connection-user-text';
        const labelStrong = document.createElement('strong');
        labelStrong.textContent = 'Usuário:';
        userText.appendChild(labelStrong);
        const userValue = connectionData && connectionData.user ? connectionData.user : 'Não conectado';
        userText.appendChild(document.createTextNode(` ${userValue}`));
        details.appendChild(userText);

        item.appendChild(details);

        if (connectionData) {
            const removeBtn = document.createElement('button');
            removeBtn.className = 'connection-remove-btn';
            removeBtn.dataset.system = system;
            removeBtn.title = 'Remover conexão';
            removeBtn.innerHTML = '<i class="fas fa-trash-alt"></i>';
            removeBtn.addEventListener('click', handleRemoveConnection);
            item.appendChild(removeBtn);
        }
        return item;
    }

    function closeConnectionsModal() {
        connectionsOverlay.classList.remove('visible');
    }

    function handleRemoveConnection(e) {
        const btn = e.currentTarget;
        const system = btn.dataset.system;
        
        fetch(`/api/hub/remove-connection/${system}`, { method: 'POST' })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                openConnectionsModal();
                if (window.location.pathname.includes('/automacao')) {
                    window.location.reload();
                }
            } else {
                alert(data.mensagem || "Erro ao remover conexão.");
            }
        });
    }

    // --- Listeners de Eventos Globais ---
    accessBtn.addEventListener('click', () => {
        accessDropdown.classList.toggle('visible');
    });
    // Atualizado para não fechar o dropdown se clicar nos modais de perfil/cropper
    document.addEventListener('click', (e) => {
        const inAccess = accessBtn.contains(e.target) || accessDropdown.contains(e.target);
        const inProfileModal = profileOverlay.contains(e.target) || (cropperOverlay && cropperOverlay.contains(e.target));
        
        if (!inAccess && !inProfileModal) {
            accessDropdown.classList.remove('visible');
        }

        // --- CÓDIGO ADICIONADO: Lógica para fechar o dropdown da busca ---
        const inSearch = searchBar.contains(e.target) || searchDropdown.contains(e.target);
        if (!inSearch) {
            searchDropdown.classList.remove('visible');
        }

    });
    if (hubLoginCloseBtn) {
        hubLoginCloseBtn.addEventListener('click', closeHubLoginModal);
    }
    if (hubLoginSubmitBtn) {
        hubLoginSubmitBtn.addEventListener('click', handleHubLogin);
    }
    if (editNameOverlay) {
        editNameCloseBtn.addEventListener('click', closeEditNameModal);
        editNameOverlay.addEventListener('mousedown', (e) => {
            if (e.target === editNameOverlay) closeEditNameModal();
        });
        editNameSaveBtn.addEventListener('click', handleSaveName);
        editNameInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') handleSaveName();
        });
    }
    hubLoginOverlay.addEventListener('mousedown', (e) => { if (e.target === hubLoginOverlay) closeHubLoginModal(); });
    hubPassInput.addEventListener('keypress', (e) => { if (e.key === 'Enter') handleHubLogin(); });
    [hubUserInput, hubPassInput].forEach(input => {
        input.addEventListener('input', updateHubLoginButtonState);
    });

    if (connectionsFooterCloseBtn) {
        connectionsFooterCloseBtn.addEventListener('click', closeConnectionsModal);
    }
    if (connectionsOverlay) {
        connectionsOverlay.addEventListener('mousedown', (e) => { if (e.target === connectionsOverlay) closeConnectionsModal(); });
    }

    // --- Inicialização da Sessão ---
fetch('/api/hub/check-session')
.then(response => response.json())
.then(data => {
    currentProfileUrl = data.profile_image || defaultProfileUrl;
    
    if(data.status === 'logado') {
        currentHubUser = data.username; 
        currentHubOriginArea = data.area; // Área de origem (estática)
        currentHubRole = data.role;
        currentHubAllowedAreas = data.allowed_areas || [data.area];
        // M1: hubUsername em sessionStorage (não persistir entre abas/sessões)
        sessionStorage.setItem('hubUsername', data.username); 
        
        // --- INÍCIO DA MODIFICAÇÃO (Req 1) ---
        // Armazena o nome globalmente E passa para o dropdown
        currentHubDisplayName = data.display_name;
        updateAccessDropdown(data.username, data.profile_image, data.area, data.role, currentHubDisplayName);
        // --- FIM DA MODIFICAÇÃO ---

        // --- Obtém área ativa (pode diferir da área padrão se o usuário trocou) ---
        const activeArea = getActiveHubArea(data.username, data.area);
        currentHubArea = activeArea;
        renderAreaSelector(activeArea, currentHubAllowedAreas);

        try {
            const savedAutomations = sessionStorage.getItem(`sortedAutomations_${data.username}`);
            if (savedAutomations) {
                globalCmsAutomations = JSON.parse(savedAutomations);
            }
            const savedDashboards = sessionStorage.getItem(`sortedDashboards_${data.username}`);
            if (savedDashboards) {
                globalCmsDashboards = JSON.parse(savedDashboards);
            }
        } catch (e) {
            console.error("Falha ao carregar CMS do sessionStorage", e);
            sessionStorage.removeItem(`sortedAutomations_${data.username}`);
            sessionStorage.removeItem(`sortedDashboards_${data.username}`);
        }
    } else {
        currentHubUser = null;
        currentHubArea = null;
        currentHubOriginArea = null;
        currentHubRole = null;
        currentHubAllowedAreas = [];
        currentHubDisplayName = null; // (Limpa o nome)
        // M1: hubUsername em sessionStorage
        sessionStorage.removeItem('hubUsername');
        updateAccessDropdown(null, data.profile_image, null, null, null);
    }

    renderQuickLinks(); 
    loadSearchData();
});

// --- NOVA LÓGICA: MODAL DE REGISTRO ---

    function resetRegisterModalState() {
        if (registerFieldsWrapper) {
            registerFieldsWrapper.classList.remove('hidden');
        }
        if (registerTokenDisplay) {
            registerTokenDisplay.classList.add('hidden');
        }
        resetRegisterSuccessView();
        resetRegisterFormFields();
        consultFetchInProgress = false;
        resetConsultCodeInputs();

        if (registerStatus) {
            registerStatus.textContent = '';
            registerStatus.classList.add('hidden');
        }
        if (registerSubmitBtn) {
            registerSubmitBtn.disabled = false;
        }
        if (consultStatusWrapper) {
            consultStatusWrapper.classList.add('hidden');
        }
        if (consultStatusError) {
            consultStatusError.classList.add('hidden');
            consultStatusError.textContent = '';
        }
        hideConsultAccessInfo();
        hideConsultRejectedInfo();
        if (consultToken) {
            consultToken.value = '';
        }
        updateConsultButtonState('');
    }

    function closeRegisterModal() {
        showRegisterTab('register');
        if (registerOverlay) {
            registerOverlay.classList.remove('visible');
        }
        resetRegisterModalState();
    }

    function openRegisterModal() {
        accessDropdown.classList.remove('visible');
        if (registerOverlay) {
            registerOverlay.classList.add('visible');
        }
        showRegisterTab('register');
        resetRegisterModalState();
    }

    function showRegisterTab(tabName) {
        if (tabName === 'register') {
            tabRegister.classList.add('active');
            tabConsult.classList.remove('active');
            registerTabContent.classList.remove('hidden');
            consultTabContent.classList.add('hidden');
        } else {
            tabRegister.classList.remove('active');
            tabConsult.classList.add('active');
            registerTabContent.classList.add('hidden');
            consultTabContent.classList.remove('hidden');
        }
        updateRegisterSubmitButtonState();
        updateConsultButtonState();
    }
    
    // Listeners das Abas de Registro
    tabRegister.addEventListener('click', () => showRegisterTab('register'));
    tabConsult.addEventListener('click', () => {
        showRegisterTab('consult');
        if (consultCodeInputs.length) {
            focusConsultCodeInput(0);
        }
    });
    registerCloseBtn.addEventListener('click', closeRegisterModal);
    registerOverlay.addEventListener('mousedown', (e) => {
        if (e.target === registerOverlay) closeRegisterModal();
    });

    if (registerCopyCodeBtn) {
        registerCopyCodeBtn.addEventListener('click', handleRegisterCodeCopy);
    }

    function updateRegisterSubmitButtonState() {
        if (!registerSubmitBtn) return;
        const isRegisterTabActive = tabRegister.classList.contains('active');
        const formVisible = !registerFieldsWrapper || !registerFieldsWrapper.classList.contains('hidden');
        const hasInput = registerUser.value.trim().length > 0 || registerArea.value;
        if (isRegisterTabActive && formVisible && hasInput) {
            registerSubmitBtn.classList.remove('hidden');
        } else {
            registerSubmitBtn.classList.add('hidden');
        }
    }

    function resetSelectToPlaceholder(selectElement) {
        if (!selectElement) return;
        const placeholder = selectElement.querySelector('option[data-placeholder="true"]');
        if (placeholder) {
            placeholder.selected = true;
            selectElement.value = placeholder.value;
        } else if (selectElement.options.length) {
            selectElement.selectedIndex = 0;
        } else {
            selectElement.value = '';
        }
    }

    function resetRegisterFormFields() {
        if (registerUser) {
            registerUser.value = '';
        }
        resetSelectToPlaceholder(registerArea);
        resetSelectToPlaceholder(registerRole);
        updateRegisterSubmitButtonState();
    }

    if (registerUser) {
        registerUser.addEventListener('input', updateRegisterSubmitButtonState);
    }
    if (registerArea) {
        registerArea.addEventListener('change', updateRegisterSubmitButtonState);
    }
    if (registerRole) {
        registerRole.addEventListener('change', updateRegisterSubmitButtonState);
    }

    updateRegisterSubmitButtonState();
    updateConsultButtonState();

    if (consultCodeInputs.length) {
        consultCodeInputs.forEach((input, index) => {
            input.addEventListener('input', (event) => handleConsultCodeInputEvent(event, index));
            input.addEventListener('keydown', (event) => handleConsultCodeKeydown(event, index));
            input.addEventListener('paste', (event) => handleConsultCodePaste(event, index));
        });
        syncConsultCodeInputs();
    }

    // Enviar Solicitação de Registro
    registerSubmitBtn.addEventListener('click', () => {
        const username = registerUser.value.trim();
        const area = registerArea.value;
        const role = registerRole.value;

        if (!username || !area) {
            registerStatus.textContent = "Preencha o Login de Funcionário e a Área.";
            registerStatus.classList.remove('hidden');
            return;
        }

        registerStatus.classList.add('hidden');
        registerSubmitBtn.disabled = true;

        fetch('/api/hub/register', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username: username, area: area, role: role })
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                showRegisterSuccessCard(data.token);
            } else {
                registerStatus.textContent = data.mensagem;
                registerStatus.classList.remove('hidden');
            }
        })
        .finally(() => {
            registerSubmitBtn.disabled = false;
        });
    });

    // Consultar Token
    if (consultTokenBtn) {
        consultTokenBtn.addEventListener('click', () => {
            const rawToken = consultToken ? consultToken.value : getConsultCodeValue();
            const token = rawToken.trim().toUpperCase();
            if (consultToken) {
                consultToken.value = token;
            }
            if (token.length !== ACCESS_CODE_LENGTH) return;

            consultFetchInProgress = true;
            updateConsultButtonState();
            consultStatusWrapper.classList.add('hidden');
            consultStatusError.classList.add('hidden');
            hideConsultAccessInfo();
            hideConsultRejectedInfo();
            resetConsultProgress();
            
            fetch('/api/hub/consult', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ token: token })
            })
            .then(response => response.json())
            .then(data => {
                if (data.status === 'sucesso') {
                    const status = data.request_data.status;
                    consultStatusWrapper.classList.remove('hidden');
                    hideConsultAccessInfo();
                    hideConsultRejectedInfo();

                    if (status === 'Aprovado') {
                        hideConsultProgress();
                        const usernameValue = data.request_data.username || '—';
                        const passwordValue = data.request_data.generated_password || '—';
                        showConsultAccessInfo(usernameValue, passwordValue);
                    } else if (status === 'Aguardando Aprovação') {
                        showConsultAwaitingProgress();
                    } else {
                        hideConsultProgress();
                        const justificationText = data.request_data.justification || 'N/A';
                        showConsultRejectedInfo(justificationText);
                    }
                } else {
                    consultStatusError.textContent = data.mensagem;
                    consultStatusError.classList.remove('hidden');
                }
            })
            .finally(() => {
                consultFetchInProgress = false;
                syncConsultCodeInputs();
            });
        });
    }

    // --- NOVA LÓGICA: MODAL DE ADMINISTRAÇÃO ---

    function openAdminModal() {
        accessDropdown.classList.remove('visible');

        // --- INÍCIO DA MODIFICAÇÃO (Req 1: Adicionar Botão de Usuário) ---
        if (!document.getElementById('admin-add-user-btn')) {
            const userTabPanel = document.getElementById('admin-users-tab');
            if (userTabPanel) {
                const buttonHtml = `
                    <button class="admin-add-btn" id="admin-add-user-btn">
                        <i class="fas fa-user-plus"></i>
                        <span>Novo</span>
                    </button>
                `;
                // Insere o botão (ele será movido pelo showAdminTab)
                userTabPanel.insertAdjacentHTML('afterbegin', buttonHtml);
                
                // --- (LÓGICA DO LISTENER ATUALIZADA) ---
                document.getElementById('admin-add-user-btn').addEventListener('click', () => {
                    // 1. Fecha outros forms
                    closeAllEditForms(adminUserListContainer);

                    // 2. Cria um objeto de usuário temporário
                    const newUser = {
                        username: 'temp_user_' + Date.now(), // ID Temporário
                        password: '',
                        area: 'Spare Parts', // Padrão
                        role: 'Analista',  // Padrão
                        isNew: true
                    };
                    
                    // 3. Adiciona o novo usuário ao TOPO da lista global
                    globalCmsUsers.unshift(newUser);
                    
                    // 4. Re-renderiza a lista inteira (que agora inclui o novo)
                    renderAdminUsers(); 

                    // 5. Encontra o card que acabou de ser renderizado
                    const newCard = adminUserListContainer.querySelector(`[data-temp-id="${newUser.username}"]`);
                    
                    if (newCard) {
                        // 6. Abre o formulário de edição (ele já está visível por padrão)
                        
                        // 7. Rola o container (a lista) para o topo
                        adminUserListContainer.scrollTop = 0;
                    }
                });
            }
        }
        // --- FIM DA MODIFICAÇÃO ---

        if (adminSearchInputField) {
            adminSearchInputField.value = '';
        }

        adminListContainer.innerHTML = '<p class="no-requests">Carregando solicitações...</p>';
        adminUserListContainer.innerHTML = '<p class="no-requests">Carregando usuários...</p>';
        adminDashboardsList.innerHTML = '<p class="no-requests">Carregando dashboards...</p>';
        adminAutomationsList.innerHTML = '<p class="no-requests">Carregando automações...</p>';
        
        showAdminTab('requests'); 
        adminOverlay.classList.add('visible');
        
        // 1. Busca Solicitações Pendentes
        fetch('/api/admin/get-requests')
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                renderAdminRequests(data.requests);
            } else {
                adminListContainer.innerHTML = `<p class="no-requests">Erro: ${data.mensagem}</p>`;
                pendingRequestsCount = 0;
                updateRequestsBadge(0);
            }
        });
        
        // 2. Busca Lista de Usuários (e salva globalmente)
        // (SÓ BUSCA SE A LISTA ESTIVER VAZIA)
        if (!globalCmsUsers || globalCmsUsers.length === 0) {
            fetch('/api/admin/get-users')
            .then(response => response.json())
            .then(data => {
                if (data.status === 'sucesso') {
                    globalCmsUsers = data.users; // Salva na variável global
                    renderAdminUsers(); // Renderiza da global
                } else {
                    adminUserListContainer.innerHTML = `<p class="no-requests">Erro: ${data.mensagem}</p>`;
                }
            });
        } else {
            renderAdminUsers(); // Renderiza a lista local
        }
        
        // 3. Busca Dados do CMS (SEMPRE re-busca do endpoint admin para dados completos)
        fetch('/api/admin/get-cms-data')
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                globalCmsDashboards = data.dashboards;
                globalCmsAutomations = data.automations;
                applySavedAutomationOrder();
                renderAdminDashboards();
                renderAdminAutomations();
            } else {
                adminDashboardsList.innerHTML = `<p class="no-requests">Erro: ${data.mensagem}</p>`;
                adminAutomationsList.innerHTML = `<p class="no-requests">Erro: ${data.mensagem}</p>`;
            }
        });
    }

    // SUBSTITUA a função renderAdminRequests por esta:
function updateRequestsBadge(count) {
    if (!adminRequestsBadge) return;
    if (count > 0) {
        adminRequestsBadge.textContent = count;
        adminRequestsBadge.classList.remove('hidden');
    } else {
        adminRequestsBadge.classList.add('hidden');
    }
}

function renderAdminRequests(requests) {
    adminListContainer.innerHTML = '';
    const tokens = Object.keys(requests);
    pendingRequestsCount = tokens.length;
    updateRequestsBadge(pendingRequestsCount);
    
    if (tokens.length === 0) {
        adminListContainer.innerHTML = '<p class="no-requests">Nenhuma solicitação pendente.</p>';
        return;
    }
    
    tokens.forEach(token => {
        const req = requests[token];
        
        // --- CORREÇÃO (Req 2): Formata a data ISO (agora com fuso) para PT-BR local
        const requestDate = new Date(req.request_date).toLocaleString('pt-BR', {
            day: '2-digit', 
            month: '2-digit', 
            year: 'numeric', 
            hour: '2-digit', 
            minute: '2-digit'
        });
        
        const item = document.createElement('div');
        item.className = 'admin-request-card'; // <-- CORRIGIDO (estava item.className)
        item.dataset.token = token;
        
        item.innerHTML = `
            <div class="admin-request-main">
                <div class="admin-request-info">
                    <div class="username">${req.username.toUpperCase()}</div> 
                    <div class="details">
                        <strong>Área:</strong> ${req.area} | <strong>Função:</strong> ${req.role}
                        <br>
                        <strong>Solicitado em:</strong> ${requestDate}
                    </div>
                </div>
                <div class="admin-request-actions">
                    <button class="button btn-execute admin-approve-btn">Aprovar</button>
                    <button class="button btn-danger admin-reject-btn">Reprovar</button>
                </div>
            </div>
            <div class="admin-justification-form hidden">
                <textarea class="admin-justification-input" placeholder="Justificativa da reprovação..." maxlength="42"></textarea>
                <div class="admin-justification-actions">
                    <button class="button btn-cancel admin-reject-cancel-btn">Cancelar</button>
                    <button class="button btn-danger admin-reject-save-btn">Confirmar</button>
                </div>
            </div>
        `;
        adminListContainer.appendChild(item);
    });

    // Adiciona listeners aos botões
    adminListContainer.querySelectorAll('.admin-approve-btn').forEach(btn => {
        btn.addEventListener('click', handleAdminApprove);
    });
    adminListContainer.querySelectorAll('.admin-reject-btn').forEach(btn => {
        btn.addEventListener('click', showAdminRejectionForm);
    });
    adminListContainer.querySelectorAll('.admin-reject-save-btn').forEach(btn => {
        btn.addEventListener('click', handleAdminReject);
    });
    adminListContainer.querySelectorAll('.admin-reject-cancel-btn').forEach(btn => {
        btn.addEventListener('click', hideAdminRejectionForm);
    });
}

    function handleAdminApprove(e) {
    const item = e.target.closest('.admin-request-card'); // <-- CORREÇÃO (Req 1)
    const token = item.dataset.token;
    e.target.disabled = true;
    
    fetch('/api/admin/approve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: token })
    })
    .then(response => response.json())
    .then(data => {
        if (data.status === 'sucesso') {
            // --- CORREÇÃO (Req 2): Remove o item em vez de esmaecer ---
            item.remove();
            
            // Verifica se a lista ficou vazia
          if (adminListContainer.children.length === 0) {
              adminListContainer.innerHTML = '<p class="no-requests">Nenhuma solicitação pendente.</p>';
          }
          pendingRequestsCount = Math.max(0, pendingRequestsCount - 1);
          updateRequestsBadge(pendingRequestsCount);
        } else {
            alert(data.mensagem);
            e.target.disabled = false;
        }
    });
}
    
    // SUBSTITUA a função showAdminRejectionForm por esta:
function showAdminRejectionForm(e) {
    const item = e.target.closest('.admin-request-card'); // <-- CORREÇÃO
    item.querySelector('.admin-justification-form').classList.remove('hidden');
    item.querySelector('.admin-request-main').classList.add('hidden'); // Esconde a linha principal
}

// SUBSTITUA a função hideAdminRejectionForm por esta:
function hideAdminRejectionForm(e) {
    const item = e.target.closest('.admin-request-card'); // <-- CORREÇÃO
    item.querySelector('.admin-justification-form').classList.add('hidden');
    item.querySelector('.admin-justification-input').value = ''; // Limpa o texto
    item.querySelector('.admin-request-main').classList.remove('hidden'); // Mostra a linha principal
}

// SUBSTITUA a função handleAdminReject por esta:
function handleAdminReject(e) {
    const item = e.target.closest('.admin-request-card'); // <-- CORREÇÃO (Req 1)
    const token = item.dataset.token;
    const justification = item.querySelector('.admin-justification-input').value;
    
    if (!justification) {
        alert("Por favor, insira uma justificativa.");
        return;
    }
    
    e.target.disabled = true;

    fetch('/api/admin/reject', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: token, justification: justification })
    })
    .then(response => response.json())
    .then(data => {
        if (data.status === 'sucesso') {
            // --- CORREÇÃO (Req 2): Remove o item em vez de esmaecer ---
            item.remove();
            
            // Verifica se a lista ficou vazia
          if (adminListContainer.children.length === 0) {
              adminListContainer.innerHTML = '<p class="no-requests">Nenhuma solicitação pendente.</p>';
          }
          pendingRequestsCount = Math.max(0, pendingRequestsCount - 1);
          updateRequestsBadge(pendingRequestsCount);
        } else {
            alert(data.mensagem);
            e.target.disabled = false;
        }
    });
}

    // Listener de fechamento do Modal de Admin
    adminCloseBtn.addEventListener('click', () => {
        // --- INÍCIO DA MODIFICAÇÃO (Limpar "Novos" não salvos) ---
        // Limpa os forms abertos (e remove itens "isNew")
        closeAllEditForms(adminUserListContainer);
        closeAllEditForms(adminDashboardsList);
        closeAllEditForms(adminAutomationsList);
        // --- FIM DA MODIFICAÇÃO ---

        adminOverlay.classList.remove('visible');
    });
    adminOverlay.addEventListener('mousedown', (e) => {
        if (e.target === adminOverlay) {
            // --- INÍCIO DA MODIFICAÇÃO (Limpar "Novos" não salvos) ---
            // Limpa os forms abertos (e remove itens "isNew")
            closeAllEditForms(adminUserListContainer);
            closeAllEditForms(adminDashboardsList);
            closeAllEditForms(adminAutomationsList);
            // --- FIM DA MODIFICAÇÃO ---
            
            adminOverlay.classList.remove('visible');
        }
    });  

// --- NOVA LÓGICA: ADMIN - GERENCIAR USUÁRIOS ---
    
    // Função para trocar as abas do modal de Admin
    const adminTabCopy = {
        requests: {
            title: 'Solicitações',
            description: 'Gerencie pedidos pendentes e responda com rapidez.'
        },
        users: {
            title: 'Usuários',
            description: 'Atualize perfis, redefina senhas e controle permissões.'
        },
        dashboards: {
            title: 'Dashboards',
            description: 'Organize o catálogo e mantenha os links atualizados.'
        },
        automations: {
            title: 'Automações',
            description: 'Organize o catálogo e seus caminhos de execução.'
        }
    };

    function showAdminTab(tabName) {
        // Esconde todos os painéis
        adminRequestsTab.classList.add('hidden');
        adminUsersTab.classList.add('hidden');
        adminDashboardsTab.classList.add('hidden');
        adminAutomationsTab.classList.add('hidden');
        
        // Remove 'active' de todas as abas
        tabAdminRequests.classList.remove('active');
        tabAdminUsers.classList.remove('active');
        tabAdminDashboards.classList.remove('active');
        tabAdminAutomations.classList.remove('active');

    const searchContainer = adminSearchContainerElement;
    const searchInput = adminSearchInputField;
        
        // (Req 1) Referencia o novo botão de usuário
        const adminAddUserBtn = document.getElementById('admin-add-user-btn');

        // 1. Reseta os botões (move de volta para suas abas e esconde)
        if (adminAddDashboardBtn) {
            adminDashboardsTab.prepend(adminAddDashboardBtn);
            adminAddDashboardBtn.style.display = 'none';
        }
        if (adminAddAutomationBtn) {
            adminAutomationsTab.prepend(adminAddAutomationBtn);
            adminAddAutomationBtn.style.display = 'none';
        }
        // (Req 1) Reseta o botão de usuário
        if (adminAddUserBtn) {
            adminUsersTab.prepend(adminAddUserBtn);
            adminAddUserBtn.style.display = 'none';
            searchContainer.classList.remove('users-active'); // Remove a classe CSS
        }

        // Remove filtros dinâmicos (serão re-adicionados se necessário)
        document.querySelectorAll('.admin-filter-wrapper').forEach(el => el.remove());
        if (searchContainer) searchContainer.classList.remove('has-area-filter');

        const searchableTabs = ['users', 'dashboards', 'automations'];
        
        if (searchContainer) { 
            if (searchableTabs.includes(tabName)) {
                searchContainer.classList.remove('hidden');
            } else {
                searchContainer.classList.add('hidden');
            }
        }
        
        if (searchInput) {
            searchInput.value = '';
        }
        
        // Mostra a aba e painel corretos E move o botão correto
        if (tabName === 'requests') {
            tabAdminRequests.classList.add('active');
            adminRequestsTab.classList.remove('hidden');
        
        } else if (tabName === 'users') {
            tabAdminUsers.classList.add('active');
            adminUsersTab.classList.remove('hidden');

            // Adiciona filtros de área e função e botão para usuários
            if (searchContainer) {
                addAdminAreaFilter(searchContainer);
                addAdminRoleFilter(searchContainer);
                searchContainer.classList.add('has-area-filter');
                searchContainer.classList.add('users-active');
            }
            if (adminAddUserBtn && searchContainer) {
                searchContainer.appendChild(adminAddUserBtn);
                adminAddUserBtn.style.display = 'block';
            }

        } else if (tabName === 'dashboards') {
            tabAdminDashboards.classList.add('active');
            adminDashboardsTab.classList.remove('hidden');
            
            // Adiciona filtros de área e plataforma, depois o botão Novo
            if (searchContainer) {
                addAdminAreaFilter(searchContainer);
                addAdminPlatformFilter(searchContainer);
                searchContainer.classList.add('has-area-filter');
                if (adminAddDashboardBtn) {
                    searchContainer.appendChild(adminAddDashboardBtn);
                    adminAddDashboardBtn.style.display = 'block';
                }
            }

        } else if (tabName === 'automations') {
            tabAdminAutomations.classList.add('active');
            adminAutomationsTab.classList.remove('hidden');
            
            // Adiciona filtros de área e plataforma, depois o botão Novo
            if (searchContainer) {
                addAdminAreaFilter(searchContainer);
                addAdminAutomationPlatformFilter(searchContainer);
                searchContainer.classList.add('has-area-filter');
                if (adminAddAutomationBtn) {
                    searchContainer.appendChild(adminAddAutomationBtn);
                    adminAddAutomationBtn.style.display = 'block';
                }
            }
        }
        
        handleAdminSearch();

        if (adminActiveTitle && adminActiveDescription) {
            const copy = adminTabCopy[tabName];
            if (copy) {
                adminActiveTitle.textContent = copy.title;
                adminActiveDescription.textContent = copy.description;
            }
        }
    }

    function createAdminCustomSelect(id, options) {
        const wrap = document.createElement('div');
        wrap.className = 'admin-custom-select';

        // Trigger (botão visível)
        const trigger = document.createElement('button');
        trigger.type = 'button';
        trigger.className = 'admin-select-trigger';

        const valueSpan = document.createElement('span');
        valueSpan.className = 'admin-select-value';
        valueSpan.textContent = options[0].label;

        const arrow = document.createElement('i');
        arrow.className = 'fas fa-chevron-down admin-select-arrow';

        trigger.appendChild(valueSpan);
        trigger.appendChild(arrow);

        // Painel de opções
        const panel = document.createElement('div');
        panel.className = 'admin-select-panel';

        // Select oculto para compatibilidade com handleAdminSearch
        const hidden = document.createElement('select');
        hidden.id = id;
        hidden.className = 'admin-area-filter-hidden';
        hidden.setAttribute('aria-hidden', 'true');
        hidden.tabIndex = -1;

        options.forEach((opt, idx) => {
            // Opção visual
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'admin-select-option' + (idx === 0 ? ' is-selected' : '');
            btn.dataset.value = opt.value;

            if (idx === 0) {
                const check = document.createElement('i');
                check.className = 'fas fa-check admin-select-check';
                btn.appendChild(check);
            } else {
                const check = document.createElement('i');
                check.className = 'fas fa-check admin-select-check';
                btn.appendChild(check);
            }
            btn.appendChild(document.createTextNode(opt.label));

            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                panel.querySelectorAll('.admin-select-option').forEach(b => b.classList.remove('is-selected'));
                btn.classList.add('is-selected');
                valueSpan.textContent = opt.label;
                hidden.value = opt.value;
                wrap.classList.remove('open');
                handleAdminSearch();
            });

            panel.appendChild(btn);

            // Opção oculta
            const o = document.createElement('option');
            o.value = opt.value;
            o.textContent = opt.label;
            hidden.appendChild(o);
        });

        trigger.addEventListener('click', (e) => {
            e.stopPropagation();
            const isOpen = wrap.classList.contains('open');
            document.querySelectorAll('.admin-custom-select.open').forEach(el => el.classList.remove('open'));
            if (!isOpen) wrap.classList.add('open');
        });

        wrap.appendChild(trigger);
        wrap.appendChild(panel);
        wrap.appendChild(hidden);
        return wrap;
    }

    // Fecha dropdowns ao clicar fora
    document.addEventListener('click', () => {
        document.querySelectorAll('.admin-custom-select.open').forEach(el => el.classList.remove('open'));
    });

    function addAdminAreaFilter(container) {
        const wrapper = document.createElement('div');
        wrapper.className = 'admin-filter-wrapper';
        const label = document.createElement('span');
        label.className = 'admin-filter-label';
        label.textContent = 'Área';
        wrapper.appendChild(label);
        wrapper.appendChild(createAdminCustomSelect('admin-area-filter', [
            { value: '', label: 'Todas' },
            { value: 'Spare Parts', label: 'Spare Parts' },
            { value: 'Finished Goods', label: 'Finished Goods' }
        ]));
        container.appendChild(wrapper);
    }

    function addAdminRoleFilter(container) {
        const wrapper = document.createElement('div');
        wrapper.className = 'admin-filter-wrapper';
        const label = document.createElement('span');
        label.className = 'admin-filter-label';
        label.textContent = 'Função';
        wrapper.appendChild(label);
        wrapper.appendChild(createAdminCustomSelect('admin-role-filter', [
            { value: '', label: 'Todas' },
            { value: 'Analista', label: 'Analista' },
            { value: 'Executor', label: 'Executor' }
        ]));
        container.appendChild(wrapper);
    }

    function addAdminPlatformFilter(container) {
        const wrapper = document.createElement('div');
        wrapper.className = 'admin-filter-wrapper';
        const label = document.createElement('span');
        label.className = 'admin-filter-label';
        label.textContent = 'Plataforma';
        wrapper.appendChild(label);
        wrapper.appendChild(createAdminCustomSelect('admin-platform-filter', [
            { value: '', label: 'Todas' },
            { value: 'looker', label: 'Looker' },
            { value: 'tableau', label: 'Tableau' },
            { value: 'library', label: 'Library' }
        ]));
        container.appendChild(wrapper);
    }

    function addAdminAutomationPlatformFilter(container) {
        const wrapper = document.createElement('div');
        wrapper.className = 'admin-filter-wrapper';
        const label = document.createElement('span');
        label.className = 'admin-filter-label';
        label.textContent = 'Plataforma';
        wrapper.appendChild(label);
        wrapper.appendChild(createAdminCustomSelect('admin-platform-filter', [
            { value: '', label: 'Todas' },
            { value: 'sap', label: 'SAP' },
            { value: 'bw', label: 'BW' },
            { value: 'salesforce', label: 'Salesforce' }
        ]));
        container.appendChild(wrapper);
    }
    
    // Listeners das Abas de Admin
    tabAdminRequests.addEventListener('click', () => showAdminTab('requests'));
    tabAdminUsers.addEventListener('click', () => showAdminTab('users'));


    // Renderiza a lista de usuários gerenciáveis
    function renderAdminUsers() {
        adminUserListContainer.innerHTML = ''; // Limpa a lista
        
        // 1. Verifica a variável global (que será preenchida pelo openAdminModal)
        if (!globalCmsUsers || globalCmsUsers.length === 0) {
            adminUserListContainer.innerHTML = '<p class="no-requests">Nenhum usuário (além do admin) encontrado.</p>';
            return;
        }

        // 2. Chama o "helper" (renderAdminUserCard) para cada usuário na lista
        globalCmsUsers.forEach(user => {
            // false = não colocar no início (append)
            renderAdminUserCard(user, adminUserListContainer, false); 
        });
    }
    
    // (Req 1) NOVO: Helper para renderizar um card de usuário (novo ou existente)
    function renderAdminUserCard(user, container, prepend = false) {
        const item = document.createElement('div');
        item.className = 'admin-user-card';
        
        const isNew = user.isNew || false;
        if (isNew) {
            item.dataset.isNew = 'true';
            // Usamos um ID temporário para o caso de "Cancelar"
            item.dataset.tempId = user.username; 
        } else {
            item.dataset.username = user.username;
        }

        // Opções para os <select>
        const areaOptions = ['Spare Parts', 'Finished Goods']
            .map(a => `<option value="${a}" ${user.area === a ? 'selected' : ''}>${a}</option>`).join('');
        const roleOptions = ['Analista', 'Executor']
            .map(r => `<option value="${r}" ${user.role === r ? 'selected' : ''}>${r}</option>`).join('');

        // Checkboxes de áreas permitidas
        const userAllowedAreas = user.allowed_areas || [user.area];
        const allowedAreasCheckboxes = (isNew)
            ? ['Spare Parts', 'Finished Goods'].map(a => `
                <label class="hub-area-checkbox-label">
                    <input type="checkbox" class="allowed-area-checkbox" value="${a}" ${a === (user.area || 'Spare Parts') ? 'checked' : ''}>
                    ${a}
                </label>`).join('')
            : ['Spare Parts', 'Finished Goods'].map(a => `
                <label class="hub-area-checkbox-label">
                    <input type="checkbox" class="allowed-area-checkbox" value="${a}" ${userAllowedAreas.includes(a) ? 'checked' : ''}>
                    ${a}
                </label>`).join('');

        // (Req 1) Define o HTML baseado se é [NOVO] ou [EXISTENTE]
        let mainHtml, editHtml;

        if (isNew) {
            // Formulário de [NOVO] usuário
            mainHtml = `
                <div class="admin-user-main hidden">
                </div>
            `;
            editHtml = `
                <div class="admin-user-edit-form">
                    <div class="modal-input-group">
                        <label>Login de Funcionário:</label>
                        <input type="text" class="hub-modal-input edit-username-input" value="">
                    </div>
                    <div class="modal-input-group">
                        <label>Senha:</label>
                        <div class="password-toggle-wrapper">
                            <input type="password" class="hub-modal-input edit-password-input" value="">
                            <i class="fas fa-eye admin-password-toggle-btn" title="Mostrar/Ocultar Senha"></i>
                        </div>
                    </div>
                    <div class="scheduler-datetime-group" style="gap: 15px;">
                        <div class="modal-input-group">
                            <label>Área</label>
                            <select class="hub-modal-input edit-area-select">${areaOptions}</select>
                        </div>
                        <div class="modal-input-group">
                            <label>Função</label>
                            <select class="hub-modal-input edit-role-select">${roleOptions}</select>
                        </div>
                    </div>
                    <div class="modal-input-group">
                        <label>Áreas liberadas:</label>
                        <div class="allowed-areas-checkboxes">${allowedAreasCheckboxes}</div>
                    </div>
                    <div class="admin-user-edit-actions">
                        <button class="button btn-cancel admin-user-cancel-btn">Cancelar</button>
                        <button class="button btn-success admin-user-savenew-btn">Salvar</button>
                    </div>
                </div>
            `;
        } else {
            // Formulário de [EDITAR] usuário existente
            
            // --- INÍCIO DA MODIFICAÇÃO (Req 1: Status e Botão de Bloqueio) ---
            // Verifica se está bloqueado (se a data existe E é no futuro)
            const isLocked = user.lockout_until && (new Date(user.lockout_until) > new Date());
            const statusHtml = isLocked ? '<span class="user-status-locked">(Bloqueado)</span>' : '';
            const unlockBtnHtml = isLocked ? '<button class="button btn-unlock admin-user-unlock-btn">Desbloquear</button>' : '';
            // --- FIM DA MODIFICAÇÃO ---
            
            mainHtml = `
                <div class="admin-user-main">
                    <div class="admin-user-info">
                        <div class="username">${user.username.toUpperCase()} ${statusHtml}</div>
                        <div class="details">
                            <strong>Área:</strong> ${user.area} | <strong>Função:</strong> ${user.role}
                        </div>
                    </div>
                    <div class="admin-user-actions">
                        <button class="button btn-warning admin-user-edit-btn">Editar</button>
                        <button class="button btn-danger admin-user-delete-btn">Excluir</button>
                        ${unlockBtnHtml}
                    </div>
                </div>
            `;

            editHtml = `
                <div class="admin-user-edit-form hidden">
                    <div class="modal-input-group">
                        <label>Senha:</label>
                        <div class="password-toggle-wrapper">
                            <input type="password" class="hub-modal-input edit-password-input" value="${user.password || ''}">
                            <i class="fas fa-eye admin-password-toggle-btn" title="Mostrar/Ocultar Senha"></i>
                        </div>
                    </div>
                    <div class="scheduler-datetime-group" style="gap: 15px;">
                        <div class="modal-input-group">
                            <label>Área</label>
                            <select class="hub-modal-input edit-area-select">${areaOptions}</select>
                        </div>
                        <div class="modal-input-group">
                            <label>Função</label>
                            <select class="hub-modal-input edit-role-select">${roleOptions}</select>
                        </div>
                    </div>
                    <div class="modal-input-group">
                        <label>Áreas liberadas:</label>
                        <div class="allowed-areas-checkboxes">${allowedAreasCheckboxes}</div>
                    </div>
                    <div class="admin-user-edit-actions">
                        <button class="button btn-cancel admin-user-cancel-btn">Cancelar</button>
                        <button class="button btn-success admin-user-save-btn">Salvar</button>
                    </div>
                </div>
            `;
        }
        
        item.innerHTML = mainHtml + editHtml;

        if (prepend) {
            container.prepend(item);
        } else {
            container.appendChild(item);
        }

        // Adiciona listeners para os botões do card
        if (isNew) {
            item.querySelector('.admin-user-savenew-btn').addEventListener('click', handleAdminSaveNewUser);
        } else {
            item.querySelector('.admin-user-edit-btn').addEventListener('click', showUserEditForm);
            item.querySelector('.admin-user-delete-btn').addEventListener('click', handleAdminDeleteUser);
            item.querySelector('.admin-user-save-btn').addEventListener('click', handleAdminUpdateUser);
            
            // (Req 1) Adiciona listener para o botão de desbloquear
            const unlockBtn = item.querySelector('.admin-user-unlock-btn');
            if (unlockBtn) {
                unlockBtn.addEventListener('click', handleAdminUnlockUser);
            }
        }
        item.querySelector('.admin-user-cancel-btn').addEventListener('click', hideUserEditForm);
        item.querySelector('.admin-password-toggle-btn').addEventListener('click', handlePasswordToggle);
    }

    // (Req 1) NOVO: Handler para o botão "Salvar Novo"
    function handleAdminSaveNewUser(e) {
        const item = e.target.closest('.admin-user-card');
        const btn = e.target;
        
        const newUsername = item.querySelector('.edit-username-input').value.trim();
        const newPassword = item.querySelector('.edit-password-input').value;
        const newArea = item.querySelector('.edit-area-select').value;
        const newRole = item.querySelector('.edit-role-select').value;
        const allowedAreas = Array.from(item.querySelectorAll('.allowed-area-checkbox:checked')).map(cb => cb.value);

        if (!newUsername || !newPassword) {
            alert("Login de funcionário e Senha são obrigatórios para criar um novo usuário.");
            return;
        }

        const payload = {
            username: newUsername,
            password: newPassword,
            area: newArea,
            role: newRole,
            allowed_areas: allowedAreas.length ? allowedAreas : [newArea]
        };

        btn.disabled = true;

        // Chama o NOVO endpoint de API que você precisará adicionar ao backend
        fetch('/api/admin/add-user', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                
                // --- INÍCIO DA MODIFICAÇÃO (Remover o "Novo" e Adicionar o "Salvo") ---
                
                // 1. Remove o card temporário (com isNew: true) da lista global
                globalCmsUsers = globalCmsUsers.filter(user => !user.isNew);

                // 2. Cria o objeto de usuário salvo
                const savedUser = {
                    username: newUsername,
                    // (Oculto na UI de edição, mas precisamos dele na global para o caso de o admin editar de novo)
                    password: newPassword, 
                    area: newArea,
                    role: newRole,
                    isNew: false 
                };
                
                // 3. Adiciona o usuário salvo ao topo da lista global
                globalCmsUsers.unshift(savedUser);
                
                // 4. Re-renderiza a lista inteira (agora correta)
                renderAdminUsers(); 
                
                // --- FIM DA MODIFICAÇÃO ---

            } else {
                alert(`Erro: ${data.mensagem || 'Falha ao criar usuário'}`);
                btn.disabled = false;
            }
        })
        .catch((err) => {
             alert('Erro de comunicação. O usuário não foi criado.');
             btn.disabled = false;
        });
    }

    // NOVO: Função para filtrar as listas do admin
    function handleAdminSearch() {
        const searchInput = document.getElementById('admin-search-input');
        // Se o input não existir (ex: modal fechado), não faz nada
        if (!searchInput) return;
        
        const searchTerm = searchInput.value.toLowerCase();
        const areaFilter = document.getElementById('admin-area-filter');
        const selectedArea = areaFilter ? areaFilter.value : '';
        const roleFilter = document.getElementById('admin-role-filter');
        const selectedRole = roleFilter ? roleFilter.value : '';
        const platformFilter = document.getElementById('admin-platform-filter');
        const selectedPlatform = platformFilter ? platformFilter.value : '';

        // 1. Descobre qual painel de aba está ativo
        let activePanel = null;
        let activeTabName = null;
        if (!adminUsersTab.classList.contains('hidden')) {
            activePanel = adminUsersTab;
            activeTabName = 'users';
        } else if (!adminDashboardsTab.classList.contains('hidden')) {
            activePanel = adminDashboardsTab;
            activeTabName = 'dashboards';
        } else if (!adminAutomationsTab.classList.contains('hidden')) {
            activePanel = adminAutomationsTab;
            activeTabName = 'automations';
        }

        // Se nenhum painel pesquisável estiver ativo, não faz nada
        if (!activePanel) return;

        // 2. Encontra o contêiner da lista dentro do painel ativo
        const listContainer = activePanel.querySelector('#admin-user-list-container, #admin-dashboards-list, #admin-automations-list');
        if (!listContainer) return;

        const allCards = listContainer.querySelectorAll('.admin-user-card, .admin-cms-card');
        let itemsFound = 0;

        // 3. Itera sobre os cards e aplica o filtro
        allCards.forEach(card => {
            // Seleciona o elemento que contém o nome (usuário ou nome do CMS)
            const nameElement = card.querySelector('.username, .admin-cms-info .name');
            if (nameElement) {
                const name = nameElement.textContent.toLowerCase();
                const matchesSearch = name.includes(searchTerm);

                // Filtro de área (para cards CMS e cards de usuário)
                let matchesArea = true;
                if (selectedArea) {
                    if (card.classList.contains('admin-cms-card')) {
                        const detailsEl = card.querySelector('.admin-cms-info .details');
                        if (detailsEl) {
                            const areaMatch = detailsEl.textContent.match(/Área:\s*([^|]+)/);
                            const cardArea = areaMatch ? areaMatch[1].trim() : '';
                            matchesArea = cardArea === selectedArea;
                        }
                    } else if (card.classList.contains('admin-user-card')) {
                        const detailsEl = card.querySelector('.admin-user-info .details');
                        if (detailsEl) {
                            const areaMatch = detailsEl.textContent.match(/Área:\s*([^|]+)/);
                            const cardArea = areaMatch ? areaMatch[1].trim() : '';
                            matchesArea = cardArea === selectedArea;
                        }
                    }
                }

                let matchesRole = true;
                if (selectedRole && activeTabName === 'users' && card.classList.contains('admin-user-card')) {
                    const detailsEl = card.querySelector('.admin-user-info .details');
                    if (detailsEl) {
                        const roleMatch = detailsEl.textContent.match(/Função:\s*([^|]+)/);
                        const cardRole = roleMatch ? roleMatch[1].trim() : '';
                        matchesRole = cardRole === selectedRole;
                    }
                }

                // Filtro de plataforma (para cards de dashboard)
                let matchesPlatform = true;
                if (selectedPlatform && card.classList.contains('admin-cms-card')) {
                    if (activeTabName === 'dashboards') {
                        matchesPlatform = card.dataset.systemKey === selectedPlatform;
                    } else if (activeTabName === 'automations') {
                        matchesPlatform = (card.dataset.platform || '').toLowerCase() === selectedPlatform;
                    }
                }

                if (matchesSearch && matchesArea && matchesRole && matchesPlatform) {
                    card.style.display = 'flex';
                    itemsFound++;
                } else {
                    card.style.display = 'none';
                }
            }
        });

        // 4. Gerencia a mensagem de "Nenhum resultado"
        let noResultsMsg = listContainer.querySelector('.no-results-message');
        if (!noResultsMsg) {
            noResultsMsg = document.createElement('p');
            noResultsMsg.className = 'no-requests no-results-message'; // Reutiliza o estilo
            noResultsMsg.style.display = 'none'; // Começa oculto
            listContainer.appendChild(noResultsMsg);
        }

        const hasFilter = searchTerm !== '' || selectedArea !== '' || selectedRole !== '' || selectedPlatform !== '';
        if (itemsFound === 0 && hasFilter) {
            noResultsMsg.textContent = 'Nenhum item encontrado para o filtro selecionado.';
            noResultsMsg.style.display = 'block';
        } else {
            noResultsMsg.style.display = 'none';
        }
    }

    // --- NOVA FUNÇÃO (Req 3): Alterna a visibilidade da senha ---
    function handlePasswordToggle(e) {
        const btn = e.target;
        const wrapper = btn.closest('.password-toggle-wrapper');
        const input = wrapper.querySelector('.edit-password-input');
        
        if (input.type === 'password') {
            input.type = 'text';
            btn.classList.remove('fa-eye');
            btn.classList.add('fa-eye-slash');
        } else {
            input.type = 'password';
            btn.classList.remove('fa-eye-slash');
            btn.classList.add('fa-eye');
        }
    }

    function showUserEditForm(e) {
        const itemClicked = e.target.closest('.admin-user-card');
        
        // --- INÍCIO DA MODIFICAÇÃO (Req 1: Fechar "Adicionar") ---
        // Fecha outros cards abertos (incluindo o card "Adicionar")
        closeAllEditForms(adminUserListContainer);
        // --- FIM DA MODIFICAÇÃO ---
        
        // Abre o card clicado
        itemClicked.querySelector('.admin-user-main').classList.add('hidden');
        itemClicked.querySelector('.admin-user-edit-form').classList.remove('hidden');
    }

    function hideUserEditForm(e) {
        const item = e.target.closest('.admin-user-card');

        // --- INÍCIO DA MODIFICAÇÃO (Req 1: Cancelar "Adicionar") ---
        if (item.dataset.isNew === 'true') {
            const tempId = item.dataset.tempId;
            globalCmsUsers = globalCmsUsers.filter(u => u.username !== tempId); // Remove da global
            item.remove(); // Remove o card
            
            if (adminUserListContainer.children.length === 0) {
                 adminUserListContainer.innerHTML = '<p class="no-requests">Nenhum usuário (além do admin) encontrado.</p>';
            }
            return;
        }
        // --- FIM DA MODIFICAÇÃO ---
        
        item.querySelector('.admin-user-main').classList.remove('hidden');
        item.querySelector('.admin-user-edit-form').classList.add('hidden');
        
        // Reseta o ícone e o tipo de input
        const input = item.querySelector('.edit-password-input');
        const icon = item.querySelector('.admin-password-toggle-btn');
        input.type = 'password';
        icon.classList.remove('fa-eye-slash');
        icon.classList.add('fa-eye');
    }

    function handleAdminUpdateUser(e) {
        const item = e.target.closest('.admin-user-card');
        const username = item.dataset.username;
        const btn = e.target;
        
        const payload = {
            username: username,
            password: item.querySelector('.edit-password-input').value, // Envia vazio ou preenchido
            area: item.querySelector('.edit-area-select').value,
            role: item.querySelector('.edit-role-select').value,
            allowed_areas: Array.from(item.querySelectorAll('.allowed-area-checkbox:checked')).map(cb => cb.value)
        };
        if (!payload.allowed_areas.length) payload.allowed_areas = [payload.area];

        btn.disabled = true;

        fetch('/api/admin/update-user', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                // Atualiza a UI localmente
                item.querySelector('.admin-user-info .details').innerHTML = `
                    <strong>Área:</strong> ${payload.area} | <strong>Função:</strong> ${payload.role}
                `;
                hideUserEditForm(e); // Volta para a tela de info
            } else {
                alert(`Erro: ${data.mensagem}`);
            }
        })
        .finally(() => {
            btn.disabled = false;
        });
    }

    function handleAdminUnlockUser(e) {
        const item = e.target.closest('.admin-user-card');
        const username = item.dataset.username;
        const btn = e.target;
        
        btn.disabled = true;
        btn.textContent = '...'; // Feedback visual

        fetch('/api/admin/unlock-user', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username: username })
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                // 1. Remove o status e o botão da UI
                const statusSpan = item.querySelector('.user-status-locked');
                if (statusSpan) statusSpan.remove();
                btn.remove(); // Remove o botão de desbloqueio
                
                // 2. Atualiza o estado global
                const userInGlobal = globalCmsUsers.find(u => u.username === username);
                if (userInGlobal) {
                    userInGlobal.lockout_until = null;
                }
            } else {
                alert(`Erro: ${data.mensagem || 'Falha ao desbloquear usuário'}`);
                btn.disabled = false;
                btn.textContent = 'Desbloquear'; // Restaura o texto
            }
        })
        .catch((err) => {
             alert('Erro de comunicação. O usuário não foi desbloqueado.');
             btn.disabled = false;
             btn.textContent = 'Desbloquear'; // Restaura o texto
        });
    }

    function handleAdminDeleteUser(e) {
        const item = e.target.closest('.admin-user-card');
        const username = item.dataset.username;
        
        // (Prevenção) Não deixa excluir o card de "novo usuário"
        if (item.dataset.isNew === 'true') {
            item.remove();
            return;
        }

        e.target.disabled = true;

        fetch('/api/admin/delete-user', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username: username })
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'sucesso') {
                item.remove(); // Remove o card da UI
                
                // --- INÍCIO DA MODIFICAÇÃO (Remover da Global) ---
                // Remove o usuário da lista local para evitar que ele reapareça
                globalCmsUsers = globalCmsUsers.filter(user => user.username !== username);
                // --- FIM DA MODIFICAÇÃO ---

                // Verifica se a lista (agora vazia no DOM) precisa da mensagem
                if (adminUserListContainer.children.length === 0) {
                    adminUserListContainer.innerHTML = '<p class="no-requests">Nenhum usuário (além do admin) encontrado.</p>';
                }
            } else {
                alert(`Erro: ${data.mensagem}`);
                e.target.disabled = false;
            }
        });
    }
    
    // Listeners das Abas
    tabAdminDashboards.addEventListener('click', () => showAdminTab('dashboards'));
    tabAdminAutomations.addEventListener('click', () => showAdminTab('automations'));

    // --- Gerenciamento de Automações ---
    
    function renderAdminAutomations() {
        adminAutomationsList.innerHTML = '';
        
        const keys = Object.keys(globalCmsAutomations);
        
        if (keys.length === 0) {
            adminAutomationsList.innerHTML = '<p class="no-requests">Nenhuma automação cadastrada.</p>';
            return;
        }

        keys.forEach((key, index) => {
            const auto = globalCmsAutomations[key];
            const item = document.createElement('div');
            item.className = 'admin-cms-card';
            item.dataset.key = key; // Usa a Chave (Nome) como ID

            const keyNameValue = auto.isNew ? '' : key;

            item.innerHTML = `
                <div class="admin-cms-main">
                    <div class="admin-cms-info">
                        <div class="name">${key}</div>
                        <div class="details"><strong>Sistema:</strong> ${auto.type.toUpperCase()} | <strong>Macro:</strong> ${auto.macro || 'N/A'} | <strong>Área:</strong> ${auto.hub_area || 'Spare Parts'}</div>
                    </div>
                    <div class="admin-cms-actions">
                        <button class="button btn-warning admin-auto-edit-btn">Editar</button>
                        <button class="button btn-danger admin-auto-delete-btn">Excluir</button>
                        <button class="admin-drag-handle" title="Arrastar para reordenar" data-key="${key}">
                            <i class="fas fa-grip-vertical"></i>
                        </button>
                    </div>
                </div>
                <div class="admin-cms-edit-form hidden">
                    <div class="modal-input-group">
                        <label>Nome de Exibição:</label>
                        <input type="text" class="hub-modal-input edit-auto-name" value="${keyNameValue}">
                    </div>
                    <div class="modal-input-group">
                        <label>Tipo:</label>
                        <select class="hub-modal-input edit-auto-type">
                            <option value="sap" ${auto.type === 'sap' ? 'selected' : ''}>SAP</option>
                            <option value="bw" ${auto.type === 'bw' ? 'selected' : ''}>BW</option>
                            <option value="salesforce" ${auto.type === 'salesforce' ? 'selected' : ''}>Salesforce</option>
                        </select>
                    </div>
                    <div class="modal-input-group">
                        <label>Área:</label>
                        <select class="hub-modal-input edit-auto-hub-area">
                            <option value="Spare Parts" ${(auto.hub_area || 'Spare Parts') === 'Spare Parts' ? 'selected' : ''}>Spare Parts</option>
                            <option value="Finished Goods" ${auto.hub_area === 'Finished Goods' ? 'selected' : ''}>Finished Goods</option>
                        </select>
                    </div>
                    <div class="modal-input-group">
                        <label>Nome da Macro:</label>
                        <input type="text" class="hub-modal-input edit-auto-macro" value="${auto.macro || ''}">
                    </div>
                    <div class="modal-input-group">
                        <label>Caminho do Arquivo:</label>
                        <input type="text" class="hub-modal-input edit-auto-file" value="${auto.arquivo || ''}">
                    </div>
                    
                    <div class="modal-input-group">
                        <label>Caminho do Preview:</label>
                        <input type="text" class="hub-modal-input edit-auto-gif" value="${auto.gif || ''}">
                    </div>
                    <div class="modal-input-group">
                        <label>Descrição do Preview:</label>
                        <textarea class="hub-modal-input edit-auto-text">${auto.text || ''}</textarea>
                    </div>
                    <div class="modal-input-group">
                        <label>Tags do Preview:</label>
                        <input type="text" class="hub-modal-input edit-auto-tags" value="${auto.tags || ''}">
                    </div>
                    <div class="modal-input-group">
                        <label>Largura do Preview:</label>
                        <input type="text" class="hub-modal-input edit-auto-width" value="${auto.width || ''}">
                    </div>
                    
                    <div class="admin-cms-edit-actions">
                        <button class="button btn-cancel admin-auto-cancel-btn">Cancelar</button>
                        <button class="button btn-success admin-auto-save-btn">Salvar</button>
                    </div>
                </div>
            `;
            adminAutomationsList.appendChild(item);
        });

        // Adiciona Listeners
        adminAutomationsList.querySelectorAll('.admin-auto-edit-btn').forEach(b => b.addEventListener('click', showCmsEditForm));
        adminAutomationsList.querySelectorAll('.admin-auto-cancel-btn').forEach(b => b.addEventListener('click', hideCmsEditForm));
        adminAutomationsList.querySelectorAll('.admin-auto-save-btn').forEach(b => b.addEventListener('click', handleAutomationSave));
        adminAutomationsList.querySelectorAll('.admin-auto-delete-btn').forEach(b => b.addEventListener('click', handleAutomationDelete));
        adminAutomationsList.querySelectorAll('.admin-drag-handle').forEach(handle => attachAutomationDragHandle(handle));
        adminAutomationsList.querySelectorAll('.admin-cms-card').forEach(card => setupAutomationDragCard(card));
    }

    let automationDragAllowed = false;
    let automationDragSourceKey = null;

    function attachAutomationDragHandle(handle) {
        handle.addEventListener('pointerdown', (event) => {
            if (event.button !== undefined && event.button !== 0) return;
            automationDragAllowed = true;
        });
    }

    document.addEventListener('pointerup', () => {
        automationDragAllowed = false;
    });

    function setupAutomationDragCard(card) {
        card.setAttribute('draggable', 'true');
        card.addEventListener('dragstart', handleAutomationDragStart);
        card.addEventListener('dragover', handleAutomationDragOver);
        card.addEventListener('drop', handleAutomationDrop);
        card.addEventListener('dragend', handleAutomationDragEnd);
    }

    function handleAutomationDragStart(e) {
        if (!automationDragAllowed) {
            e.preventDefault();
            return;
        }
        automationDragSourceKey = e.currentTarget.dataset.key;
        e.currentTarget.classList.add('dragging');
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/plain', automationDragSourceKey);
    }

    function handleAutomationDragOver(e) {
        if (!automationDragSourceKey) return;
        e.preventDefault();
        const target = e.currentTarget;
        if (target.dataset.key === automationDragSourceKey) return;

        const draggingCard = adminAutomationsList.querySelector('.admin-cms-card.dragging');
        if (!draggingCard) return;

        const bounding = target.getBoundingClientRect();
        const offset = e.clientY - bounding.top;
        const shouldInsertBefore = offset < bounding.height / 2;

        if (shouldInsertBefore) {
            adminAutomationsList.insertBefore(draggingCard, target);
        } else {
            adminAutomationsList.insertBefore(draggingCard, target.nextSibling);
        }
    }

    function handleAutomationDrop(e) {
        e.preventDefault();
    }

    function handleAutomationDragEnd(e) {
        e.currentTarget.classList.remove('dragging');
        automationDragAllowed = false;
        automationDragSourceKey = null;
        persistAutomationOrderFromDom();
    }

    function persistAutomationOrderFromDom() {
        const currentOrder = Object.keys(globalCmsAutomations);
        const domOrder = Array.from(adminAutomationsList.querySelectorAll('.admin-cms-card'))
            .map(card => card.dataset.key);

        if (domOrder.length !== currentOrder.length) {
            renderAdminAutomations();
            return;
        }

        let changed = false;
        for (let i = 0; i < domOrder.length; i++) {
            if (domOrder[i] !== currentOrder[i]) {
                changed = true;
                break;
            }
        }

        if (!changed) {
            renderAdminAutomations();
            return;
        }

        const reordered = {};
        domOrder.forEach(key => {
            reordered[key] = globalCmsAutomations[key];
        });

        globalCmsAutomations = reordered;
        saveCmsData('automations');
        renderAdminAutomations();
    }

    // Botão Adicionar (Automação) - CORRIGIDO
    adminAddAutomationBtn.addEventListener('click', () => {
        // (Req 3: Evitar Auto-Save) - Fecha qualquer outro formulário
        closeAllEditForms(adminAutomationsList); 

        const newKey = `Nova Automação ${Date.now()}`; // Garante chave única
        
        // --- INÍCIO DA MODIFICAÇÃO (Req 1: Adicionar campos de preview) ---
        const newAutomation = {
            [newKey]: {
                arquivo: "", // Vazio
                macro: "", // Vazio
                type: "sap",
                hub_area: "Spare Parts", // Padrão
                isNew: true, // Flag para o "Cancelar"
                // Novos campos de preview
                gif: "",
                text: "",
                tags: "",
                width: null
            }
        };
        // --- FIM DA MODIFICAÇÃO ---

        // Coloca o novo item no início do objeto global
        globalCmsAutomations = { ...newAutomation, ...globalCmsAutomations };
        
        renderAdminAutomations();
        
        const newCard = adminAutomationsList.querySelector(`[data-key="${newKey}"]`);
        if (newCard) {
            newCard.querySelector('.admin-cms-main').classList.add('hidden');
            newCard.querySelector('.admin-cms-edit-form').classList.remove('hidden');
            adminAutomationsList.scrollTop = 0;
        }
    });

    function handleAutomationSave(e) {
        const item = e.target.closest('.admin-cms-card');
        const oldKey = item.dataset.key;
        
        // --- INÍCIO DA MODIFICAÇÃO (Req 1: Check if new) ---
        // 1. Verifica se era um item "Novo" ANTES de modificar
        const wasNewItem = globalCmsAutomations[oldKey]?.isNew === true;
        // --- FIM DA MODIFICAÇÃO ---

        const form = item.querySelector('.admin-cms-edit-form');
        const newKey = form.querySelector('.edit-auto-name').value;
        const newType = form.querySelector('.edit-auto-type').value;
        const newHubArea = form.querySelector('.edit-auto-hub-area').value;
        const newMacro = form.querySelector('.edit-auto-macro').value;
        const newFile = form.querySelector('.edit-auto-file').value;
        
        const newGif = form.querySelector('.edit-auto-gif').value;
        const newText = form.querySelector('.edit-auto-text').value;
        const newTags = form.querySelector('.edit-auto-tags').value;
        const newWidth = form.querySelector('.edit-auto-width').value;

        if (!newKey) {
            alert("O Nome (Chave) é obrigatório.");
            return;
        }

        const newData = {
            type: newType,
            hub_area: newHubArea,
            macro: newMacro || null,
            arquivo: newFile || null,
            gif: newGif || null,
            text: newText || null,
            tags: newTags || null,
            width: newWidth || null
            // A flag 'isNew' é omitida, removendo-a
        };

        // --- INÍCIO DA MODIFICAÇÃO (Req 1 & 2: Manter Ordem) ---

        // 2. Reconstrói o objeto global na ordem correta (em memória)
        const newGlobalCms = {};
        const currentKeys = Object.keys(globalCmsAutomations);

        if (wasNewItem) {
            // (Item NOVO) Adiciona o novo item salvo ao topo
            newGlobalCms[newKey] = newData;
            // Adiciona o restante, pulando a chave temporária ("Nova Automação...")
            currentKeys.forEach(key => {
                if (key !== oldKey) {
                    newGlobalCms[key] = globalCmsAutomations[key];
                }
            });

        } else {
            // (Item EDITADO) Reconstrói na mesma ordem, apenas substituindo a chave/dados
            currentKeys.forEach(key => {
                if (key === oldKey) {
                    // Se a chave mudou (ex: "Nome A" -> "Nome B"), usa a nova chave
                    newGlobalCms[newKey] = newData;
                } else {
                    // Senão, mantém o item antigo
                    newGlobalCms[key] = globalCmsAutomations[key];
                }
            });
        }
        
        // 3. Atualiza a memória global
        globalCmsAutomations = newGlobalCms;

        // 4. Salva a nova ordem (corrigida) no servidor e no sessionStorage
        saveCmsData('automations');
        
        // 5. Re-renderiza a lista (agora na ordem correta da memória)
        renderAdminAutomations();
        handleAdminSearch();

        // 6. Se era um item NOVO, rola a lista para o topo
        if (wasNewItem) {
            adminAutomationsList.scrollTop = 0;
        }
        // --- FIM DA MODIFICAÇÃO ---
    }
    
    function handleAutomationDelete(e) {
        const item = e.target.closest('.admin-cms-card');
        const key = item.dataset.key;
        
        delete globalCmsAutomations[key];
        saveCmsData('automations');
        renderAdminAutomations();
        handleAdminSearch();
    }


    // --- Gerenciamento de Dashboards ---
    
    function renderAdminDashboards() {
        adminDashboardsList.innerHTML = '';
        
        // (Req 1) Ordem Fixa
        const systemOrder = ['looker', 'tableau', 'library'];

        // (Req 1) Gera as <options> para o seletor de Sistema
        const systemOptionsHtml = systemOrder.map(sysKey => {
            if (!globalCmsDashboards[sysKey]) return '';
            return `<option value="${sysKey}">${globalCmsDashboards[sysKey].system_name}</option>`;
        }).join('');

        systemOrder.forEach(systemKey => {
            const systemData = globalCmsDashboards[systemKey];
            if (!systemData) return; 

            for (const [areaKey, areaData] of Object.entries(systemData.areas)) {
                areaData.items.forEach((item, index) => {
                    
                    // (Req 1) Gera as <options> de Área para este item específico
                    const areaOptionsHtml = Object.keys(systemData.areas).map(aKey => {
                        const selected = (aKey === areaKey) ? 'selected' : '';
                        return `<option value="${aKey}" ${selected}>${systemData.areas[aKey].name}</option>`;
                    }).join('');

                    // (Req 1) Atualiza o seletor de Sistema para este item
                    const itemSystemOptionsHtml = systemOrder.map(sysKey => {
                        if (!globalCmsDashboards[sysKey]) return '';
                        const selected = (sysKey === systemKey) ? 'selected' : '';
                        return `<option value="${sysKey}" ${selected}>${globalCmsDashboards[sysKey].system_name}</option>`;
                    }).join('');

                    const card = document.createElement('div');
                    card.className = 'admin-cms-card';
                    card.dataset.systemKey = systemKey;
                    card.dataset.areaKey = areaKey;
                    card.dataset.index = index; 

                    // --- INÍCIO DA MODIFICAÇÃO (Req 2) ---
                    card.innerHTML = `
                        <div class="admin-cms-main">
                            <div class="admin-cms-info">
                                <div class="name">${item.name}${item.general ? ' <span class="admin-badge-general">Geral</span>' : ''}</div>
                                <div class="details">
                                    <strong>Plataforma:</strong> ${systemData.system_name} | <strong>Negócio:</strong> ${areaData.name} | <strong>Área:</strong> ${item.hub_area || 'Spare Parts'}
                                </div>
                            </div>
                            <div class="admin-cms-actions">
                                <button class="button btn-warning admin-dash-edit-btn">Editar</button>
                                <button class="button btn-danger admin-dash-delete-btn">Excluir</button>
                            </div>
                        </div>
                        <div class="admin-cms-edit-form hidden">
                            
                            <div class="scheduler-datetime-group" style="gap: 15px;">
                                <div class="modal-input-group">
                                    <label>Plataforma:</label>
                                    <select class="hub-modal-input edit-dash-systemKey">${itemSystemOptionsHtml}</select>
                                </div>
                                <div class="modal-input-group">
                                    <label>Negócio:</label>
                                    <select class="hub-modal-input edit-dash-areaKey">${areaOptionsHtml}</select>
                                </div>
                                <div class="modal-input-group">
                                    <label>Área:</label>
                                    <select class="hub-modal-input edit-dash-hub-area">
                                    <option value="Spare Parts" ${(item.hub_area || 'Spare Parts') === 'Spare Parts' ? 'selected' : ''}>Spare Parts</option>
                                    <option value="Finished Goods" ${item.hub_area === 'Finished Goods' ? 'selected' : ''}>Finished Goods</option>
                                    </select>
                                </div>
                            </div>

                            <div class="modal-input-group">
                                <label>ID:</label>
                                <input type="text" class="hub-modal-input edit-dash-id" value="${item.id}">
                            </div>
                            <div class="modal-input-group">
                                <label>Nome de Exibição:</label>
                                <input type="text" class="hub-modal-input edit-dash-name" value="${item.name}">
                            </div>
                            <div class="modal-input-group">
                                <label>URL:</label>
                                <input type="text" class="hub-modal-input edit-dash-url" value="${item.url}">
                            </div>
                            <div class="modal-input-group">
                                <label>Caminho do Preview:</label>
                                <input type="text" class="hub-modal-input edit-dash-gif" value="${item.gif || ''}">
                            </div>
                            <div class="modal-input-group">
                                <label>Descrição do Preview:</label>
                                <textarea class="hub-modal-input edit-dash-text">${item.text || ''}</textarea>
                            </div>
                            <div class="modal-input-group">
                                <label>Tags do Preview:</label>
                                <input type="text" class="hub-modal-input edit-dash-tags" value="${item.tags || ''}">
                            </div>
                            
                            <div class="modal-input-group">
                                <label>Largura do Preview:</label>
                                <input type="text" class="hub-modal-input edit-dash-width" value="${item.width || ''}">
                            </div>
                            
                            <div class="modal-input-group admin-toggle-group">
                                <label class="admin-toggle-label">
                                    <input type="checkbox" class="edit-dash-general" ${item.general ? 'checked' : ''}>
                                    <span>Dashboard Geral</span>
                                </label>
                                <small class="admin-toggle-hint">Visível para todos, inclusive usuários não logados.</small>
                            </div>

                            <div class="admin-cms-edit-actions">
                                <button class="button btn-cancel admin-dash-cancel-btn">Cancelar</button>
                                <button class="button btn-success admin-dash-save-btn">Salvar</button>
                            </div>
                        </div>
                    `;
                    adminDashboardsList.appendChild(card);

                    // (Req 1) Adiciona o listener para atualizar as Áreas dinamicamente
                    const systemSelect = card.querySelector('.edit-dash-systemKey');
                    const areaSelect = card.querySelector('.edit-dash-areaKey');
                    systemSelect.addEventListener('change', () => updateAreaDropdown(systemSelect, areaSelect));
                });
            }
        }); 
        
        // Listeners (movidos para fora do loop)
        adminDashboardsList.querySelectorAll('.admin-dash-edit-btn').forEach(b => b.addEventListener('click', showCmsEditForm));
        adminDashboardsList.querySelectorAll('.admin-dash-cancel-btn').forEach(b => b.addEventListener('click', hideCmsEditForm));
        adminDashboardsList.querySelectorAll('.admin-dash-save-btn').forEach(b => b.addEventListener('click', handleDashboardSave));
        adminDashboardsList.querySelectorAll('.admin-dash-delete-btn').forEach(b => b.addEventListener('click', handleDashboardDelete));
    }
    
    // NOVO: Botão Adicionar Dashboard (Req 2)
    if (adminAddDashboardBtn) { 
        adminAddDashboardBtn.addEventListener('click', () => {
            
            // (Req 3: Evitar Auto-Save) - Fecha qualquer outro formulário
            closeAllEditForms(adminDashboardsList);

            const defaultSystemKey = 'looker';
            if (!globalCmsDashboards[defaultSystemKey] || !globalCmsDashboards[defaultSystemKey].areas) {
                alert("Erro: Sistema 'looker' não encontrado. Não é possível adicionar dashboard.");
                return;
            }
            const defaultAreaKey = Object.keys(globalCmsDashboards[defaultSystemKey].areas)[0];
            if (!defaultAreaKey) {
                alert("Erro: Sistema 'looker' não possui áreas. Não é possível adicionar dashboard.");
                return;
            }
            
            // (Req 1: Campos Vazios)
            const newItem = {
                id: "", // Vazio
                name: "", // Vazio
                url: "", // Vazio
                gif: "", // Vazio
                text: "", // Vazio
                tags: "", // Vazio
                width: null,
                isNew: true // Flag para o "Cancelar"
            };

            // Adiciona o novo item ao INÍCIO da área padrão
            globalCmsDashboards[defaultSystemKey].areas[defaultAreaKey].items.unshift(newItem);
            
            renderAdminDashboards();
            
            const newCard = adminDashboardsList.firstChild; 
            if (newCard) {
                newCard.querySelector('.admin-cms-main').classList.add('hidden');
                newCard.querySelector('.admin-cms-edit-form').classList.remove('hidden');
                
                // --- INÍCIO DA MODIFICAÇÃO (Rolar para o Topo) ---
                // Muda de 'center' para 'start'
                adminDashboardsList.scrollTop = 0;
                // --- FIM DA MODIFICAÇÃO ---
            }
        });
    } // <-- FECHA A VERIFICAÇÃO

    function handleDashboardSave(e) {
        const item = e.target.closest('.admin-cms-card');
        // Localização ANTIGA (de onde o item VEIO)
        const { systemKey: oldSystemKey, areaKey: oldAreaKey, index: oldIndex } = item.dataset;
        
        const form = item.querySelector('.admin-cms-edit-form');

        // --- INÍCIO DA MODIFICAÇÃO (Req 1) ---
        // Localização NOVA (para onde o item VAI)
        const newSystemKey = form.querySelector('.edit-dash-systemKey').value;
        const newAreaKey = form.querySelector('.edit-dash-areaKey').value;
        const newHubArea = form.querySelector('.edit-dash-hub-area').value;

        // Pega o item original (do local antigo)
        const dashboardItem = globalCmsDashboards[oldSystemKey].areas[oldAreaKey].items[oldIndex];
        
        // Atualiza o objeto com os dados do formulário
        dashboardItem.id = form.querySelector('.edit-dash-id').value;
        dashboardItem.name = form.querySelector('.edit-dash-name').value;
        dashboardItem.url = form.querySelector('.edit-dash-url').value;
        dashboardItem.gif = form.querySelector('.edit-dash-gif').value;
        dashboardItem.text = form.querySelector('.edit-dash-text').value;
        dashboardItem.tags = form.querySelector('.edit-dash-tags').value;
        dashboardItem.width = form.querySelector('.edit-dash-width').value || null;
        dashboardItem.hub_area = newHubArea;
        dashboardItem.general = form.querySelector('.edit-dash-general').checked;
        dashboardItem.isNew = false; // (Garante que a flag de "novo" seja removida)

        // (Req 1) Lógica de MOVER o item se o sistema ou área mudou
        if (oldSystemKey !== newSystemKey || oldAreaKey !== newAreaKey) {
            
            // 1. Verifica se a nova área existe (segurança)
            if (!globalCmsDashboards[newSystemKey] || !globalCmsDashboards[newSystemKey].areas[newAreaKey]) {
                alert(`Erro: A área '${newAreaKey}' não existe no sistema '${newSystemKey}'.`);
                return;
            }

            // 2. Remove do local antigo
            globalCmsDashboards[oldSystemKey].areas[oldAreaKey].items.splice(oldIndex, 1);
            // 3. Adiciona ao novo local (no final da lista)
            globalCmsDashboards[newSystemKey].areas[newAreaKey].items.push(dashboardItem);
        }
        // --- FIM DA MODIFICAÇÃO ---
        
        // Salva no servidor
        saveCmsData('dashboards');
        // Re-renderiza a UI inteira com a nova estrutura
        renderAdminDashboards();
        handleAdminSearch();
    }
    
    function handleDashboardDelete(e) {
        const item = e.target.closest('.admin-cms-card');
        const { systemKey, areaKey, index } = item.dataset;
        const itemData = globalCmsDashboards[systemKey].areas[areaKey].items[index];
        
        // Remove o item do array
        globalCmsDashboards[systemKey].areas[areaKey].items.splice(index, 1);
        
        saveCmsData('dashboards');
        renderAdminDashboards();
        handleAdminSearch();
    }


    // --- Funções Genéricas do CMS ---
    
    // NOVO: Fecha todos os formulários de edição abertos em um contêiner
    function closeAllEditForms(container) {
        if (!container) return;
        
        let itemRemoved = false; // Flag para ver se precisamos re-renderizar

        container.querySelectorAll('.admin-cms-card, .admin-user-card').forEach(card => {
            const editForm = card.querySelector('.admin-cms-edit-form') || card.querySelector('.admin-user-edit-form');
            const mainView = card.querySelector('.admin-cms-main') || card.querySelector('.admin-user-main');
            
            if (editForm && !editForm.classList.contains('hidden')) {
                
                // --- INÍCIO DA MODIFICAÇÃO (Req 1: Adicionar checagem de Usuário) ---
                const isNewUser = card.dataset.isNew === 'true';
                if (isNewUser) {
                    const tempId = card.dataset.tempId;
                    globalCmsUsers = globalCmsUsers.filter(u => u.username !== tempId); // Remove da global
                    itemRemoved = true; // Marca para re-renderizar
                    return; // Sai do loop deste card, pois ele será removido
                }
                // --- FIM DA MODIFICAÇÃO ---

                const autoKey = card.dataset.key;
                if (autoKey && globalCmsAutomations[autoKey]?.isNew) {
                    delete globalCmsAutomations[autoKey];
                    itemRemoved = true; 
                    return; 
                }

                const dashIndex = card.dataset.index;
                if (dashIndex) {
                    const { systemKey, areaKey } = card.dataset;
                    if (globalCmsDashboards[systemKey]?.areas[areaKey]?.items[dashIndex]?.isNew) {
                        globalCmsDashboards[systemKey].areas[areaKey].items.splice(dashIndex, 1);
                        itemRemoved = true; 
                        return; 
                    }
                }

                // Se não for um item novo, apenas esconde o form
                editForm.classList.add('hidden');
                if (mainView) mainView.classList.remove('hidden');
                
                // Reseta a senha se for um card de usuário
                const passInput = editForm.querySelector('.edit-password-input');
                if (passInput) passInput.type = 'password';
                const passIcon = editForm.querySelector('.admin-password-toggle-btn');
                if (passIcon) {
                    passIcon.classList.remove('fa-eye-slash');
                    passIcon.classList.add('fa-eye');
                }
            }
        });

        // Se removemos um item, re-renderiza a lista correta
        if (itemRemoved) {
            if (container.id === 'admin-automations-list') {
                renderAdminAutomations();
                handleAdminSearch();
            } else if (container.id === 'admin-dashboards-list') {
                renderAdminDashboards();
                handleAdminSearch();
            } else if (container.id === 'admin-user-list-container') {
                // (Req 1) Adiciona o re-render para usuários
                renderAdminUsers();
            }
        }
    }

    function showCmsEditForm(e) {
        const item = e.target.closest('.admin-cms-card');
        // REQ 3: Fecha outros forms abertos
        closeAllEditForms(item.parentElement); 
        
        item.querySelector('.admin-cms-main').classList.add('hidden');
        item.querySelector('.admin-cms-edit-form').classList.remove('hidden');
    }
    
    function hideCmsEditForm(e) {
        const item = e.target.closest('.admin-cms-card');

        // --- INÍCIO DA MODIFICAÇÃO (Req 3: Cancelar "Adicionar") ---
        const autoKey = item.dataset.key;
        if (autoKey && globalCmsAutomations[autoKey]?.isNew) {
            delete globalCmsAutomations[autoKey];
            renderAdminAutomations(); // Re-renderiza para remover
            return;
        }

        const dashIndex = item.dataset.index;
        if (dashIndex) {
            const { systemKey, areaKey } = item.dataset;
             if (globalCmsDashboards[systemKey]?.areas[areaKey]?.items[dashIndex]?.isNew) {
                globalCmsDashboards[systemKey].areas[areaKey].items.splice(dashIndex, 1);
                renderAdminDashboards(); // Re-renderiza para remover
                return;
            }
        }
        // --- FIM DA MODIFICAÇÃO ---

        // Comportamento normal (se não for um item novo)
        item.querySelector('.admin-cms-main').classList.remove('hidden');
        item.querySelector('.admin-cms-edit-form').classList.add('hidden');
    }
    
    // Função para salvar os dados globais no backend
    function saveCmsData(type, feedbackElement) {
        let endpoint = '';
        let payload = {};
        
        if (type === 'dashboards') {
            endpoint = '/api/admin/save-dashboards';

            // Reconstrói o payload na ordem correta para salvar o JSON
            payload = {
                "looker": globalCmsDashboards.looker,
                "tableau": globalCmsDashboards.tableau,
                "library": globalCmsDashboards.library
            };
            Object.keys(payload).forEach(key => {
                if (payload[key] === undefined) {
                    delete payload[key];
                }
            });
            
            // --- INÍCIO DA MODIFICAÇÃO (Persistir Ordem) ---
            if (currentHubUser) { // Só salva se o usuário estiver logado
                try {
                    sessionStorage.setItem(`sortedDashboards_${currentHubUser}`, JSON.stringify(payload));
                } catch (e) {
                    console.error("Falha ao salvar dashboards no sessionStorage", e);
                }
            }
            // --- FIM DA MODIFICAÇÃO ---

        } else if (type === 'automations') {
            endpoint = '/api/admin/save-automations';
            
            // (Req 2: Forçar Ordem de Save)
            payload = {}; // Começa com um objeto vazio
            const automationKeys = Object.keys(globalCmsAutomations);
            automationKeys.forEach(key => {
                payload[key] = globalCmsAutomations[key];
            });
            
            // --- INÍCIO DA MODIFICAÇÃO (Persistir Ordem) ---
            if (currentHubUser) { // Só salva se o usuário estiver logado
                try {
                    sessionStorage.setItem(`sortedAutomations_${currentHubUser}`, JSON.stringify(payload));
                } catch (e) {
                    console.error("Falha ao salvar automações no sessionStorage", e);
                }
            }
            // --- FIM DA MODIFICAÇÃO ---

        } else {
            return;
        }
        
        fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
        .then(res => res.json())
        .then(data => {
            if (data.status !== 'sucesso') {
                alert(`Falha ao salvar: ${data.mensagem}`);
            }
        })
        .catch(err => {
            alert(`Erro de rede ao salvar: ${err}`);
        });
    }

});