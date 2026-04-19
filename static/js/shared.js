const HUB_SUPPORTED_LANGUAGES = ['pt', 'en'];

// ---------------------------------------------------------------------------
// A4: Interceptor global de fetch — adiciona X-CSRF-Token em todas as
// requisições POST/PUT/DELETE para proteção contra CSRF.
// ---------------------------------------------------------------------------
(function patchFetchCSRF() {
    const _origFetch = window.fetch;
    window.fetch = function patchedFetch(url, options) {
        options = options || {};
        const method = (options.method || 'GET').toUpperCase();
        if (['POST', 'PUT', 'DELETE', 'PATCH'].includes(method)) {
            const meta = document.querySelector('meta[name="csrf-token"]');
            if (meta && meta.content) {
                options.headers = Object.assign({}, options.headers, {
                    'X-CSRF-Token': meta.content
                });
            }
        }
        return _origFetch.apply(this, [url, options]);
    };
})();

// ---------------------------------------------------------------------------
// A3: Sanitizador HTML simples — permite apenas tags seguras do chatbot.
// Previne XSS na renderização de respostas do LLM.
// ---------------------------------------------------------------------------
function hubSanitizeHTML(html) {
    const ALLOWED_TAGS = /^(strong|em|b|i|code|br|p|span)$/i;
    const tmp = document.createElement('div');
    tmp.innerHTML = html;
    (function clean(node) {
        Array.from(node.childNodes).forEach(child => {
            if (child.nodeType === Node.ELEMENT_NODE) {
                if (!ALLOWED_TAGS.test(child.tagName)) {
                    // Substitui tag não permitida pelo seu conteúdo de texto
                    const text = document.createTextNode(child.textContent);
                    node.replaceChild(text, child);
                } else {
                    // Remove todos os atributos da tag permitida
                    Array.from(child.attributes).forEach(attr => child.removeAttribute(attr.name));
                    clean(child);
                }
            }
        });
    })(tmp);
    return tmp.innerHTML;
}

const HUB_I18N_ENTRIES = {
    'app.title': { pt: 'Spare Parts', en: 'Spare Parts' },
    'header.subtitle': { pt: 'Selecione a ferramenta que deseja utilizar', en: 'Select the tool you want to use' },
    'search.placeholder': { pt: 'Pesquisar ferramenta...', en: 'Search for a tool...' },
    'search.label': { pt: 'Pesquisar ferramenta', en: 'Search tool' },
    'button.about': { pt: 'Sobre', en: 'About' },
    'button.settings': { pt: 'Configurações', en: 'Settings' },
    'button.fullscreen': { pt: 'Tela Cheia', en: 'Fullscreen' },
    'button.access': { pt: 'Acesso', en: 'Access' },
    'button.logout': { pt: 'Deslogar', en: 'Sign out' },
    'aria.automations': { pt: 'Automações. Execute robôs para tarefas de SAP, BW e extração de relatórios.', en: 'Automations. Run bots for SAP, BW, and report extraction tasks.' },
    'aria.dashboards': { pt: 'Dashboards. Acesse o portal com os dashboards do Looker Studio e Tableau.', en: 'Dashboards. Access the portal with Looker Studio and Tableau dashboards.' },
    'aria.drive': { pt: 'Drive Online. Navegue pelas pastas da rede sem necessidade de uso da VPN.', en: 'Drive Online. Browse the network folders without needing the VPN.' },
    'card.automation.title': { pt: 'Automações', en: 'Automations' },
    'card.automation.description': { pt: 'Execute robôs para tarefas de SAP, BW e extração de relatórios.', en: 'Run bots for SAP, BW, and report extraction tasks.' },
    'card.requestAccess': { pt: 'Solicite para ter acesso.', en: 'Request access.' },
    'card.loginAccess': { pt: 'Faça login para ter acesso.', en: 'Log in to gain access.' },
    'card.dashboards.title': { pt: 'Dashboards', en: 'Dashboards' },
    'card.dashboards.description': { pt: 'Acesse o portal com os dashboards do Looker Studio e Tableau.', en: 'Access the portal with Looker Studio and Tableau dashboards.' },
    'card.drive.title': { pt: 'Drive Online', en: 'Drive Online' },
    'card.drive.description': { pt: 'Navegue pelas pastas da rede sem necessidade de uso da VPN.', en: 'Browse the network folders without needing the VPN.' },
    'quickLinks.title': { pt: 'Acesso Rápido', en: 'Quick Access' },
    'modal.settings.title': { pt: 'Configurações', en: 'Settings' },
    'settings.menuTitle': { pt: 'Menu Principal', en: 'Main Menu' },
    'settings.appearance': { pt: 'Aparência', en: 'Appearance' },
    'settings.theme.light': { pt: 'Claro', en: 'Light' },
    'settings.theme.dark': { pt: 'Escuro', en: 'Dark' },
    'settings.theme.system': { pt: 'Sistema', en: 'System' },
    'settings.theme.lightTitle': { pt: 'Modo Claro', en: 'Light Mode' },
    'settings.theme.darkTitle': { pt: 'Modo Escuro', en: 'Dark Mode' },
    'settings.theme.systemTitle': { pt: 'Padrão do Sistema', en: 'System Default' },
    'settings.cards.label': { pt: 'Quantidade de Cards', en: 'Card Quantity' },
    'settings.cards.option4': { pt: '4 Espaços', en: '4 Slots' },
    'settings.cards.option6': { pt: '6 Espaços', en: '6 Slots' },
    'settings.cards.option8': { pt: '8 Espaços', en: '8 Slots' },
    'settings.cards.clear': { pt: 'Limpar Cards', en: 'Clear Cards' },
    'settings.clearCardsConfirm': { pt: 'Tem certeza que deseja limpar todos os cards do Acesso Rápido?', en: 'Are you sure you want to clear all Quick Access cards?' },
    'modal.about.title': { pt: 'Sobre', en: 'About' },
    'about.version': { pt: 'Versão Atual:', en: 'Current Version:' },
    'about.owners': { pt: 'Responsáveis pelo Projeto:', en: 'Project Owners:' },
    'about.changelog': { pt: 'Histórico de Versões', en: 'Version History' },
    'modal.profile.title': { pt: 'Minha Conta', en: 'My Account' },
    'profile.tab.profile': { pt: 'Perfil', en: 'Profile' },
    'profile.tab.security': { pt: 'Segurança', en: 'Security' },
    'profile.tab.activity': { pt: 'Atividade', en: 'Activity' },
    'profile.tab.settings': { pt: 'Configurações', en: 'Settings' },
    'profile.displayNameLabel': { pt: 'Nome de Exibição', en: 'Display Name' },
    'profile.loginLabel': { pt: 'Login de Funcionário', en: 'Employee Login' },
    'profile.permissions.label': { pt: 'Permissões', en: 'Permissions' },
    'profile.permissions.description': { pt: 'Permitir acesso a dados de terceiros.', en: 'Allow access to third-party data.' },
    'profile.dropdown.changePhoto': { pt: 'Alterar Foto', en: 'Change Photo' },
    'profile.dropdown.removePhoto': { pt: 'Remover Foto', en: 'Remove Photo' },
    'actions.chooseImage': { pt: 'Escolher Imagem', en: 'Choose Image' },
    'profile.security.heading': { pt: 'Alterar Senha', en: 'Change Password' },
    'profile.password.current': { pt: 'Senha Atual:', en: 'Current Password:' },
    'profile.password.new': { pt: 'Nova Senha:', en: 'New Password:' },
    'profile.password.confirm': { pt: 'Confirmar Nova Senha:', en: 'Confirm New Password:' },
    'profile.activity.heading': { pt: 'Últimos Agendamentos', en: 'Latest Schedules' },
    'profile.activity.loading': { pt: 'Carregando...', en: 'Loading...' },
    'common.loading': { pt: 'Carregando...', en: 'Loading...' },
    'profile.language.label': { pt: 'Idioma', en: 'Language' },
    'profile.language.option.pt': { pt: 'Português', en: 'Portuguese' },
    'profile.language.option.en': { pt: 'Inglês', en: 'English' },
    'profile.language.statusApplying': { pt: 'Aplicando idioma...', en: 'Applying language...' },
    'profile.language.statusApplied': { pt: 'Idioma aplicado. Salve para manter.', en: 'Language applied. Save to keep it.' },
    'profile.language.statusSaved': { pt: 'Preferências de idioma salvas.', en: 'Language preferences saved.' },
    'profile.language.statusError': { pt: 'Não foi possível aplicar o idioma.', en: 'Unable to apply the language.' },
    'profile.image.invalidType': { pt: 'Apenas arquivos PNG ou JPG são permitidos.', en: 'Only PNG or JPG files are allowed.' },
    'profile.image.removeCommunicationError': { pt: 'Erro de comunicação ao remover imagem.', en: 'Communication error while removing the image.' },
    'actions.saveChanges': { pt: 'Salvar Alterações', en: 'Save Changes' },
    'actions.discard': { pt: 'Descartar', en: 'Discard' },
    'actions.savePassword': { pt: 'Salvar Senha', en: 'Save Password' },
    'actions.close': { pt: 'Fechar', en: 'Close' },
    'actions.clearCards': { pt: 'Limpar Cards', en: 'Clear Cards' },
    'actions.removeImage': { pt: 'Remover Imagem', en: 'Remove Image' },
    'actions.login': { pt: 'Entrar', en: 'Sign in' },
    'actions.consult': { pt: 'Consultar', en: 'Check' },
    'actions.save': { pt: 'Salvar', en: 'Save' },
    'actions.showPassword': { pt: 'Mostrar senha', en: 'Show password' },
    'actions.hidePassword': { pt: 'Ocultar senha', en: 'Hide password' },
    'form.selectPlaceholder': { pt: 'Selecione...', en: 'Select...' },
    'modal.access.title': { pt: 'Acesso ao Hub', en: 'Hub Access' },
    'form.userLabel': { pt: 'Usuário:', en: 'User:' },
    'form.passwordLabel': { pt: 'Senha:', en: 'Password:' },
    'modal.connections.title': { pt: 'Minhas Conexões', en: 'My Connections' },
    'profile.preview.alt': { pt: 'Prévia do Perfil', en: 'Profile Preview' },
    'profile.avatar.alt': { pt: 'Foto de Perfil', en: 'Profile Photo' },
    'alt.preview.automations': { pt: 'Preview Automações', en: 'Automations Preview' },
    'alt.preview.dashboards': { pt: 'Preview Dashboards', en: 'Dashboards Preview' },
    'alt.preview.drive': { pt: 'Preview Drive', en: 'Drive Preview' },
    'modal.editName.title': { pt: 'Alterar Nome', en: 'Change Name' },
    'modal.register.title': { pt: 'Solicitar Acesso', en: 'Request Access' },
    'tabs.register.form': { pt: 'Preenchimento', en: 'Application' },
    'tabs.register.consult': { pt: 'Consulta', en: 'Status' },
    'register.loginLabel': { pt: 'Login de Funcionário:', en: 'Employee Login:' },
    'register.areaLabel': { pt: 'Área:', en: 'Area:' },
    'register.roleLabel': { pt: 'Função:', en: 'Role:' },
    'register.button.submit': { pt: 'Solicitar', en: 'Request' },
    'register.success': { pt: 'Sua solicitação foi enviada!', en: 'Your request has been sent!' },
    'register.keepToken': { pt: 'Guarde este token para consultar seu status:', en: 'Keep this token to check your status:' },
    'consult.tokenPrompt': { pt: 'Insira seu Token de Acesso:', en: 'Enter your Access Token:' },
    'consult.statusTitle': { pt: 'Status da Solicitação:', en: 'Request Status:' },
    'consult.newPasswordLabel': { pt: 'Defina sua nova senha:', en: 'Set your new password:' },
    'admin.tab.requests': { pt: 'Solicitações', en: 'Requests' },
    'admin.tab.users': { pt: 'Usuários', en: 'Users' },
    'admin.tab.dashboards': { pt: 'Dashboards', en: 'Dashboards' },
    'admin.tab.automations': { pt: 'Automações', en: 'Automations' },
    'admin.active.description': { pt: 'Acompanhe pedidos pendentes e responda com rapidez.', en: 'Track pending requests and respond quickly.' },
    'actions.new': { pt: 'Novo', en: 'New' },
    'profile.displayName.tooLong': { pt: 'O Nome de Exibição não pode ter mais de 16 caracteres.', en: 'The display name cannot exceed 16 characters.' },
    'profile.displayName.saved': { pt: 'Nome de exibição salvo.', en: 'Display name saved.' },
    'profile.language.selectLabel': { pt: 'Selecione o idioma de exibição.', en: 'Select the display language.' },
    'profile.dropdown.title': { pt: 'Alterar foto', en: 'Change photo' },
    'chatbot.placeholder': { pt: 'Digite o que precisa...', en: 'Type what you need...' },
    'chatbot.greeting': { pt: 'Olá! Sou o Hub Assistant. Como posso ajudar?', en: "Hi! I'm the Hub Assistant. How can I help?" },
    'chatbot.typing': { pt: 'Digitando...', en: 'Typing...' },
    'chatbot.connectionError': { pt: 'Não foi possível conectar ao assistente. Verifique sua conexão.', en: 'Could not connect to the assistant. Please check your connection.' },
    'profile.permissions.toggle': { pt: 'Permitir acesso a dados de terceiros.', en: 'Allow access to third-party data.' }
    ,'nav.restrictedExecutor': { pt: 'Acesso restrito a Executores', en: 'Access restricted to Executors' }
    ,'nav.loginRequiredDrive': { pt: 'Faça login para acessar o Drive', en: 'Log in to access Drive' }
    ,'button.scheduler': { pt: 'Agendador', en: 'Scheduler' }
    ,'button.backToHub': { pt: 'Voltar ao Hub', en: 'Back to Hub' }
    ,'search.automation.placeholder': { pt: 'Buscar automação...', en: 'Search automation...' }
    ,'subtitle.automation': { pt: 'Selecione o sistema para executar uma tarefa', en: 'Select the system to run a task' }
    ,'modal.subtitle.login': { pt: 'Insira suas credenciais para logar.', en: 'Enter your credentials to sign in.' }
    ,'button.login': { pt: 'Logar', en: 'Sign in' }
    ,'button.addConnection': { pt: 'Adicionar Conexão', en: 'Add Connection' }
    ,'confirm.logout.title': { pt: 'Deseja deslogar?', en: 'Do you want to sign out?' }
    ,'button.no': { pt: 'Não', en: 'No' }
    ,'button.yes': { pt: 'Sim', en: 'Yes' }
    ,'button.download.inactive': { pt: 'Arquivo não encontrado.', en: 'File not found.' }
    ,'search.dashboard.placeholder': { pt: 'Buscar dashboard...', en: 'Search dashboard...' }
    ,'subtitle.dashboards': { pt: 'Selecione a plataforma para visualizar um relatório', en: 'Select the platform to view a report' }
    ,'button.backToAreas': { pt: 'Voltar para Áreas', en: 'Back to Areas' }
    ,'pagination.prev': { pt: 'Anterior', en: 'Previous' }
    ,'pagination.next': { pt: 'Próxima', en: 'Next' }
    ,'pagination.pageInfo': { pt: 'Página {current} de {total}', en: 'Page {current} of {total}' }
    ,'subtitle.drive': { pt: 'Navegue pelas pastas da rede', en: 'Browse the network folders' }
    ,'label.path': { pt: 'Caminho:', en: 'Path:' }
    ,'table.header.name': { pt: 'Nome', en: 'Name' }
    ,'table.header.modified': { pt: 'Modificado em', en: 'Modified on' }
    ,'table.header.size': { pt: 'Tamanho', en: 'Size' }
    ,'search.drive.placeholder': { pt: 'Buscar nesta pasta...', en: 'Search this folder...' }
    ,'about.devlog.v44': { pt: 'Lançamento do novo design de janelas do Hub', en: 'Launch of the new Hub window design' }
    ,'about.devlog.v42': { pt: 'Layout alterado por completo, melhorias de perfil e correções', en: 'Layout completely revamped with profile improvements and fixes' }
    ,'about.devlog.v409': { pt: 'Melhoria de layout e nova seleção de tela nas páginas', en: 'Layout improvements and new screen selection across the pages' }
    ,'about.devlog.v408': { pt: 'Melhorias no Drive Online e alterações nos ícones das páginas', en: 'Drive Online improvements and updated icons across pages' }
    ,'about.devlog.v406': { pt: 'Novo perfil de usuário e correção de "alert" da janela de registro', en: 'New user profile and fix for the registration window alert' }
    ,'about.devlog.v403': { pt: 'Correção de icones e melhoria do modo escuro', en: 'Icon fixes and dark mode improvements' }
    ,'about.devlog.v402': { pt: 'Adição de novas lógicas de "@media" para atualização do modo mobile', en: 'Added new @media logic to refresh the mobile mode' }
    ,'about.devlog.v309': { pt: 'Adicionado login para Tableau no Dashboards e correção da quebra de linha do Chatbot', en: 'Added Tableau login inside Dashboards and fixed the chatbot line break' }
    ,'about.devlog.v307': { pt: 'Barra de pesquisa aprimorada e novo preview na página de automações', en: 'Improved search bar and new preview on the automations page' }
    ,'about.devlog.v304': { pt: 'Atualização da lógica e prompt do Chatbot', en: 'Chatbot logic and prompt update' }
    ,'about.devlog.v303': { pt: 'Melhoria no desbloqueio de usuário e informações do Drive Online', en: 'Improvements to user unlocking and Drive Online information' }
    ,'about.devlog.v301': { pt: 'Adicionado botão de criação de usuário na tela de administração', en: 'Added user creation button on the administration screen' }
    ,'about.devlog.v30': { pt: 'Janela de administração aprimorada e correções de problemas', en: 'Enhanced admin window and bug fixes' }
    ,'about.devlog.v292': { pt: 'Melhoria de layout da página incial e lógica da barra de pesquisa', en: 'Homepage layout improvements and search bar logic updates' }
    ,'about.devlog.v29': { pt: 'Novo Chatbot com integração de inteligência artificial', en: 'New chatbot with artificial intelligence integration' }
    ,'about.devlog.v26': { pt: 'Melhorias no Agendador de Tarefas', en: 'Task scheduler improvements' }
    ,'about.devlog.v24': { pt: 'Novo layout, funcionalidades e correções gerais', en: 'New layout, features, and general fixes' }
    ,'about.devlog.v20': { pt: 'Melhorias no registro, tela de administração e lógica dos perfis', en: 'Registration, admin screen, and profile logic improvements' }
    ,'about.devlog.v19': { pt: 'Novo registro de usuário, consulta e sistema de perfis', en: 'New user registration, query, and profile system' }
    ,'about.devlog.v184': { pt: 'Mudanças de placeholders e ajustes de icones no agendamento de tarefas', en: 'Placeholder changes and icon tweaks in the task scheduler' }
    ,'about.devlog.v182': { pt: 'Melhorias no perfil de usuário do hub', en: 'Hub user profile improvements' }
    ,'about.devlog.v18': { pt: 'Agendador de tarefas aprimorado e correções de estrutura', en: 'Task scheduler enhancements and structure fixes' }
    ,'about.devlog.v17': { pt: 'Acesso de usuário, conexões e melhorias na página de automatizações', en: 'User access, connections, and automations page improvements' }
    ,'about.devlog.v15': { pt: 'Qualidade de vida, correções de instabilidade e melhorias nas páginas', en: 'Quality-of-life updates, stability fixes, and page improvements' }
    ,'about.devlog.v1114': { pt: 'Adicionado Barra de Pesquisa na Página Inicial', en: 'Added a search bar to the home page' }
    ,'about.devlog.v1112': { pt: 'Adição de apresentações "Processo de devolução" ao Library', en: 'Added “Return process” presentations to the Library' }
    ,'about.devlog.v1111': { pt: 'Adição de arquivos "google apresentações" ao Library. Adição do label data "Gardem".', en: 'Added Google Slides files to the Library and the “Gardem” data label.' }
    ,'about.devlog.v1110': { pt: 'Início da preparação para port mobile.', en: 'Started preparing the mobile port.' }
    ,'about.devlog.v11': { pt: 'Adição de label referenciando a qual indicador o dashboard atende.', en: 'Added a label showing which indicator each dashboard serves.' }
    ,'about.devlog.v109': { pt: 'Adição de preview aos dashboards pendentes', en: 'Added previews to pending dashboards' }
    ,'about.devlog.v107': { pt: 'Correção: Oculta o chatbot ao visualizar dashboards do Looker para evitar sobreposição', en: 'Fix: Hide the chatbot when viewing Looker dashboards to avoid overlap' }
    ,'about.devlog.v106': { pt: 'Correção: Adiciona sistema de paginação e preview latera com etiquetas', en: 'Fix: Added pagination and a side preview with labels' }
    ,'about.devlog.v105': { pt: 'Correção do servidor_unico.py', en: 'Fix for servidor_unico.py' }
    ,'about.devlog.v102': { pt: 'Remoção de Arquivos Desnecessários', en: 'Removed unnecessary files' }
    ,'about.devlog.v101': { pt: 'Atualizações Gerais e Novos Aprimoramentos', en: 'General updates and new enhancements' }
    ,'about.devlog.v10': { pt: 'Atualização Inicial', en: 'Initial update' }
};

(function initializeHubI18n() {
    const ATTRIBUTE_CONFIG = [
        { attr: 'placeholder', datasetKey: 'i18nPlaceholderKey' },
        { attr: 'title', datasetKey: 'i18nTitleKey' },
        { attr: 'aria-label', datasetKey: 'i18nAriaLabelKey' },
        { attr: 'alt', datasetKey: 'i18nAltKey' }
    ];
    const reverseIndex = {};
    const textNodeKeyCache = new WeakMap();
    let currentLanguage = detectInitialLanguage();
    let observer = null;
    let domReady = document.readyState !== 'loading';
    let isTranslating = false;

    HUB_SUPPORTED_LANGUAGES.forEach(lang => {
        reverseIndex[lang] = {};
    });

    Object.entries(HUB_I18N_ENTRIES).forEach(([key, value]) => {
        HUB_SUPPORTED_LANGUAGES.forEach(lang => {
            const phrase = value[lang];
            if (phrase) {
                reverseIndex[lang][normalizeText(phrase)] = key;
            }
        });
    });

    setDocumentLanguage(currentLanguage);

    if (!domReady) {
        document.addEventListener('DOMContentLoaded', () => {
            domReady = true;
            applyLanguageToDocument();
            startObserver();
        });
    } else {
        applyLanguageToDocument();
        startObserver();
    }

    patchDialogs();

    function normalizeText(value) {
        return (value || '').replace(/\s+/g, ' ').trim();
    }

    function detectInitialLanguage() {
        try {
            // M1: hubUsername migrado para sessionStorage
            const storedUser = sessionStorage.getItem('hubUsername') || '_default';
            return localStorage.getItem(`hubLanguage_${storedUser}`) || localStorage.getItem('hubLanguage_default') || document.documentElement.dataset.hubLang || 'pt';
        } catch (err) {
            return 'pt';
        }
    }

    function saveLanguage(lang) {
        try {
            // M1: hubUsername migrado para sessionStorage
            const storedUser = sessionStorage.getItem('hubUsername') || '_default';
            localStorage.setItem(`hubLanguage_${storedUser}`, lang);
            localStorage.setItem('hubLanguage_default', lang);
        } catch (err) {
            console.error('Não foi possível salvar o idioma.', err);
        }
    }

    function setDocumentLanguage(lang) {
        document.documentElement.setAttribute('lang', lang === 'en' ? 'en' : 'pt-br');
        document.documentElement.dataset.hubLang = lang;
        document.body && (document.body.dataset.hubLanguage = lang);
    }

    function getTranslation(key, lang) {
        const entry = HUB_I18N_ENTRIES[key];
        if (!entry) return null;
        return entry[lang] || entry['pt'] || null;
    }

    function findKeyFromValue(value) {
        const normalized = normalizeText(value);
        if (!normalized) return null;
        for (const lang of HUB_SUPPORTED_LANGUAGES) {
            if (reverseIndex[lang][normalized]) {
                return reverseIndex[lang][normalized];
            }
        }
        return null;
    }

    function translateTextNode(node, lang) {
        if (!node || !node.parentElement) return;
        if (node.parentElement.closest('[data-i18n-ignore="true"]')) return;
        const parentTag = node.parentElement.tagName;
        if (parentTag === 'SCRIPT' || parentTag === 'STYLE') return;
        let key = textNodeKeyCache.get(node);
        if (!key) {
            key = findKeyFromValue(node.textContent);
            if (key) {
                textNodeKeyCache.set(node, key);
            }
        }
        if (!key) return;
        const translated = getTranslation(key, lang);
        if (translated && node.textContent !== translated) {
            node.textContent = translated;
        }
    }

    function translateAttributes(root, lang) {
        if (!root || root.nodeType !== Node.ELEMENT_NODE) return;
        const elements = root.querySelectorAll('*');
        elements.forEach(el => {
            if (el.closest('[data-i18n-ignore="true"]')) return;
            if (el.tagName === 'SCRIPT' || el.tagName === 'STYLE') return;
            ATTRIBUTE_CONFIG.forEach(({ attr, datasetKey }) => {
                let key = el.dataset[datasetKey];
                if (!key) {
                    const attrValue = attr === 'placeholder' ? el.getAttribute(attr) : el.getAttribute(attr);
                    if (!attrValue) return;
                    key = findKeyFromValue(attrValue);
                    if (!key) return;
                    el.dataset[datasetKey] = key;
                }
                const translated = getTranslation(key, lang);
                if (translated) {
                    el.setAttribute(attr, translated);
                }
            });
        });
    }

    function translateTextNodes(root, lang) {
        if (!root) return;
        const walker = document.createTreeWalker(
            root,
            NodeFilter.SHOW_TEXT,
            {
                acceptNode(node) {
                    if (!node || !node.textContent || !node.textContent.trim()) return NodeFilter.FILTER_REJECT;
                    if (!node.parentElement) return NodeFilter.FILTER_REJECT;
                    if (node.parentElement.closest('[data-i18n-ignore="true"]')) return NodeFilter.FILTER_REJECT;
                    const tag = node.parentElement.tagName;
                    if (tag === 'SCRIPT' || tag === 'STYLE') return NodeFilter.FILTER_REJECT;
                    return NodeFilter.FILTER_ACCEPT;
                }
            }
        );
        let current = walker.nextNode();
        while (current) {
            translateTextNode(current, lang);
            current = walker.nextNode();
        }
    }

    function translateTree(root, lang) {
        if (!root) return;
        if (root.nodeType === Node.TEXT_NODE) {
            translateTextNode(root, lang);
            return;
        }
        translateTextNodes(root, lang);
        translateAttributes(root, lang);
    }

    function applyLanguageToDocument(options = {}) {
        if (!document.body) return Promise.resolve(currentLanguage);
        isTranslating = true;
        document.body.classList.add('language-updating');
        return new Promise(resolve => {
            requestAnimationFrame(() => {
                translateTree(document.body, currentLanguage);
                isTranslating = false;
                document.body.classList.remove('language-updating');
                if (!options.silent) {
                    document.dispatchEvent(new CustomEvent('hub:language-changed', { detail: { language: currentLanguage } }));
                }
                resolve(currentLanguage);
            });
        });
    }

    function startObserver() {
        if (observer || !document.body) return;
        observer = new MutationObserver((mutations) => {
            if (isTranslating) return;
            mutations.forEach(mutation => {
                mutation.addedNodes.forEach(node => {
                    if (node.nodeType === Node.TEXT_NODE) {
                        translateTextNode(node, currentLanguage);
                    } else if (node.nodeType === Node.ELEMENT_NODE) {
                        translateTree(node, currentLanguage);
                    }
                });
            });
        });
        observer.observe(document.body, { childList: true, subtree: true });
    }

    function setLanguage(lang, options = {}) {
        const targetLang = HUB_SUPPORTED_LANGUAGES.includes(lang) ? lang : 'pt';
        currentLanguage = targetLang;
        setDocumentLanguage(targetLang);
        if (options.persist !== false) {
            saveLanguage(targetLang);
        }
        if (domReady && document.body) {
            return applyLanguageToDocument({ silent: options.silent }).then(() => targetLang);
        }
        return Promise.resolve(targetLang);
    }

    function getLanguage() {
        return currentLanguage;
    }

    function translateText(value, lang = currentLanguage) {
        if (!value) return value;
        const key = findKeyFromValue(value);
        if (!key) return value;
        return getTranslation(key, lang) || value;
    }

    function t(key, fallback) {
        const translated = getTranslation(key, currentLanguage);
        if (translated) return translated;
        return fallback || HUB_I18N_ENTRIES[key]?.pt || key;
    }

    function patchDialogs() {
        if (typeof window === 'undefined') return;
        ['alert', 'confirm', 'prompt'].forEach(method => {
            const original = window[method];
            if (!original) return;
            window[method] = function patchedDialog(message, ...rest) {
                const finalMessage = typeof message === 'string' ? translateText(message) : message;
                return original.call(window, finalMessage, ...rest);
            };
        });
    }

    window.hubI18n = {
        getLanguage,
        setLanguage,
        t,
        translateText,
        applyLanguage: () => domReady ? applyLanguageToDocument() : Promise.resolve(currentLanguage),
    };
})();

// --- 1. LÓGICA DE TEMA GLOBAL (DEFINIÇÃO) ---
// Esta função agora está no escopo global, acessível por outros scripts.
function applyGlobalTheme(theme) {
    if (theme === 'dark') {
        document.body.classList.add('theme-dark');
    } else if (theme === 'light') {
        document.body.classList.remove('theme-dark');
    } else { // 'system'
        if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
            document.body.classList.add('theme-dark');
        } else {
            document.body.classList.remove('theme-dark');
        }
    }
}

document.addEventListener('DOMContentLoaded', () => {
    let chatHistory = []; // (Req 1) Armazena o histórico da conversa
    
    // Listener para mudança automática de tema do sistema (se o usuário selecionou 'system')
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', e => {
        const currentTheme = localStorage.getItem('hubTheme') || 'system';
        if (currentTheme === 'system') {
            applyGlobalTheme('system');
        }
    });
    // --- FIM DA LÓGICA DE TEMA ---

    // --- ADICIONAR ESTE BLOCO: LÓGICA DE TELA CHEIA GLOBAL ---
    const fullscreenBtn = document.getElementById('fullscreen-btn');
    
    if (fullscreenBtn) {
        const fullscreenIcon = fullscreenBtn.querySelector('i');

        function toggleFullscreen() {
            if (!document.fullscreenElement) {
                document.documentElement.requestFullscreen().catch(err => {
                    console.error(`Erro ao tentar entrar em tela cheia: ${err.message}`);
                });
            } else {
                if (document.exitFullscreen) {
                    document.exitFullscreen();
                }
            }
        }

        // Listener para o clique no botão
        fullscreenBtn.addEventListener('click', toggleFullscreen);

        // Listener para atualizar o ícone (quando o usuário usa ESC, por exemplo)
        document.addEventListener('fullscreenchange', () => {
            if (document.fullscreenElement) {
                // Estamos em tela cheia
                fullscreenIcon.classList.remove('fa-expand');
                fullscreenIcon.classList.add('fa-compress');
                fullscreenBtn.title = "Sair da Tela Cheia";
                fullscreenBtn.setAttribute('aria-label', "Sair da Tela Cheia");
            } else {
                // Não estamos em tela cheia
                fullscreenIcon.classList.remove('fa-compress');
                fullscreenIcon.classList.add('fa-expand');
                fullscreenBtn.title = "Tela Cheia";
                fullscreenBtn.setAttribute('aria-label', "Tela Cheia");
            }
        });
    }
    // --- FIM DA LÓGICA DE TELA CHEIA ---

    // --- 2. INJEÇÃO DINÂMICA DO HTML DO CHATBOT ---
    function injectChatbotHTML() {
        const chatbotContainer = document.createElement('div');
        chatbotContainer.id = 'chatbot-wrapper';
        
        chatbotContainer.innerHTML = `
            <div class="fab" id="feedback-fab" title="Feedback e Demandas">
                <i class="fas fa-comment-dots"></i>
            </div>
            <div class="chat-popup" id="feedback-popup">
                <div class="chat-header">
                    <h2 id="modal-title">Hub Assistant</h2>
                    <button class="close-button" id="modal-close-btn">&times;</button>
                </div>
                <div class="chat-messages" id="chat-messages"></div>
                <div class="iframe-container" id="iframe-container">
                    <iframe src="about:blank" frameborder="0"></iframe>
                </div>
                <div class="chat-input-area" id="chat-input-area">
                    <input type="text" id="chat-input" class="chat-input" placeholder="Digite o que precisa...">
                    <button class="send-button" id="send-btn"><i class="fas fa-paper-plane"></i></button>
                </div>
            </div>
        `;
        document.body.appendChild(chatbotContainer);
    }
    
    injectChatbotHTML();
    // --- FIM DA INJEÇÃO DO HTML ---


    // --- 3. LÓGICA DO CHATBOT (ATUALIZADA COM GEMINI) ---
    const fab = document.getElementById('feedback-fab');
    const popup = document.getElementById('feedback-popup');
    if (!fab || !popup) return;

    const closeBtn = document.getElementById('modal-close-btn');
    const chatMessages = document.getElementById('chat-messages');
    const iframeContainer = document.getElementById('iframe-container');
    const iframe = iframeContainer.querySelector('iframe');
    const modalTitle = document.getElementById('modal-title');
    const chatInputArea = document.getElementById('chat-input-area');
    const chatInput = document.getElementById('chat-input');
    const sendBtn = document.getElementById('send-btn');

    const forms = {
        demanda: { title: "Solicitação de Demanda", url: "https://docs.google.com/forms/d/e/1FAIpQLSdBvCg6jU3XjXn-dFLfwRZU-fj80fMbAT1vv6J6hg9yUIH1Jg/viewform?embedded=true" },
        sugestao: { title: "Sugestões de Melhoria", url: "https://docs.google.com/forms/d/e/1FAIpQLScIp_mkk0kMZuJgjchiq5O2fHGTkPSjXYpsi4G5Xw2e297C6w/viewform?embedded=true" }
    };

    // NOVO: Função para converter Markdown básico (negrito) para HTML
    function parseMarkdown(text) {
        // Converte **negrito** para <strong>negrito</strong>
        // Adiciona .replace() para outras formatações se necessário (ex: *itálico*)
        return text.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
    }

    // (A variável 'keywords' foi removida, pois a IA decide)

    const addMessage = (text, type, containsHtml = false, extraClass = '') => {
        const messageDiv = document.createElement('div');
        messageDiv.className = `chat-message ${type} ${extraClass}`;
        if (containsHtml) {
            // A3: Sanitiza HTML do LLM antes de inserir no DOM
            messageDiv.innerHTML = hubSanitizeHTML(text);
        } else {
            messageDiv.textContent = text;
        }
        chatMessages.appendChild(messageDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;

        // --- INÍCIO DA MODIFICAÇÃO (Req 1) ---
        // Não adiciona o indicador "Digitando..." ao histórico
        if (type !== 'bot-message' || !extraClass.includes('typing')) {
            // Converte 'user-reply' para 'user' e 'bot-message' para 'model'
            const role = (type === 'user-reply') ? 'user' : 'model';
            
            // Adiciona ao histórico (o backend lidará com a remoção do HTML se houver)
            chatHistory.push({ role: role, text: text });
        }
        // --- FIM DA MODIFICAÇÃO ---

        return messageDiv; // Retorna o elemento criado
    };

    // Mostra o iframe do Google Forms
    const showForm = (formType) => {
        const formData = forms[formType];
        modalTitle.textContent = formData.title;
        iframe.src = formData.url;
        chatMessages.style.display = 'none';
        chatInputArea.style.display = 'none';
        iframeContainer.style.display = 'block';
    };

    // (A função 'presentFormOption' foi removida)
    
    // Reseta o chat ao fechar
    const resetChat = () => {
        popup.classList.remove('visible');
        setTimeout(() => {
            chatMessages.innerHTML = ''; // Limpa mensagens
            chatMessages.style.display = 'flex';
            chatInputArea.style.display = 'flex';
            chatInput.disabled = false;
            sendBtn.disabled = false;
            iframeContainer.style.display = 'none';
            iframe.src = 'about:blank';
            modalTitle.textContent = 'Hub Assistant';
            
            // --- INÍCIO DA MODIFICAÇÃO (Req 1) ---
            chatHistory = []; // Limpa o histórico
            // --- FIM DA MODIFICAÇÃO ---
        }, 300);
    };
    
    // Inicia a conversa
    const startConversation = () => {
        // --- INÍCIO DA MODIFICAÇÃO (Req 1) ---
        // Garante que o histórico está limpo e adiciona a primeira msg do bot
        chatHistory = []; 
        addMessage("Olá! Sou o Hub Assistant. Como posso ajudar?", 'bot-message');
        // --- FIM DA MODIFICAÇÃO ---
    };

    const toggleChat = () => {
        const isVisible = popup.classList.contains('visible');
        if (isVisible) {
            resetChat();
        } else {
            popup.classList.add('visible');
            if (chatMessages.children.length === 0) {
                startConversation();
            }
        }
    };
    
    // Remove o indicador "Digitando..."
    function removeTypingIndicator() {
        const typingIndicator = chatMessages.querySelector('.bot-message.typing');
        if (typingIndicator) {
            typingIndicator.remove();
        }
    }

    // Processa a resposta do backend
    const processBackendResponse = (data) => {
        removeTypingIndicator();

        if (data.status === 'sucesso') {
            
            const formattedText = parseMarkdown(data.text);
            // (Esta chamada agora também adiciona ao histórico)
            addMessage(formattedText, 'bot-message', true); 
            
            if (data.form_trigger) {
                const formType = data.form_trigger; 
                const buttonText = forms[formType] ? forms[formType].title : "Abrir Formulário"; 

                setTimeout(() => {
                    const buttonHtml = `<button class="chat-action-button" data-form-type="${formType}">${buttonText}</button>`;
                    const buttonBubble = addMessage(buttonHtml, 'bot-message', true, 'button-bubble');
                    
                    const button = buttonBubble.querySelector('.chat-action-button');
                    if (button) {
                        button.addEventListener('click', () => {
                            chatInput.disabled = true;
                            sendBtn.disabled = true;
                            showForm(formType);
                        });
                    }
                }, 800); 
            }
        } else {
            addMessage(data.text || "Ocorreu um erro. Tente novamente.", 'bot-message', false, 'error');
        }
    };

    // Manipula o envio do usuário
    const handleUserInput = () => {
        const userInput = chatInput.value.trim();
        if (!userInput) return;

        addMessage(userInput, 'user-reply'); // (Isso adiciona ao histórico)
        chatInput.value = '';
        chatInput.disabled = true;
        sendBtn.disabled = true;

        addMessage("Digitando...", 'bot-message', false, 'typing');

        // --- INÍCIO DA MODIFICAÇÃO (Req 1) ---
        // Pega as últimas 10 mensagens (incluindo a que o usuário acabou de digitar)
        const historyToSend = chatHistory.slice(-10);
        
        // Envia para o backend (Gemini)
        fetch('/api/chatbot/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            // Envia o histórico em vez da mensagem única
            body: JSON.stringify({ history: historyToSend }) 
        })
        // --- FIM DA MODIFICAÇÃO ---
        .then(response => response.json())
        .then(data => {
            processBackendResponse(data);
        })
        .catch(err => {
            console.error("Erro no fetch do chatbot:", err);
            removeTypingIndicator();
            addMessage("Não foi possível conectar ao assistente. Verifique sua conexão.", 'bot-message', false, 'error');
        })
        .finally(() => {
            chatInput.disabled = false;
            sendBtn.disabled = false;
            chatInput.focus();
        });
    };

    // Listeners (sem alteração)
    fab.addEventListener('click', toggleChat);
    closeBtn.addEventListener('click', resetChat);
    sendBtn.addEventListener('click', handleUserInput);
    chatInput.addEventListener('keypress', (event) => {
        if (event.key === 'Enter') {
            handleUserInput();
        }
    });
});

// --- Atualiza rótulo de área nas subpáginas ---
(function() {
    const labels = document.querySelectorAll('.hub-area-label-text');
    if (!labels.length) return;
    try {
        // M1: hubUsername migrado para sessionStorage
    const username = sessionStorage.getItem('hubUsername');
        if (!username) {
            labels.forEach(el => { el.textContent = 'Hub Dynamics'; });
            return;
        }
        // Prioriza a área definida pelo backend (data-active-area no body)
        const serverArea = document.body.dataset.activeArea;
        if (serverArea) {
            sessionStorage.setItem('activeHubArea_' + username, serverArea);
        }
        const activeArea = serverArea || sessionStorage.getItem('activeHubArea_' + username) || 'Spare Parts';
        labels.forEach(el => { el.textContent = 'Hub ' + activeArea; });
    } catch (e) { /* ignora */ }
})();