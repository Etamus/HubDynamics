// --- SCRIPT UNIFICADO E COMPLETO ---
document.addEventListener('DOMContentLoaded', () => {
    // --- Seletores dos elementos ---
    const selectionScreen = document.getElementById('selection-screen');
    const dashboardView = document.getElementById('dashboard-view');
    const radios = document.querySelectorAll('input[name="dashboard_system"]');
    const backButton = document.getElementById('back-btn');
    let isViewingDashboard = false;
    let isSplitMode = false;
    let isSplitView = false;
    let splitFirstUrl = null;
    let splitFirstMaxWidth = null;
    const splitBtn = document.getElementById('split-btn');
    const allViewButtons = document.querySelectorAll('.view-button');
    const lookerOptions = document.getElementById('looker-options');
    const tableauOptions = document.getElementById('tableau-options');
    const libraryOptions = document.getElementById('library-options');
    const allAreaContainers = document.querySelectorAll('.area-selection-container');
    const allAreaButtons = document.querySelectorAll('.area-button');
    const allDashboardGroups = document.querySelectorAll('.dashboard-options[id^="dashboards-"]');
    const allBackToAreasButtons = document.querySelectorAll('.back-to-areas-button');
    const previewPanel = document.getElementById('preview-panel');
    const previewImage = document.getElementById('preview-image');
    const previewDescription = document.getElementById('preview-description');
    const previewTagsContainer = document.getElementById('preview-tags-container');
    const pageSearchContainer = document.querySelector('.page-search-container');
    const translate = (key, fallback, vars = {}) => {
        const base = (window.hubI18n && typeof hubI18n.t === 'function') ? hubI18n.t(key, fallback) : (fallback || key);
        return base.replace(/\{(\w+)\}/g, (match, token) => (token in vars ? vars[token] : match));
    };
    // --- ADICIONAR: Seletores da Paginação Looker ---
    const lookerPaginationControls = document.getElementById('looker-pagination');
    const lookerPrevPage = document.getElementById('looker-prev-page');
    const lookerNextPage = document.getElementById('looker-next-page');
    const lookerPageInfo = document.getElementById('looker-page-info');
    const lookerAreaButtons = document.querySelectorAll('#looker-area-selection .area-button');

    // --- (Req 1) INÍCIO: Seletores e Variáveis do Modal de Login ---
    
    const LOOKER_PAGE_SIZE = 4;
    let lookerCurrentPage = 1;
    const lookerTotalPages = Math.ceil(lookerAreaButtons.length / LOOKER_PAGE_SIZE);
    // --- FIM DA ADIÇÃO ---

    // =================================================================
    // ===== INÍCIO: NOVAS FUNÇÕES PARA O "ACESSO RÁPIDO" =====
    // =================================================================
    
    /**
     * Atualiza o localStorage com o item recém-clicado.
     * Mantém apenas os 4 itens mais recentes.
     */
    function updateLocalStorage(id, name, icon, hubArea) {
        if (!id || !name || !icon) return;

        // --- LÓGICA DE USUÁRIO (Req 1) ---
        // Lê o usuário salvo pelo hub.js ou usa '_guest'
        const username = localStorage.getItem('hubUsername') || '_guest';
        const storageKey = `recentDashboards_${username}`;
        // --------------------------------

        let recents = JSON.parse(localStorage.getItem(storageKey)) || [];
        
        const newItem = { id, name, icon, hub_area: hubArea || 'Spare Parts' };

        // Remove o item se ele já existir...
        recents = recents.filter(item => item.id !== id);
        
        // Adiciona o novo item no início...
        recents.unshift(newItem);

        // --- CORREÇÃO APLICADA AQUI ---
        // Lê a contagem salva do localStorage (padrão 4)
        const savedCount = parseInt(localStorage.getItem('hubItemCount') || 4);
        
        // Limita a lista com base na contagem salva
        recents = recents.slice(0, savedCount); 
        // --- FIM DA CORREÇÃO ---
        
        localStorage.setItem(storageKey, JSON.stringify(recents));
    }

/**
     * Pega o botão clicado, extrai seus dados e salva no localStorage.
     */
    function saveToRecents(button) {
        try {
            const name = button.textContent.trim();
            const id = button.dataset.id;
            
            // Lógica para determinar o ícone (MAIS PRECISA)
            let icon = 'fas fa-chart-pie'; // Ícone padrão (Looker/Tableau)
            const parentSection = button.closest('#looker-options, #tableau-options, #library-options');
            
            if (parentSection && parentSection.id === 'library-options') {
                const tags = (button.dataset.previewTags || '').toLowerCase();
                
                // Mapeia ícones com base nas tags de preview
                if (id === 'caminhos_bq' || tags.includes('database')) {
                    icon = 'fas fa-database';
                } else if (id === 'jira_ongoing' || tags.includes('tracking')) {
                    icon = 'fas fa-tasks';
                } else if (id === 'sgi_spareparts') {
                    icon = 'fas fa-clipboard-list';
                } else if (id === 'folheto_devolucao' || tags.includes('consultation') || tags.includes('presentation')) {
                    icon = 'fas fa-book-open';
                } else {
                    icon = 'fas fa-file-alt'; // Ícone genérico de biblioteca
                }
            }
            
            // (1) Lógica de Recência (para Acesso Rápido)
            updateLocalStorage(id, name, icon, button.dataset.hubArea);

            // --- INÍCIO DA MODIFICAÇÃO (Req 1: Frequência) ---
            // (2) Lógica de Frequência (para "Mais Acessados")
            // (Usamos o nome de usuário do localStorage, que o hub.js já salvou)
            const username = localStorage.getItem('hubUsername') || '_guest';
            const countStorageKey = `dashboardAccessCounts_${username}`;
            
            let counts = JSON.parse(localStorage.getItem(countStorageKey)) || {};
            // Incrementa a contagem para este ID (ou define como 1)
            counts[id] = (counts[id] || 0) + 1;
            
            localStorage.setItem(countStorageKey, JSON.stringify(counts));
            // --- FIM DA MODIFICAÇÃO ---
        
        } catch (e) {
            console.error("Falha ao salvar item recente/contagem:", e);
        }
    }

    /**
     * Verifica se a URL tem um parâmetro "open" e clica no botão correspondente.
     */
    function checkForAutoOpen() {
        const urlParams = new URLSearchParams(window.location.search);
        const dashboardToOpen = urlParams.get('open');
        
        if (dashboardToOpen) {
            const button = document.querySelector(`.view-button[data-id="${dashboardToOpen}"]`);
            if (button) {
                // Remove o parâmetro da URL para evitar reabertura no refresh
                history.replaceState(null, '', window.location.pathname);
                
                // Simula a navegação manual
                const system = button.closest('#looker-options, #tableau-options, #library-options');
                if (system) {
                    // 1. Clica no rádio do sistema correto
                    const radioValue = system.id.replace('-options', ''); // 'looker', 'tableau', 'library'
                    document.querySelector(`input[name="dashboard_system"][value="${radioValue}"]`).click();
                    
                    // 2. Clica no botão de área (se aplicável)
                    const dashboardGroup = button.closest('.dashboard-options[id^="dashboards-"]');
                    if (dashboardGroup) {
                        const areaId = dashboardGroup.id.replace('dashboards-', '');
                        const areaButton = document.querySelector(`.area-button[data-area="${areaId}"]`);
                        if (areaButton) {
                            areaButton.click();
                        }
                    }
                }
                
                // 3. Clica no botão final do dashboard
                button.click();
            }
        }
    }
    
    
    // (Helper) Refatoração da lógica que cria o Iframe
    function showDashboard(button) {
        if (!button) return;
        
        // --- INÍCIO DA MODIFICAÇÃO (Req 1: Forçar Paisagem) ---
        // 1. Verifica se estamos em um dispositivo móvel (baseado na largura da tela)
        const isMobile = window.matchMedia("(max-width: 768px)").matches;

        if (isMobile) {
            // 2. Tenta entrar em tela cheia (obrigatório para o lock)
            const elem = document.documentElement; // Pega a raiz <html>
            if (elem.requestFullscreen) {
                elem.requestFullscreen().then(() => {
                    // 3. Tenta travar a orientação
                    if (screen.orientation && screen.orientation.lock) {
                        screen.orientation.lock('landscape').catch(err => {
                            // Aviso (pode falhar se o usuário não permitir)
                            console.warn("Aviso: Não foi possível travar a orientação:", err.message);
                        });
                    } else {
                        console.warn("Aviso: API screen.orientation.lock() não suportada (ex: Safari/iOS).");
                    }
                }).catch(err => {
                    console.warn("Aviso: Não foi possível entrar em tela cheia:", err.message);
                });
            }
        }
        // --- FIM DA MODIFICAÇÃO ---

        // (Lógica original de clique movida para cá)
        saveToRecents(button); // Salva no Acesso Rápido

        const dashboardUrl = button.getAttribute('data-url');
        const customMaxWidth = button.dataset.width;

        const fab = document.getElementById('feedback-fab');
        if (fab) fab.classList.add('hidden');

        if (isSplitMode) {
            // --- Entra no modo split view: mostra dois iframes lado a lado ---
            isSplitMode = false;
            isSplitView = true;
            selectionScreen.classList.remove('split-picking');
            const splitBanner = document.getElementById('split-pick-banner');
            if (splitBanner) splitBanner.remove();

            dashboardView.innerHTML = '';
            dashboardView.classList.add('split-view');

            const iframe1 = document.createElement('iframe');
            iframe1.src = splitFirstUrl;
            iframe1.className = 'dashboard-iframe';
            iframe1.setAttribute('allowfullscreen', '');
            if (splitFirstMaxWidth) iframe1.style.maxWidth = splitFirstMaxWidth;

            const iframe2 = document.createElement('iframe');
            iframe2.src = dashboardUrl;
            iframe2.className = 'dashboard-iframe';
            iframe2.setAttribute('allowfullscreen', '');
            if (customMaxWidth) iframe2.style.maxWidth = customMaxWidth;

            dashboardView.appendChild(iframe1);
            dashboardView.appendChild(iframe2);
            dashboardView.style.display = 'flex';
            selectionScreen.style.display = 'none';
        } else {
            // --- Modo normal: um único iframe ---
            isSplitView = false;
            dashboardView.classList.remove('split-view');
            dashboardView.innerHTML = '';
            const iframe = document.createElement('iframe');
            iframe.src = dashboardUrl;
            iframe.className = 'dashboard-iframe';
            iframe.setAttribute('allowfullscreen', '');
            if (customMaxWidth) iframe.style.maxWidth = customMaxWidth;
            dashboardView.appendChild(iframe);
            selectionScreen.style.display = 'none';
            dashboardView.style.display = 'flex';
        }

        isViewingDashboard = true;
        document.body.classList.add('dashboard-view-active');
        if (pageSearchContainer) pageSearchContainer.classList.add('hidden');
        updateBackButton();
        updateSplitBtn();
    }
    // --- FIM: Funções de Login ---

    // =================================================================
    // ===== FIM: NOVAS FUNÇÕES PARA O "ACESSO RÁPIDO" =====
    // =================================================================

    // =================================================================
    // ===== INÍCIO: NOVAS FUNÇÕES DA PAGINAÇÃO LOOKER =====
    // =================================================================

    function updateLookerPagination() {
        if (!lookerPaginationControls) return; // Segurança
        
        // Atualiza o texto
        lookerPageInfo.textContent = translate('pagination.pageInfo', `Página ${lookerCurrentPage} de ${lookerTotalPages}`, {
            current: lookerCurrentPage,
            total: lookerTotalPages
        });

        // Habilita/Desabilita botões
        lookerPrevPage.disabled = (lookerCurrentPage === 1);
        lookerNextPage.disabled = (lookerCurrentPage === lookerTotalPages);

        // Calcula os índices
        const startIndex = (lookerCurrentPage - 1) * LOOKER_PAGE_SIZE;
        const endIndex = startIndex + LOOKER_PAGE_SIZE;

        // Mostra/Esconde os botões de área
        lookerAreaButtons.forEach((button, index) => {
            if (index >= startIndex && index < endIndex) {
                button.style.display = 'block';
            } else {
                button.style.display = 'none';
            }
        });
    }

    // Adiciona Listeners aos botões de paginação
    if (lookerNextPage) {
        lookerNextPage.addEventListener('click', () => {
            if (lookerCurrentPage < lookerTotalPages) {
                lookerCurrentPage++;
                updateLookerPagination();
            }
        });
    }
    
    if (lookerPrevPage) {
        lookerPrevPage.addEventListener('click', () => {
            if (lookerCurrentPage > 1) {
                lookerCurrentPage--;
                updateLookerPagination();
            }
        });
    }

        const dashboardsSearchInput = document.getElementById('dashboards-search-input');
    if (dashboardsSearchInput) {
    function handleDashboardSearch() {
    const searchInput = document.getElementById('dashboards-search-input');
    const searchTerm = searchInput.value.toLowerCase();
    
    // 1. --- Lógica de Reset Limpa (Voltar ao Estado Inicial) ---
    if (searchTerm === '') {
        // Seleciona TODOS os botões de área e view em TODOS os sistemas
        const allSystemButtons = document.querySelectorAll('.area-button, .view-button');
        
        // Remove a propriedade de display inline que o filtro adicionou (display: none/block)
        allSystemButtons.forEach(button => {
            button.style.removeProperty('display');
        });

        // Restaura o estado paginado (hiding buttons > 4)
        updateLookerPagination(); 
        
        return; 
    }
    // --- FIM DA LÓGICA DE RESET ---

    // 2. --- Lógica de Filtragem (Se há termo) ---
    // Seleciona TODOS os botões que estão em containers visíveis/ativos
    const activeButtons = document.querySelectorAll('.dashboard-options:not(.hidden) .area-button, .dashboard-options:not(.hidden) .view-button');

    activeButtons.forEach(button => {
        const name = button.textContent.toLowerCase();

        // Determina o display correto (view buttons são inline-block, area buttons são block)
        const displayType = button.classList.contains('view-button') ? 'inline-block' : 'block';

        if (name.includes(searchTerm)) {
            // Aplica a regra de exibição
            button.style.display = displayType; 
        } else {
            // Esconde
            button.style.display = 'none'; 
        }
    });
}

    dashboardsSearchInput.addEventListener('keyup', handleDashboardSearch);
}
    
    // =================================================================
    // ===== FIM: NOVAS FUNÇÕES DA PAGINAÇÃO LOOKER =====
    // =================================================================

    // --- LÓGICA DE EVENTOS (Modificada) ---

    function resetViews() {
        allDashboardGroups.forEach(group => group.classList.add('hidden'));
        allAreaContainers.forEach(container => container.classList.remove('hidden'));
    }

    function handleRadioChange() {
        resetViews();
        const selectedSystem = document.querySelector('input[name="dashboard_system"]:checked').value;
        
        // Esconde tudo
        if (lookerOptions) { lookerOptions.classList.add('hidden'); }
        if (tableauOptions) { tableauOptions.classList.add('hidden'); }
        if (libraryOptions) { libraryOptions.classList.add('hidden'); }
        if (lookerPaginationControls) { lookerPaginationControls.classList.add('hidden'); }

        // Mostra o selecionado
        if (selectedSystem === 'looker') {
            lookerOptions.classList.remove('hidden');
            if (lookerPaginationControls) { lookerPaginationControls.classList.remove('hidden'); }
        } else if (selectedSystem === 'tableau') {
            tableauOptions.classList.remove('hidden');
        } else if (selectedSystem === 'library') {
            if (libraryOptions) { libraryOptions.classList.remove('hidden'); }
        }
    }
    radios.forEach(radio => radio.addEventListener('change', handleRadioChange));

    allViewButtons.forEach(button => {
        
        // Lógica do clique (para abrir o dashboard)
        button.addEventListener('click', () => {
            showDashboard(button);
        }); 

        // Lógica do mouseover (para mostrar o preview com etiquetas)
        button.addEventListener('mouseover', () => {
            // 1. Verifica se estamos em um dispositivo móvel
            const isMobile = window.matchMedia("(max-width: 768px)").matches;
            
            // 2. Se for mobile, NÃO execute a função de preview.
            if (isMobile) {
                return; 
            }
            const gifPath = button.dataset.previewGif;
            const text = button.dataset.previewText;
            const tagsString = button.dataset.previewTags;

            if (gifPath && text) {
                previewImage.src = gifPath;
                previewDescription.textContent = text;
                previewTagsContainer.innerHTML = '';

                if (tagsString) {
                    const tagsArray = tagsString.split(',');
                    const knownNonKpiTags = [
                        'daily', 'weekly', 'monthly', 'gcp', 'sheets', 'drive', 'consultation',
                        'dashboard', 'database', 'revenue', 'efficiency', 'accuracy',
                        'management', 'performance', 'planning', 'sla', 'tracking',
                        'safety', 'costs', 'data'
                    ];

                    tagsArray.forEach(tagText => {
                        const cleanTagText = tagText.trim();
                        const tagElement = document.createElement('span');
                        tagElement.className = 'preview-tag';
                        tagElement.textContent = cleanTagText;

                        const lowerCaseTag = cleanTagText.toLowerCase();

                        if (['consultation', 'dashboard', 'database'].includes(lowerCaseTag)) {
                            tagElement.classList.add('tag-orange');
                        } else if (['daily', 'weekly', 'monthly'].includes(lowerCaseTag)) {
                            tagElement.classList.add('tag-green');
                        } else if (['gcp', 'gardem', 'sheets', 'drive', 'presentation'].includes(lowerCaseTag)) {
                            tagElement.classList.add('tag-blue');
                        } else if (knownNonKpiTags.includes(lowerCaseTag)) {
                            tagElement.classList.add('tag-gray');
                        } else {
                            tagElement.classList.add('tag-purple'); // KPI
                        }

                        previewTagsContainer.appendChild(tagElement);
                    });
                }

                previewPanel.classList.add('visible');
            }
        }); 

        // Lógica do mouseout (para esconder o preview)
        button.addEventListener('mouseout', () => {
            previewPanel.classList.remove('visible');
        });
    }); // Fim do 'allViewButtons.forEach'

    
    function updateBackButton() {
            if (isViewingDashboard) {
                // Estado: VENDO DASHBOARD (Botão "Voltar")
                // --- INÍCIO DA MODIFICAÇÃO (Req 1) ---
                backButton.innerHTML = '<i class="fas fa-arrow-left"></i>'; // Seta para voltar
                // --- FIM DA MODIFICAÇÃO ---
                backButton.href = '#'; 
                backButton.title = 'Voltar'; 
            } else {
                // Estado: TELA DE SELEÇÃO (Botão "Voltar ao Hub")
                // --- INÍCIO DA MODIFICAÇÃO (Req 1) ---
                backButton.innerHTML = '<i class="fas fa-home"></i>'; // Casa para ir ao Hub
                // --- FIM DA MODIFICAÇÃO ---
                backButton.href = '/'; 
                backButton.title = 'Voltar ao Hub';
                document.body.classList.remove('dashboard-view-active');
                
                // (Lógica para sair da tela cheia/paisagem permanece a mesma)
                if (document.fullscreenElement && document.exitFullscreen) {
                    document.exitFullscreen().catch(err => {
                        console.warn("Falha ao sair da tela cheia:", err.message);
                    });
                }
                if (screen.orientation && screen.orientation.unlock) {
                    try {
                        screen.orientation.unlock();
                    } catch(e) {
                        // Ignora erros
                    }
                }
            }
        }

    function updateSplitBtn() {
        if (!splitBtn) return;
        // Esconde em: tela de seleção, modo split view ativo
        if (!isViewingDashboard || isSplitView) {
            splitBtn.classList.add('hidden');
            splitBtn.querySelector('i').className = 'fas fa-table-columns';
            splitBtn.classList.remove('split-btn-active');
            return;
        }
        splitBtn.classList.remove('hidden');
        if (isSplitMode) {
            splitBtn.querySelector('i').className = 'fas fa-xmark';
            splitBtn.title = 'Cancelar Split';
            splitBtn.classList.add('split-btn-active');
        } else {
            splitBtn.querySelector('i').className = 'fas fa-table-columns';
            splitBtn.title = 'Split Screen';
            splitBtn.classList.remove('split-btn-active');
        }
    }

    function restoreSingleIframe(url, maxWidth) {
        isSplitMode = false;
        selectionScreen.classList.remove('split-picking');
        const splitBanner = document.getElementById('split-pick-banner');
        if (splitBanner) splitBanner.remove();
        dashboardView.classList.remove('split-view');
        dashboardView.innerHTML = '';
        const iframe = document.createElement('iframe');
        iframe.src = url;
        iframe.className = 'dashboard-iframe';
        iframe.setAttribute('allowfullscreen', '');
        if (maxWidth) iframe.style.maxWidth = maxWidth;
        dashboardView.appendChild(iframe);
        dashboardView.style.display = 'flex';
        selectionScreen.style.display = 'none';
        isViewingDashboard = true;
        updateBackButton();
        updateSplitBtn();
    }

    if (splitBtn) {
        splitBtn.addEventListener('click', () => {
            if (isSplitMode) {
                // Cancela: restaura o iframe original
                restoreSingleIframe(splitFirstUrl, splitFirstMaxWidth);
                return;
            }
            // Inicia split pick: salva iframe atual e mostra seleção como overlay
            const currentIframe = dashboardView.querySelector('.dashboard-iframe:first-child');
            if (!currentIframe) return;
            splitFirstUrl = currentIframe.src;
            splitFirstMaxWidth = currentIframe.style.maxWidth || null;
            isSplitMode = true;

            // Banner informativo no topo da tela de seleção
            if (!document.getElementById('split-pick-banner')) {
                const banner = document.createElement('div');
                banner.id = 'split-pick-banner';
                banner.innerHTML = '<i class="fas fa-table-columns"></i> Escolha o segundo dashboard';
                selectionScreen.insertAdjacentElement('afterbegin', banner);
            }

            dashboardView.style.display = 'none';
            selectionScreen.classList.add('split-picking');
            selectionScreen.style.display = 'flex';
            isViewingDashboard = true;
            updateBackButton();
            updateSplitBtn();
        });
    }

    backButton.addEventListener('click', (event) => {
        if (isSplitMode) {
            // Volta ao dashboard único sem entrar na tela de seleção
            event.preventDefault();
            restoreSingleIframe(splitFirstUrl, splitFirstMaxWidth);
            return;
        }
        if (isViewingDashboard) {
            event.preventDefault();
            isSplitView = false;
            dashboardView.classList.remove('split-view');
            dashboardView.innerHTML = '';
            dashboardView.style.display = 'none';
            selectionScreen.style.display = 'flex';
            isViewingDashboard = false;
            document.body.classList.remove('dashboard-view-active');
            if (pageSearchContainer) {
                pageSearchContainer.classList.remove('hidden');
            }
            updateBackButton();
            updateSplitBtn();
            try { 
                document.getElementById('feedback-fab').classList.remove('hidden');
            } catch (e) {
                // ignora se o fab não for encontrado
            }
        }
    });

    allAreaButtons.forEach(button => {
        button.addEventListener('click', () => {
            const area = button.dataset.area;
            const targetDashboardGroup = document.getElementById(`dashboards-${area}`);
            const parentAreaContainer = button.closest('.area-selection-container');

            // --- INÍCIO DA CORREÇÃO ---
            // Verifica se o botão clicado está dentro das opções do Looker
            if (button.closest('#looker-options')) {
                if (lookerPaginationControls) { // Garante que o elemento existe
                    lookerPaginationControls.classList.add('hidden');
                }
            }
            // --- FIM DA CORREÇÃO ---
            
            // --- ADICIONAR ESTAS 2 LINHAS ---
            if (parentAreaContainer && parentAreaContainer.id === 'area-selection') {
                lookerPaginationControls.classList.add('hidden');
            }
            // --- FIM DA ADIÇÃO ---
            
            if (parentAreaContainer) { parentAreaContainer.classList.add('hidden'); }
            if (targetDashboardGroup) { targetDashboardGroup.classList.remove('hidden'); }
        });
    });

    allBackToAreasButtons.forEach(button => {
        button.addEventListener('click', () => {
            const parentDashboardGroup = button.parentElement;
            const mainContainer = button.closest('#looker-options, #tableau-options, #library-options');
            parentDashboardGroup.classList.add('hidden');

            if (mainContainer && mainContainer.id === 'looker-options') {
                lookerPaginationControls.classList.remove('hidden');
            }

            if (mainContainer) {
                const areaContainer = mainContainer.querySelector('.area-selection-container');
                if (areaContainer) areaContainer.classList.remove('hidden');
            }
        });
    });

    // --- Script do Chatbot (Original, sem alteração) ---
    // (Omitido por brevidade, mas o seu 'shared.js' vai carregar isso)
    
    // --- CHAMADA DAS NOVAS FUNÇÕES ---
    handleRadioChange(); // Inicia o estado dos rádios
    checkForAutoOpen();  // Verifica se veio do Hub com um link
    updateLookerPagination(); // --- ADICIONAR ESTA LINHA ---

    // --- FIM DA MODIFICAÇÃO ---

}); // <-- Este é o fim do DOMContentLoaded

    const pageKey = window.location.pathname.replace('/', '').split('?')[0]; // Extrai "automacao", "dashboards", ou "drive"
    const currentTab = document.querySelector(`.main-nav-tabs .nav-tab[data-page="${pageKey}"]`);
    if (currentTab) {
        currentTab.classList.add('active');
    }

    const dashboardsSearchInput = document.getElementById('dashboards-search-input');
    if (dashboardsSearchInput) {
        function handleDashboardSearch() {
    const searchInput = document.getElementById('dashboards-search-input');
    const searchTerm = searchInput.value.toLowerCase();
    
    // --- NOVO RESET DE PESQUISA ---
    if (searchTerm === '') {
        // 1. Seleciona TODOS os botões de Área (genérico) e View
        const allAreaButtonsGen = document.querySelectorAll('.area-selection-container .area-button');
        const allViewButtons = document.querySelectorAll('.view-button');
        
        // 2. Limpa o display style de TODOS os botões de área genéricos
        allAreaButtonsGen.forEach(button => { 
            // Garante que TODOS os botões de área (Looker, Tableau, etc)
            // tenham seu estilo inline removido para voltarem ao padrão CSS.
            button.style.display = ''; 
        });
        
        // 3. Limpa o display style de TODOS os botões de view
        allViewButtons.forEach(button => { 
            button.style.display = ''; 
        });

        // 4. CHAVE: Reativa a lógica de paginação *apenas* para os botões Looker.
        // Isso re-esconde os botões Looker que excedem o limite da página atual (4).
        updateLookerPagination(); 
        
        return; 
    }
    // --- FIM DA MODIFICAÇÃO ---

    // Filtra todos os botões que estão visíveis no momento (Áreas ou Dashboards)
    const activeButtons = document.querySelectorAll('.dashboard-options:not(.hidden) .area-button, .dashboard-options:not(.hidden) .view-button');

    activeButtons.forEach(button => {
        const name = button.textContent.toLowerCase();

        // Determina o display correto (view buttons são inline-block, area buttons são block)
        const displayType = button.classList.contains('view-button') ? 'inline-block' : 'block';

        if (name.includes(searchTerm)) {
            button.style.display = displayType; 
        } else {
            button.style.display = 'none'; 
        }
    });
}

        dashboardsSearchInput.addEventListener('keyup', handleDashboardSearch);
    }