// ===========================================
// ARB Terminal - Frontend Application
// ===========================================

(function() {
    'use strict';

    // ===========================================
    // State
    // ===========================================

    const state = {
        pairs: [],
        exchanges: [],
        balances: {},
        settings: {},
        stats: {},
        ws: null,
        wsReconnectAttempts: 0,
        wsMaxReconnectAttempts: 10,
        wsReconnectDelay: 1000,
    };

    // ===========================================
    // DOM Elements
    // ===========================================

    const elements = {
        // Tabs
        tabs: document.querySelectorAll('.tab'),
        tabContents: document.querySelectorAll('.tab-content'),

        // Header
        balancesBar: document.getElementById('balances-bar'),
        connectionStatus: document.getElementById('connection-status'),

        // Bot tab
        pairsGrid: document.getElementById('pairs-grid'),
        pairsCount: document.getElementById('pairs-count'),
        activeCount: document.getElementById('active-count'),
        btnAddPair: document.getElementById('btn-add-pair'),
        emptyPairs: document.getElementById('empty-pairs'),

        // Exchanges
        exchangesGrid: document.getElementById('exchanges-grid'),

        // Stats
        statTrades: document.getElementById('stat-trades'),
        statPnl: document.getElementById('stat-pnl'),
        statSl: document.getElementById('stat-sl'),
        statLiq: document.getElementById('stat-liq'),
        topProfitable: document.getElementById('top-profitable'),
        topLosing: document.getElementById('top-losing'),
        topActive: document.getElementById('top-active'),
        btnResetStats: document.getElementById('btn-reset-stats'),

        // Settings
        settingMaxPositions: document.getElementById('setting-max-positions'),
        settingLeverage: document.getElementById('setting-leverage'),
        settingMaxPairs: document.getElementById('setting-max-pairs'),
        btnSaveSettings: document.getElementById('btn-save-settings'),
        infoExchanges: document.getElementById('info-exchanges'),
        infoActivePairs: document.getElementById('info-active-pairs'),
        infoWsStatus: document.getElementById('info-ws-status'),

        // Modals
        modalPair: document.getElementById('modal-pair'),
        modalExchange: document.getElementById('modal-exchange'),
        modalConfirm: document.getElementById('modal-confirm'),

        // Forms
        formPair: document.getElementById('form-pair'),
        formExchange: document.getElementById('form-exchange'),

        // Toast
        toastContainer: document.getElementById('toast-container'),
    };

    // ===========================================
    // API Functions
    // ===========================================

    const api = {
        async request(method, url, data = null, timeout = 30000) {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), timeout);

            const options = {
                method,
                headers: { 'Content-Type': 'application/json' },
                signal: controller.signal,
            };
            if (data) options.body = JSON.stringify(data);

            try {
                const response = await fetch(url, options);
                clearTimeout(timeoutId);

                if (!response.ok) {
                    const error = await response.json().catch(() => ({}));
                    throw new Error(error.detail || `HTTP ${response.status}`);
                }
                return response.json();
            } catch (error) {
                clearTimeout(timeoutId);
                if (error.name === 'AbortError') {
                    throw new Error('Request timeout');
                }
                throw error;
            }
        },

        // Exchanges
        getExchanges: () => api.request('GET', '/api/exchanges'),
        connectExchange: (name, data) => api.request('POST', `/api/exchanges/${name}/connect`, data),
        disconnectExchange: (name) => api.request('DELETE', `/api/exchanges/${name}/connect`),
        refreshBalances: () => api.request('POST', '/api/exchanges/refresh-balances'),

        // Pairs
        getPairs: () => api.request('GET', '/api/pairs'),
        createPair: (data) => api.request('POST', '/api/pairs', data),
        updatePair: (id, data) => api.request('PATCH', `/api/pairs/${id}`, data),
        deletePair: (id, force = false) => api.request('DELETE', `/api/pairs/${id}?force=${force}`),
        startPair: (id) => api.request('POST', `/api/pairs/${id}/start`),
        pausePair: (id) => api.request('POST', `/api/pairs/${id}/pause`),
        closePair: (id) => api.request('POST', `/api/pairs/${id}/close`),

        // Stats
        getStats: () => api.request('GET', '/api/stats'),
        resetStats: () => api.request('POST', '/api/stats/reset'),

        // Settings
        getSettings: () => api.request('GET', '/api/settings'),
        updateSettings: (data) => api.request('PATCH', '/api/settings', data),
    };

    // ===========================================
    // WebSocket
    // ===========================================

    function connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws/stream`;

        state.ws = new WebSocket(wsUrl);

        state.ws.onopen = () => {
            console.log('WebSocket connected');
            state.wsReconnectAttempts = 0;
            updateConnectionStatus(true);
        };

        state.ws.onclose = () => {
            console.log('WebSocket disconnected');
            updateConnectionStatus(false);
            scheduleReconnect();
        };

        state.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };

        state.ws.onmessage = (event) => {
            try {
                const msg = JSON.parse(event.data);
                handleWebSocketMessage(msg);
            } catch (e) {
                console.error('Failed to parse WS message:', e);
            }
        };

        // Ping interval
        setInterval(() => {
            if (state.ws && state.ws.readyState === WebSocket.OPEN) {
                state.ws.send(JSON.stringify({ type: 'ping' }));
            }
        }, 25000);
    }

    function scheduleReconnect() {
        if (state.wsReconnectAttempts >= state.wsMaxReconnectAttempts) {
            console.log('Max reconnect attempts reached');
            return;
        }

        const delay = state.wsReconnectDelay * Math.pow(1.5, state.wsReconnectAttempts);
        state.wsReconnectAttempts++;

        console.log(`Reconnecting in ${delay}ms (attempt ${state.wsReconnectAttempts})`);
        setTimeout(connectWebSocket, delay);
    }

    function handleWebSocketMessage(msg) {
        switch (msg.type) {
            case 'init':
                // Initial state from server
                state.pairs = msg.pairs || [];
                state.balances = msg.balances || {};
                state.settings = msg.settings || {};
                renderPairs();
                renderBalances();
                updateSettings(msg.settings);
                break;

            case 'pairUpdate':
                updatePairInState(msg.pair);
                renderPairs();
                break;

            case 'pairCreated':
                state.pairs.push(msg.pair);
                renderPairs();
                break;

            case 'pairDeleted':
                state.pairs = state.pairs.filter(p => p.id !== msg.pairId);
                renderPairs();
                break;

            case 'balanceUpdate':
                state.balances[msg.exchange] = msg.balance;
                renderBalances();
                break;

            case 'exchangeUpdate':
                loadExchanges();
                break;

            case 'statsUpdate':
                loadStats();
                break;

            case 'settingsUpdate':
                Object.assign(state.settings, msg.settings);
                updateSettingsUI();
                break;

            case 'notification':
                showNotificationToast(msg.notif);
                break;

            case 'pong':
                // Heartbeat response
                break;
        }
    }

    function updateConnectionStatus(online) {
        const dot = elements.connectionStatus.querySelector('.status-dot');
        const text = elements.connectionStatus.querySelector('.status-text');

        dot.classList.toggle('online', online);
        dot.classList.toggle('offline', !online);
        text.textContent = online ? 'Online' : 'Offline';

        elements.infoWsStatus.textContent = online ? 'Connected' : 'Disconnected';
    }

    // ===========================================
    // Rendering Functions
    // ===========================================

    function renderBalances() {
        const html = Object.entries(state.balances)
            .filter(([_, bal]) => bal !== null)
            .map(([ex, bal]) => `
                <div class="balance-item">
                    <span class="balance-exchange">${ex}</span>
                    <span class="balance-value">$${formatNumber(bal)}</span>
                </div>
            `).join('');

        elements.balancesBar.innerHTML = html || '<span class="balance-item">No balances</span>';
    }

    function renderPairs() {
        const pairs = state.pairs;

        elements.pairsCount.textContent = pairs.length;
        elements.activeCount.textContent = pairs.filter(p => p.status === 'active' || p.has_position).length;

        // Update button state
        elements.btnAddPair.disabled = pairs.length >= 30;

        if (pairs.length === 0) {
            elements.pairsGrid.innerHTML = '';
            elements.emptyPairs.style.display = 'flex';
            return;
        }

        elements.emptyPairs.style.display = 'none';

        const html = pairs.map(pair => renderPairCard(pair)).join('');
        elements.pairsGrid.innerHTML = html;

        // Attach event listeners
        attachPairCardListeners();

        // Update system info
        elements.infoActivePairs.textContent = pairs.filter(p => p.status === 'active').length;
    }

    function renderPairCard(pair) {
        const hasPosition = pair.has_position || pair.filled_parts > 0;
        const statusClass = hasPosition ? 'active' : (pair.status === 'paused' ? 'paused' : 'ready');
        const statusText = hasPosition ? 'ACTIVE' : (pair.status === 'paused' ? 'PAUSED' : 'READY');

        const pnlClass = pair.total_pnl >= 0 ? 'positive' : 'negative';
        const pnlSign = pair.total_pnl >= 0 ? '+' : '';

        let positionHtml = '';
        if (hasPosition && pair.long_exchange && pair.short_exchange) {
            positionHtml = `
                <div class="pair-position">
                    <div class="position-legs">
                        <div class="position-leg">
                            <div class="position-leg-title">
                                <span class="leg-type long">LONG</span>
                                ${pair.long_exchange.toUpperCase()}
                            </div>
                            <div>Entry: $${formatNumber(pair.avg_entry_long || 0)}</div>
                        </div>
                        <div class="position-leg">
                            <div class="position-leg-title">
                                <span class="leg-type short">SHORT</span>
                                ${pair.short_exchange.toUpperCase()}
                            </div>
                            <div>Entry: $${formatNumber(pair.avg_entry_short || 0)}</div>
                        </div>
                    </div>
                    <div class="position-pnl">
                        <span class="pnl-label">Open Volume: ${formatNumber(pair.open_volume || 0)}</span>
                        <span class="pnl-value">Parts: ${pair.filled_parts || 0}/${pair.n_orders}</span>
                    </div>
                </div>
            `;
        }

        const startPauseBtn = pair.status === 'paused'
            ? `<button class="btn btn-success btn-small" data-action="start" data-id="${pair.id}">Start</button>`
            : `<button class="btn btn-ghost btn-small" data-action="pause" data-id="${pair.id}">Pause</button>`;

        const closeBtn = hasPosition
            ? `<button class="btn btn-danger btn-small" data-action="close" data-id="${pair.id}">Close</button>`
            : '';

        return `
            <div class="pair-card status-${statusClass}" data-pair-id="${pair.id}">
                <div class="pair-header">
                    <span class="pair-symbol">${pair.symbol}</span>
                    <span class="pair-status ${statusClass}">${statusText}</span>
                </div>
                <div class="pair-params">
                    <div class="pair-param">
                        <span class="pair-param-label">Volume</span>
                        <span class="pair-param-value">${formatNumber(pair.volume)}</span>
                    </div>
                    <div class="pair-param">
                        <span class="pair-param-label">Entry</span>
                        <span class="pair-param-value">${pair.entry_spread}%</span>
                    </div>
                    <div class="pair-param">
                        <span class="pair-param-label">Exit</span>
                        <span class="pair-param-value">${pair.exit_spread}%</span>
                    </div>
                    <div class="pair-param">
                        <span class="pair-param-label">SL</span>
                        <span class="pair-param-value">${pair.stop_loss ? '$' + pair.stop_loss : '-'}</span>
                    </div>
                    <div class="pair-param">
                        <span class="pair-param-label">Orders</span>
                        <span class="pair-param-value">${pair.n_orders}</span>
                    </div>
                    <div class="pair-param">
                        <span class="pair-param-label">Leverage</span>
                        <span class="pair-param-value">${pair.leverage}x</span>
                    </div>
                </div>
                ${positionHtml}
                <div class="pair-stats">
                    <span>Trades: <span class="pair-stats-value">${pair.sl_count || 0}</span></span>
                    <span>PnL: <span class="pair-stats-value ${pnlClass}">${pnlSign}$${formatNumber(pair.total_pnl || 0)}</span></span>
                </div>
                <div class="pair-actions">
                    ${startPauseBtn}
                    <button class="btn btn-ghost btn-small" data-action="edit" data-id="${pair.id}">Edit</button>
                    ${closeBtn}
                    <button class="btn btn-ghost btn-small" data-action="delete" data-id="${pair.id}">Delete</button>
                </div>
            </div>
        `;
    }

    function attachPairCardListeners() {
        elements.pairsGrid.querySelectorAll('[data-action]').forEach(btn => {
            btn.addEventListener('click', handlePairAction);
        });
    }

    async function handlePairAction(e) {
        const action = e.target.dataset.action;
        const id = parseInt(e.target.dataset.id);
        const pair = state.pairs.find(p => p.id === id);

        if (!pair) return;

        try {
            switch (action) {
                case 'start':
                    await api.startPair(id);
                    showToast('success', `${pair.symbol} started`);
                    break;

                case 'pause':
                    await api.pausePair(id);
                    showToast('info', `${pair.symbol} paused`);
                    break;

                case 'close':
                    showConfirm(
                        'Close Position',
                        `Close position for ${pair.symbol}? This will market-close both legs.`,
                        async () => {
                            const result = await api.closePair(id);
                            if (result.success) {
                                showToast('success', `Position closed. PnL: $${formatNumber(result.pnl || 0)}`);
                            }
                        }
                    );
                    break;

                case 'edit':
                    openEditPairModal(pair);
                    break;

                case 'delete':
                    const hasPosition = pair.has_position || pair.filled_parts > 0;
                    const message = hasPosition
                        ? `Delete ${pair.symbol}? This will close the open position first.`
                        : `Delete ${pair.symbol}?`;

                    showConfirm('Delete Pair', message, async () => {
                        await api.deletePair(id, hasPosition);
                        showToast('success', `${pair.symbol} deleted`);
                    });
                    break;
            }
        } catch (error) {
            showToast('error', error.message);
        }
    }

    function renderExchanges() {
        const html = state.exchanges.map(ex => {
            const isConnected = ex.connected;
            const statusClass = isConnected ? 'connected' : '';
            const statusDot = isConnected ? 'online' : 'offline';
            const statusText = isConnected ? 'Connected' : 'Not connected';

            const balanceHtml = isConnected ? `
                <div class="exchange-balance">
                    <span class="exchange-balance-label">Balance</span>
                    <span class="exchange-balance-value">$${formatNumber(ex.balance || 0)}</span>
                </div>
            ` : '';

            const actionsHtml = isConnected ? `
                <button class="btn btn-ghost btn-small" data-action="disconnect" data-name="${ex.name}">Disconnect</button>
            ` : `
                <button class="btn btn-primary btn-small" data-action="connect" data-name="${ex.name}">Connect</button>
            `;

            return `
                <div class="exchange-card ${statusClass}">
                    <div class="exchange-header">
                        <div class="exchange-name">
                            <div class="exchange-icon">${ex.display_name.substring(0, 2)}</div>
                            <span class="exchange-title">${ex.display_name}</span>
                        </div>
                        <div class="exchange-status">
                            <span class="status-dot ${statusDot}"></span>
                            <span>${statusText}</span>
                        </div>
                    </div>
                    ${balanceHtml}
                    <div class="exchange-fee">Taker fee: ${ex.taker_fee}%</div>
                    <div class="exchange-actions">
                        ${actionsHtml}
                    </div>
                </div>
            `;
        }).join('');

        elements.exchangesGrid.innerHTML = html;

        // Attach listeners
        elements.exchangesGrid.querySelectorAll('[data-action]').forEach(btn => {
            btn.addEventListener('click', handleExchangeAction);
        });

        // Update system info
        elements.infoExchanges.textContent = state.exchanges.filter(e => e.connected).length;
    }

    async function handleExchangeAction(e) {
        const action = e.target.dataset.action;
        const name = e.target.dataset.name;

        if (action === 'connect') {
            openConnectExchangeModal(name);
        } else if (action === 'disconnect') {
            showConfirm(
                'Disconnect Exchange',
                `Disconnect ${name.toUpperCase()}?`,
                async () => {
                    try {
                        await api.disconnectExchange(name);
                        showToast('success', `${name.toUpperCase()} disconnected`);
                        await loadExchanges();
                    } catch (error) {
                        showToast('error', error.message);
                    }
                }
            );
        }
    }

    function renderStats() {
        const totals = state.stats.totals || {};

        elements.statTrades.textContent = totals.total_trades || 0;

        const pnl = totals.total_pnl || 0;
        elements.statPnl.textContent = `${pnl >= 0 ? '+' : ''}$${formatNumber(pnl)}`;
        elements.statPnl.className = `stat-value ${pnl >= 0 ? 'positive' : 'negative'}`;

        elements.statSl.textContent = totals.total_sl || 0;
        elements.statLiq.textContent = totals.total_liq || 0;

        // Top lists
        renderTopList(elements.topProfitable, state.stats.top_profitable, true);
        renderTopList(elements.topLosing, state.stats.top_losing, false);
        renderTopList(elements.topActive, state.stats.top_by_trades, null);
    }

    function renderTopList(container, items, showPnl) {
        if (!items || items.length === 0) {
            container.innerHTML = '<div class="empty-list">No data</div>';
            return;
        }

        const html = items.map(item => {
            let valueHtml;
            if (showPnl === null) {
                valueHtml = `<span class="value">${item.trades} trades</span>`;
            } else {
                const pnl = item.pnl || 0;
                const cls = pnl >= 0 ? 'positive' : 'negative';
                valueHtml = `<span class="value ${cls}">${pnl >= 0 ? '+' : ''}$${formatNumber(pnl)}</span>`;
            }

            return `
                <div class="top-list-item">
                    <span class="symbol">${item.symbol}</span>
                    ${valueHtml}
                </div>
            `;
        }).join('');

        container.innerHTML = html;
    }

    // ===========================================
    // Modal Functions
    // ===========================================

    function openModal(modal) {
        modal.classList.add('active');
        document.body.style.overflow = 'hidden';
    }

    function closeModal(modal) {
        modal.classList.remove('active');
        document.body.style.overflow = '';
    }

    function openAddPairModal() {
        document.getElementById('modal-pair-title').textContent = 'Add New Pair';
        document.getElementById('btn-submit-pair').textContent = 'Add Pair';
        document.getElementById('pair-edit-id').value = '';

        elements.formPair.reset();
        document.getElementById('pair-leverage').value = state.settings.default_leverage?.value || 10;

        openModal(elements.modalPair);
    }

    function openEditPairModal(pair) {
        document.getElementById('modal-pair-title').textContent = 'Edit Pair';
        document.getElementById('btn-submit-pair').textContent = 'Save Changes';
        document.getElementById('pair-edit-id').value = pair.id;

        document.getElementById('pair-symbol').value = pair.symbol;
        document.getElementById('pair-symbol').disabled = true;
        document.getElementById('pair-volume').value = pair.volume;
        document.getElementById('pair-orders').value = pair.n_orders;
        document.getElementById('pair-entry').value = pair.entry_spread;
        document.getElementById('pair-exit').value = pair.exit_spread;
        document.getElementById('pair-sl').value = pair.stop_loss || '';
        document.getElementById('pair-leverage').value = pair.leverage;

        openModal(elements.modalPair);
    }

    function openConnectExchangeModal(name) {
        const ex = state.exchanges.find(e => e.name === name);
        if (!ex) return;

        document.getElementById('modal-exchange-title').textContent = `Connect ${ex.display_name}`;
        document.getElementById('exchange-name').value = name;

        // Show passphrase field for OKX and Bitget
        const passphraseGroup = document.getElementById('exchange-passphrase-group');
        passphraseGroup.style.display = (name === 'okx' || name === 'bitget') ? 'block' : 'none';

        elements.formExchange.reset();
        openModal(elements.modalExchange);
    }

    let confirmCallback = null;

    function showConfirm(title, message, onConfirm) {
        document.getElementById('confirm-title').textContent = title;
        document.getElementById('confirm-message').textContent = message;
        confirmCallback = onConfirm;
        openModal(elements.modalConfirm);
    }

    // ===========================================
    // Form Handlers
    // ===========================================

    async function handlePairFormSubmit(e) {
        e.preventDefault();

        const editId = document.getElementById('pair-edit-id').value;
        const isEdit = !!editId;

        const data = {
            symbol: document.getElementById('pair-symbol').value.toUpperCase(),
            volume: parseFloat(document.getElementById('pair-volume').value),
            n_orders: parseInt(document.getElementById('pair-orders').value),
            entry_spread: parseFloat(document.getElementById('pair-entry').value),
            exit_spread: parseFloat(document.getElementById('pair-exit').value),
            stop_loss: parseFloat(document.getElementById('pair-sl').value) || null,
            leverage: parseInt(document.getElementById('pair-leverage').value),
        };

        try {
            if (isEdit) {
                delete data.symbol; // Can't change symbol
                await api.updatePair(parseInt(editId), data);
                showToast('success', 'Pair updated');
            } else {
                await api.createPair(data);
                showToast('success', `${data.symbol} added`);
            }

            closeModal(elements.modalPair);
            document.getElementById('pair-symbol').disabled = false;
        } catch (error) {
            showToast('error', error.message);
        }
    }

    async function handleExchangeFormSubmit(e) {
        e.preventDefault();

        const name = document.getElementById('exchange-name').value;
        const data = {
            api_key: document.getElementById('exchange-api-key').value,
            secret_key: document.getElementById('exchange-secret').value,
            passphrase: document.getElementById('exchange-passphrase').value || null,
        };

        try {
            const btn = e.target.querySelector('button[type="submit"]');
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner"></span> Connecting...';

            await api.connectExchange(name, data);
            showToast('success', `${name.toUpperCase()} connected`);
            closeModal(elements.modalExchange);
            await loadExchanges();
        } catch (error) {
            showToast('error', error.message);
        } finally {
            const btn = e.target.querySelector('button[type="submit"]');
            btn.disabled = false;
            btn.textContent = 'Connect';
        }
    }

    async function handleSaveSettings() {
        const data = {
            max_open_positions: parseInt(elements.settingMaxPositions.value),
            default_leverage: parseInt(elements.settingLeverage.value),
            max_monitored_pairs: parseInt(elements.settingMaxPairs.value),
        };

        try {
            await api.updateSettings(data);
            showToast('success', 'Settings saved');
        } catch (error) {
            showToast('error', error.message);
        }
    }

    // ===========================================
    // Toast Notifications
    // ===========================================

    function showToast(type, message, duration = 4000) {
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;

        const icons = {
            success: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22,4 12,14.01 9,11.01"/></svg>',
            error: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>',
            warning: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
            info: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>',
        };

        toast.innerHTML = `
            <span class="toast-icon">${icons[type] || icons.info}</span>
            <span class="toast-message">${message}</span>
            <button class="toast-close">&times;</button>
        `;

        elements.toastContainer.appendChild(toast);

        const closeBtn = toast.querySelector('.toast-close');
        closeBtn.addEventListener('click', () => removeToast(toast));

        setTimeout(() => removeToast(toast), duration);
    }

    function removeToast(toast) {
        toast.style.animation = 'toastOut 0.3s ease forwards';
        setTimeout(() => toast.remove(), 300);
    }

    function showNotificationToast(notif) {
        const typeMap = {
            'OPEN': 'success',
            'CLOSE': 'success',
            'ENTRY_OK': 'success',
            'EXIT_OK': 'success',
            'SL': 'warning',
            'LIQUIDATION': 'error',
            'ERROR': 'error',
            'ENTRY_ERROR': 'error',
        };

        const type = typeMap[notif.type] || 'info';
        showToast(type, notif.message);
    }

    // ===========================================
    // Data Loading
    // ===========================================

    async function loadExchanges() {
        try {
            state.exchanges = await api.getExchanges();
            renderExchanges();
        } catch (error) {
            console.error('Failed to load exchanges:', error);
        }
    }

    async function loadPairs() {
        try {
            state.pairs = await api.getPairs();
            renderPairs();
        } catch (error) {
            console.error('Failed to load pairs:', error);
        }
    }

    async function loadStats() {
        try {
            state.stats = await api.getStats();
            renderStats();
        } catch (error) {
            console.error('Failed to load stats:', error);
        }
    }

    async function loadSettings() {
        try {
            state.settings = await api.getSettings();
            updateSettingsUI();
        } catch (error) {
            console.error('Failed to load settings:', error);
        }
    }

    function updateSettings(settings) {
        state.settings = settings;
        updateSettingsUI();
    }

    function updateSettingsUI() {
        const s = state.settings;
        if (s.max_open_positions) {
            elements.settingMaxPositions.value = s.max_open_positions.value;
        }
        if (s.default_leverage) {
            elements.settingLeverage.value = s.default_leverage.value;
        }
        if (s.max_monitored_pairs) {
            elements.settingMaxPairs.value = s.max_monitored_pairs.value;
        }
    }

    function updatePairInState(updatedPair) {
        const index = state.pairs.findIndex(p => p.id === updatedPair.id);
        if (index !== -1) {
            state.pairs[index] = { ...state.pairs[index], ...updatedPair };
        }
    }

    // ===========================================
    // Utility Functions
    // ===========================================

    function formatNumber(num, decimals = 2) {
        if (num === null || num === undefined) return '0';
        return parseFloat(num).toLocaleString('en-US', {
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals,
        });
    }

    // ===========================================
    // Tab Navigation
    // ===========================================

    function initTabs() {
        elements.tabs.forEach(tab => {
            tab.addEventListener('click', () => {
                const tabId = tab.dataset.tab;

                // Update active tab
                elements.tabs.forEach(t => t.classList.remove('active'));
                tab.classList.add('active');

                // Update active content
                elements.tabContents.forEach(content => {
                    content.classList.remove('active');
                    if (content.id === `tab-${tabId}`) {
                        content.classList.add('active');
                    }
                });

                // Load data for tab
                if (tabId === 'api-stats') {
                    loadExchanges();
                    loadStats();
                } else if (tabId === 'settings') {
                    loadSettings();
                }
            });
        });
    }

    // ===========================================
    // Event Listeners
    // ===========================================

    function initEventListeners() {
        // Add pair button
        elements.btnAddPair.addEventListener('click', openAddPairModal);

        // Modal close buttons
        document.getElementById('modal-pair-close').addEventListener('click', () => {
            closeModal(elements.modalPair);
            document.getElementById('pair-symbol').disabled = false;
        });
        document.getElementById('btn-cancel-pair').addEventListener('click', () => {
            closeModal(elements.modalPair);
            document.getElementById('pair-symbol').disabled = false;
        });

        document.getElementById('modal-exchange-close').addEventListener('click', () => closeModal(elements.modalExchange));
        document.getElementById('btn-cancel-exchange').addEventListener('click', () => closeModal(elements.modalExchange));

        document.getElementById('btn-confirm-cancel').addEventListener('click', () => closeModal(elements.modalConfirm));
        document.getElementById('btn-confirm-ok').addEventListener('click', async () => {
            if (confirmCallback) {
                try {
                    await confirmCallback();
                } catch (error) {
                    showToast('error', error.message);
                }
            }
            closeModal(elements.modalConfirm);
            confirmCallback = null;
        });

        // Modal backdrop clicks
        document.querySelectorAll('.modal-backdrop').forEach(backdrop => {
            backdrop.addEventListener('click', () => {
                closeModal(backdrop.closest('.modal'));
                document.getElementById('pair-symbol').disabled = false;
            });
        });

        // Form submissions
        elements.formPair.addEventListener('submit', handlePairFormSubmit);
        elements.formExchange.addEventListener('submit', handleExchangeFormSubmit);

        // Settings
        elements.btnSaveSettings.addEventListener('click', handleSaveSettings);

        // Reset stats
        elements.btnResetStats.addEventListener('click', () => {
            showConfirm('Reset Statistics', 'Reset all statistics? This cannot be undone.', async () => {
                await api.resetStats();
                await loadStats();
                showToast('success', 'Statistics reset');
            });
        });

        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                document.querySelectorAll('.modal.active').forEach(modal => {
                    closeModal(modal);
                    document.getElementById('pair-symbol').disabled = false;
                });
            }
        });
    }

    // ===========================================
    // Initialization
    // ===========================================

    async function init() {
        console.log('Initializing ARB Terminal...');

        initTabs();
        initEventListeners();

        // Initial data load
        await Promise.all([
            loadPairs(),
            loadExchanges(),
            loadSettings(),
        ]);

        // Connect WebSocket
        connectWebSocket();

        // Periodic balance refresh
        setInterval(async () => {
            try {
                const balances = await api.refreshBalances();
                state.balances = balances;
                renderBalances();
            } catch (error) {
                console.error('Balance refresh error:', error);
            }
        }, 60000); // Every minute

        console.log('ARB Terminal initialized');
    }

    // Start app when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // Add toast out animation
    const style = document.createElement('style');
    style.textContent = `
        @keyframes toastOut {
            from { opacity: 1; transform: translateX(0); }
            to { opacity: 0; transform: translateX(100%); }
        }
    `;
    document.head.appendChild(style);

})();
