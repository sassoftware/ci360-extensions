// Snowy CI360 Monitor - Main Application Script

// Configuration
const CONFIG = {
    CONSOLE_DEFAULTS: {
        collapsed: true,
        maxLogs: 1000
    },
    DISPLAY_DEFAULTS: {
        showIdentityId: true,
        showTenantId: true,
        showActiveAgent: true
    },
    THEME_DEFAULTS: {
        theme: 'light',
        colorPreference: 'default'
    },
    STORAGE_KEYS: {
        theme: 'theme',
        colorPreference: 'colorPreference',
        consoleCollapsed: 'consoleCollapsed',
        showIdentityId: 'showIdentityId',
        showTenantId: 'showTenantId',
        showActiveAgent: 'showActiveAgent',
        identityId: 'identityId'
    }
};

// Utility Functions
const Utils = {
    getStorageItem(key, defaultValue = null) {
        return localStorage.getItem(key) || defaultValue;
    },
    
    setStorageItem(key, value) {
        localStorage.setItem(key, value);
    },
    
    getStorageBoolean(key, defaultValue = false) {
        const stored = localStorage.getItem(key);
        if (stored === null) {
            localStorage.setItem(key, defaultValue.toString());
            return defaultValue;
        }
        return stored === 'true';
    },
    
    formatTimestamp() {
        return new Date().toLocaleTimeString('en-US', { hour12: false });
    },
    
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
};

// Theme Management
class ThemeManager {
    constructor() {
        // Initialize theme from localStorage or browser preference
        this.currentTheme = this.getInitialTheme();
        this.colorPreference = Utils.getStorageItem(CONFIG.STORAGE_KEYS.colorPreference, CONFIG.THEME_DEFAULTS.colorPreference);
        this.applyTheme();
        this.applyColorPreference();
        this.setupEventListeners();
    }

    getInitialTheme() {
        const savedTheme = Utils.getStorageItem(CONFIG.STORAGE_KEYS.theme);
        if (savedTheme) return savedTheme;
        
        // Check browser preference
        if (window.matchMedia?.('(prefers-color-scheme: dark)').matches) {
            return 'dark';
        }
        
        return CONFIG.THEME_DEFAULTS.theme;
    }

    applyTheme() {
        document.body.setAttribute('data-bs-theme', this.currentTheme);
        Utils.setStorageItem(CONFIG.STORAGE_KEYS.theme, this.currentTheme);
        
        // Update the theme toggle switch if it exists
        const themeToggle = document.getElementById('theme-toggle-switch');
        if (themeToggle) {
            themeToggle.checked = this.currentTheme === 'dark';
        }
    }

    applyColorPreference() {
        document.body.setAttribute('data-user-colors', this.colorPreference);
        Utils.setStorageItem(CONFIG.STORAGE_KEYS.colorPreference, this.colorPreference);
    }

    toggleTheme() {
        this.currentTheme = this.currentTheme === 'light' ? 'dark' : 'light';
        this.applyTheme();
        console.log('Theme switched to:', this.currentTheme);
    }

    setColorPreference(preference) {
        this.colorPreference = preference;
        this.applyColorPreference();
        console.log('Color preference set to:', preference);
    }

    setupEventListeners() {
        // Setup theme toggle switch
        setTimeout(() => {
            const themeToggle = document.getElementById('theme-toggle-switch');
            if (themeToggle) {
                themeToggle.checked = this.currentTheme === 'dark';
                themeToggle.addEventListener('change', (e) => {
                    this.currentTheme = e.target.checked ? 'dark' : 'light';
                    this.applyTheme();
                });
            }
        }, 100);

        // Listen for browser theme changes
        if (window.matchMedia) {
            window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
                if (!localStorage.getItem('theme')) {
                    this.currentTheme = e.matches ? 'dark' : 'light';
                    this.applyTheme();
                    // Update the switch to reflect the change
                    const themeToggle = document.getElementById('theme-toggle-switch');
                    if (themeToggle) {
                        themeToggle.checked = this.currentTheme === 'dark';
                    }
                    console.log('Browser theme changed, updated to:', this.currentTheme);
                }
            });
        }

        // Setup color preference controls
        this.setupColorPreferenceControls();
    }

    setupColorPreferenceControls() {
        // Set initial state of radio buttons
        setTimeout(() => {
            const currentPreferenceRadio = document.getElementById(`color-${this.colorPreference}`);
            if (currentPreferenceRadio) {
                currentPreferenceRadio.checked = true;
            }
        }, 100);

        // Listen for color preference changes
        document.querySelectorAll('input[name="colorPreference"]').forEach(radio => {
            radio.addEventListener('change', (e) => {
                if (e.target.checked) {
                    this.setColorPreference(e.target.value);
                }
            });
        });
    }

    // Method to cycle through color preferences for demo
    cycleColorPreferences() {
        const preferences = ['default', 'high-contrast', 'colorblind-friendly', 'soft'];
        const currentIndex = preferences.indexOf(this.colorPreference);
        const nextIndex = (currentIndex + 1) % preferences.length;
        this.setColorPreference(preferences[nextIndex]);
        return preferences[nextIndex];
    }
}

// Tab Management
class TabManager {
    constructor() {
        this.activeTab = 'network';
        this.setupEventListeners();
    }

    setupEventListeners() {
        // Manual tab switching without Bootstrap JS
        const tabs = document.querySelectorAll('[data-bs-toggle="pill"]');
        
        tabs.forEach(tab => {
            tab.addEventListener('click', (event) => {
                event.preventDefault();
                const tabName = event.currentTarget.getAttribute('data-tab');
                this.activateTab(event.currentTarget, tabName);
            });
        });
    }

    activateTab(tabElement, tabName) {
        // Remove active class from all tabs
        document.querySelectorAll('[data-bs-toggle="pill"]').forEach(t => {
            t.classList.remove('active');
            t.setAttribute('aria-selected', 'false');
        });
        
        // Remove active class from all tab panes
        document.querySelectorAll('.tab-pane').forEach(pane => {
            pane.classList.remove('show', 'active');
        });
        
        // Activate clicked tab
        tabElement.classList.add('active');
        tabElement.setAttribute('aria-selected', 'true');
        
        // Restore recording animation if network tab and recording is active
        if (tabName === 'network' && window.networkTableManager && window.networkTableManager.isRecording) {
            tabElement.classList.add('recording-active');
        }
        
        // Activate corresponding tab pane
        const targetPaneId = tabElement.getAttribute('data-bs-target');
        const targetPane = document.querySelector(targetPaneId);
        if (targetPane) {
            targetPane.classList.add('show', 'active');
        }
        
        this.activeTab = tabName;
        console.log('Tab activated:', tabName);
    }

    switchTab(tabName) {
        const targetTab = document.querySelector(`[data-tab="${tabName}"]`);
        if (targetTab) {
            this.activateTab(targetTab, tabName);
        }
    }
}

// Display Management for Top Bar Items
class DisplayManager {
    constructor() {
        // Initialize UUID
        this.identityId = Utils.getStorageItem(CONFIG.STORAGE_KEYS.identityId) || this.generateUUID();
        Utils.setStorageItem(CONFIG.STORAGE_KEYS.identityId, this.identityId);
        
        // Initialize visibility settings with proper defaults
        this.identityVisible = Utils.getStorageBoolean(CONFIG.STORAGE_KEYS.showIdentityId, CONFIG.DISPLAY_DEFAULTS.showIdentityId);
        this.tenantVisible = Utils.getStorageBoolean(CONFIG.STORAGE_KEYS.showTenantId, CONFIG.DISPLAY_DEFAULTS.showTenantId);
        this.agentVisible = Utils.getStorageBoolean(CONFIG.STORAGE_KEYS.showActiveAgent, CONFIG.DISPLAY_DEFAULTS.showActiveAgent);
        
        console.log('DisplayManager initialized:', {
            identityVisible: this.identityVisible,
            tenantVisible: this.tenantVisible,
            agentVisible: this.agentVisible
        });
        
        this.setupEventListeners();
        this.updateDisplay();
    }

    generateUUID() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = Math.random() * 16 | 0;
            const v = c === 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    }

    setIdentityVisibility(visible) {
        this.identityVisible = visible;
        Utils.setStorageItem(CONFIG.STORAGE_KEYS.showIdentityId, visible.toString());
        console.log('Identity visibility set to:', visible);
        this.updateDisplay();
    }

    setTenantVisibility(visible) {
        this.tenantVisible = visible;
        Utils.setStorageItem(CONFIG.STORAGE_KEYS.showTenantId, visible.toString());
        console.log('Tenant visibility set to:', visible);
        this.updateDisplay();
    }

    setAgentVisibility(visible) {
        this.agentVisible = visible;
        Utils.setStorageItem(CONFIG.STORAGE_KEYS.showActiveAgent, visible.toString());
        console.log('Agent visibility set to:', visible);
        this.updateDisplay();
    }

    updateDisplay() {
        // Update Identity ID display
        const identityInfo = document.getElementById('identity-info');
        const identityText = document.getElementById('identity-text');
        
        if (identityInfo) {
            identityInfo.style.display = this.identityVisible ? 'block' : 'none';
        }
        
        if (identityText) {
            identityText.textContent = this.identityId;
        }

        // Update Tenant ID display
        const tenantInfo = document.getElementById('tenant-info');
        if (tenantInfo) {
            tenantInfo.style.display = this.tenantVisible ? 'block' : 'none';
        }

        // Update Active Agent display
        const agentInfo = document.getElementById('agent-info');
        if (agentInfo) {
            agentInfo.style.display = this.agentVisible ? 'block' : 'none';
        }
    }

    setupEventListeners() {
        // Set initial state of checkboxes and setup listeners
        setTimeout(() => {
            // Identity ID toggle
            const identityCheckbox = document.getElementById('show-identity-id');
            if (identityCheckbox) {
                identityCheckbox.checked = this.identityVisible;
                console.log('Identity checkbox initialized to:', this.identityVisible);
                identityCheckbox.addEventListener('change', (e) => {
                    this.setIdentityVisibility(e.target.checked);
                });
            }

            // Tenant ID toggle
            const tenantCheckbox = document.getElementById('show-tenant-id');
            if (tenantCheckbox) {
                tenantCheckbox.checked = this.tenantVisible;
                console.log('Tenant checkbox initialized to:', this.tenantVisible);
                tenantCheckbox.addEventListener('change', (e) => {
                    this.setTenantVisibility(e.target.checked);
                });
            }

            // Active Agent toggle
            const agentCheckbox = document.getElementById('show-active-agent');
            if (agentCheckbox) {
                agentCheckbox.checked = this.agentVisible;
                console.log('Agent checkbox initialized to:', this.agentVisible);
                agentCheckbox.addEventListener('change', (e) => {
                    this.setAgentVisibility(e.target.checked);
                });
            }
        }, 100);
    }

    // Method to refresh display settings from localStorage (useful for debugging)
    refreshFromStorage() {
        this.identityVisible = this.getStoredBoolean('showIdentityId', true);
        this.tenantVisible = this.getStoredBoolean('showTenantId', true);
        this.agentVisible = this.getStoredBoolean('showActiveAgent', true);
        
        // Update checkboxes
        const identityCheckbox = document.getElementById('show-identity-id');
        const tenantCheckbox = document.getElementById('show-tenant-id');
        const agentCheckbox = document.getElementById('show-active-agent');
        
        if (identityCheckbox) identityCheckbox.checked = this.identityVisible;
        if (tenantCheckbox) tenantCheckbox.checked = this.tenantVisible;
        if (agentCheckbox) agentCheckbox.checked = this.agentVisible;
        
        this.updateDisplay();
    }

    regenerateIdentity() {
        this.identityId = this.generateUUID();
        this.updateDisplay();
        return this.identityId;
    }
}

// Info Panel Management
class InfoManager {
    constructor() {
        this.tenantId = 'Not Connected';
        this.tenantStatus = 'disconnected';
        this.activeAgent = 'General Agent';
        this.agentStatus = 'active';
        this.updateDisplay();
    }

    updateTenantId(tenantId, status = 'connected') {
        this.tenantId = tenantId;
        this.tenantStatus = status;
        this.updateDisplay();
    }

    updateActiveAgent(agent, status = 'active') {
        this.activeAgent = agent;
        this.agentStatus = status;
        this.updateDisplay();
    }

    setTenantStatus(status) {
        this.tenantStatus = status;
        this.updateDisplay();
    }

    setAgentStatus(status) {
        this.agentStatus = status;
        this.updateDisplay();
    }

    updateDisplay() {
        // Update tenant display
        const tenantText = document.getElementById('tenant-text');
        const tenantStatus = document.getElementById('tenant-status');
        
        if (tenantText && tenantStatus) {
            tenantText.textContent = this.tenantId;
            
            // Remove all status classes
            tenantStatus.className = 'status-indicator';
            tenantText.className = '';
            
            // Add appropriate status class
            switch (this.tenantStatus) {
                case 'connected':
                    tenantStatus.classList.add('status-connected');
                    tenantText.classList.add('text-connected');
                    break;
                case 'warning':
                    tenantStatus.classList.add('status-warning');
                    tenantText.classList.add('text-warning');
                    break;
                case 'disconnected':
                default:
                    tenantStatus.classList.add('status-disconnected');
                    tenantText.classList.add('text-disconnected');
                    break;
            }
        }

        // Update agent display
        const agentText = document.getElementById('agent-text');
        const agentStatus = document.getElementById('agent-status');
        
        if (agentText && agentStatus) {
            agentText.textContent = this.activeAgent;
            
            // Remove all status classes
            agentStatus.className = 'status-indicator';
            agentText.className = '';
            
            // Add appropriate status class
            switch (this.agentStatus) {
                case 'active':
                    agentStatus.classList.add('status-active');
                    agentText.classList.add('text-active');
                    break;
                case 'warning':
                    agentStatus.classList.add('status-warning');
                    agentText.classList.add('text-warning');
                    break;
                case 'inactive':
                default:
                    agentStatus.classList.add('status-inactive');
                    agentText.classList.add('text-inactive');
                    break;
            }
        }
    }

    // Simulate connection states for demo
    simulateConnectionStates() {
        // Start with disconnected tenant
        setTimeout(() => {
            this.updateTenantId('Connecting...', 'warning');
            if (window.consoleManager) {
                window.consoleManager.warning('Attempting to connect to tenant...');
            }
        }, 1000);

        // Show connected tenant
        setTimeout(() => {
            this.updateTenantId('DEMO-TENANT-123', 'connected');
            if (window.consoleManager) {
                window.consoleManager.success('Connected to tenant: DEMO-TENANT-123');
            }
        }, 2500);

        // Show agent switching
        setTimeout(() => {
            this.updateActiveAgent('Diagnostic Agent', 'active');
            if (window.consoleManager) {
                window.consoleManager.info('Switched to Diagnostic Agent');
            }
        }, 4000);

        // Show warning state
        setTimeout(() => {
            this.setAgentStatus('warning');
            if (window.consoleManager) {
                window.consoleManager.warning('Agent status: High activity detected');
            }
        }, 6000);

        // Back to active
        setTimeout(() => {
            this.setAgentStatus('active');
            if (window.consoleManager) {
                window.consoleManager.success('Agent status: Normal operation resumed');
            }
        }, 8000);

        // Demo color preference cycling (optional)
        let colorCycleCount = 0;
        const colorCycleInterval = setInterval(() => {
            if (window.themeManager && colorCycleCount < 3) {
                const newPreference = window.themeManager.cycleColorPreferences();
                console.log('Demo: Cycled to color preference:', newPreference);
                colorCycleCount++;
            } else {
                clearInterval(colorCycleInterval);
            }
        }, 10000); // Change every 10 seconds for demo
    }
}

// Console Management
class ConsoleManager {
    constructor() {
        this.consoleElement = document.getElementById('snowy-console');
        this.consoleContent = document.getElementById('console-content');
        this.consoleCount = document.getElementById('console-count');
        this.consoleHeader = document.getElementById('console-header');
        this.consoleClear = document.getElementById('console-clear');
        this.consoleLatestMessage = null; // Will create this element
        this.logCount = 0;
        this.maxLogs = CONFIG.CONSOLE_DEFAULTS.maxLogs;
        this.isCollapsed = Utils.getStorageBoolean(CONFIG.STORAGE_KEYS.consoleCollapsed, CONFIG.CONSOLE_DEFAULTS.collapsed);
        
        this.setupLatestMessageDisplay();
        this.setupEventListeners();
        this.applyCollapsedState();
        
        // Log initial message
        this.log('Snowy CI360 Monitor initialized', 'success');
    }
    
    setupLatestMessageDisplay() {
        // Add latest message element to header
        const headerLeft = this.consoleHeader.querySelector('.d-flex');
        this.consoleLatestMessage = document.createElement('span');
        this.consoleLatestMessage.className = 'console-latest-message text-muted small ms-2';
        this.consoleLatestMessage.style.overflow = 'hidden';
        this.consoleLatestMessage.style.textOverflow = 'ellipsis';
        this.consoleLatestMessage.style.whiteSpace = 'nowrap';
        this.consoleLatestMessage.style.flex = '1';
        headerLeft.appendChild(this.consoleLatestMessage);
    }

    setupEventListeners() {
        // Toggle console on header click
        this.consoleHeader.addEventListener('click', (e) => {
            // Don't toggle if clicking the clear button
            if (e.target.closest('#console-clear')) return;
            this.toggleCollapse();
        });

        // Clear console
        this.consoleClear.addEventListener('click', (e) => {
            e.stopPropagation();
            this.clear();
        });
    }

    toggleCollapse() {
        this.isCollapsed = !this.isCollapsed;
        this.applyCollapsedState();
        Utils.setStorageItem(CONFIG.STORAGE_KEYS.consoleCollapsed, this.isCollapsed.toString());
    }

    applyCollapsedState() {
        this.consoleElement.classList.toggle('collapsed', this.isCollapsed);
        document.body.classList.toggle('console-expanded', !this.isCollapsed);
    }

    log(message, type = 'info') {
        const logEntry = document.createElement('div');
        logEntry.className = `console-log ${type}`;
        logEntry.innerHTML = `<span class="timestamp">[${Utils.formatTimestamp()}]</span><span>${Utils.escapeHtml(message)}</span>`;
        
        // Insert at the top instead of bottom
        this.consoleContent.insertBefore(logEntry, this.consoleContent.firstChild);
        this.logCount++;
        this.consoleCount.textContent = this.logCount;
        
        // Update latest message in header
        if (this.consoleLatestMessage) {
            this.consoleLatestMessage.textContent = message;
            this.consoleLatestMessage.className = `console-latest-message small ms-2 text-${type === 'error' ? 'danger' : type === 'warning' ? 'warning' : type === 'success' ? 'success' : 'muted'}`;
        }
        
        // Limit log count to prevent memory issues
        if (this.logCount > this.maxLogs) {
            this.consoleContent.lastChild.remove();
            this.logCount--;
        }
        
        // No need to scroll since latest is at top
        
        // Also log to browser console
        console.log(`[Snowy Console] ${message}`);
    }

    clear() {
        this.consoleContent.innerHTML = '';
        this.logCount = 0;
        this.consoleCount.textContent = '0';
        this.log('Console cleared', 'info');
    }

    // Public methods for different log types
    info(message) { this.log(message, 'info'); }
    success(message) { this.log(message, 'success'); }
    warning(message) { this.log(message, 'warning'); }
    error(message) { this.log(message, 'error'); }
}

// Tenant Configuration Manager
class TenantConfigManager {
    constructor() {
        this.savedTenants = [];
        this.activeTenant = null;
        this.storageKey = 'ci360SavedTenantList';
        
        this.initializeElements();
        this.loadSavedTenants();
        this.setupEventListeners();
    }

    initializeElements() {
        this.savedTenantList = $('#saved-tenant-list');
        this.activateTenantBtn = $('#activate-tenant-btn');
        this.forgetTenantBtn = $('#forget-tenant-btn');
        this.gatewayDropdown = $('#gateway-dropdown');
        this.gatewayCustom = $('#gateway-custom');
        this.tenantIdInput = $('#tenant-id-input');
        this.tenantSecretInput = $('#tenant-secret-input');
        this.tenantNameInput = $('#tenant-name-input');
        this.saveActivateBtn = $('#save-activate-btn');
        this.activateOnlyBtn = $('#activate-only-btn');
        this.clearConfigBtn = $('#clear-config-btn');
        this.activeConfigDisplay = $('#active-config-display');
    }

    setupEventListeners() {
        // Saved tenant actions
        this.savedTenantList.on('change', () => {
            const hasSelection = this.savedTenantList.val() !== '';
            this.activateTenantBtn.prop('disabled', !hasSelection);
            this.forgetTenantBtn.prop('disabled', !hasSelection);
        });

        this.activateTenantBtn.on('click', () => this.activateSavedTenant());
        this.forgetTenantBtn.on('click', () => this.forgetTenant());

        // New tenant actions
        this.saveActivateBtn.on('click', () => this.saveAndActivate());
        this.activateOnlyBtn.on('click', () => this.activateWithoutSaving());
        this.clearConfigBtn.on('click', () => this.clearForm());

        // Auto-disable custom input when dropdown is selected
        this.gatewayDropdown.on('change', () => {
            if (this.gatewayDropdown.val()) {
                this.gatewayCustom.val('').prop('disabled', true);
            } else {
                this.gatewayCustom.prop('disabled', false);
            }
        });

        this.gatewayCustom.on('input', () => {
            if (this.gatewayCustom.val()) {
                this.gatewayDropdown.val('');
            }
        });
    }

    loadSavedTenants() {
        const stored = Utils.getStorageItem(this.storageKey);
        if (stored) {
            try {
                const data = JSON.parse(stored);
                this.savedTenants = data.tenants || [];
                this.updateSavedTenantsList();
                
                if (this.savedTenants.length > 0) {
                    consoleManager.info(`Loaded ${this.savedTenants.length} saved tenant(s)`);
                }
            } catch (e) {
                consoleManager.error('Failed to load saved tenants');
            }
        }
    }

    updateSavedTenantsList() {
        this.savedTenantList.empty();
        
        if (this.savedTenants.length === 0) {
            this.savedTenantList.append('<option value="">-- No saved tenants --</option>');
            this.activateTenantBtn.prop('disabled', true);
            this.forgetTenantBtn.prop('disabled', true);
        } else {
            this.savedTenantList.append('<option value="">-- Select a tenant --</option>');
            this.savedTenants.forEach(tenant => {
                this.savedTenantList.append(`<option value="${tenant.id}">${tenant.tenantName}</option>`);
            });
        }
    }

    validateInputs() {
        const errors = [];
        
        // Clear previous error states
        $('.form-control, .form-select').removeClass('is-invalid');

        // Get gateway URL
        let gatewayUrl = this.gatewayDropdown.val();
        if (this.gatewayCustom.val().trim()) {
            gatewayUrl = this.gatewayCustom.val().trim();
            if (gatewayUrl.includes(':') || gatewayUrl.includes(' ')) {
                this.gatewayCustom.addClass('is-invalid');
                errors.push('Invalid gateway URL format');
            }
        }

        // Validate tenant ID
        const tenantId = this.tenantIdInput.val().trim();
        if (!tenantId) {
            this.tenantIdInput.addClass('is-invalid');
            errors.push('Tenant ID is required');
        }

        // Validate secret
        const tenantSecret = this.tenantSecretInput.val().trim();
        if (!tenantSecret) {
            this.tenantSecretInput.addClass('is-invalid');
            errors.push('Client Secret is required');
        }

        // Validate name
        const tenantName = this.tenantNameInput.val().trim();
        if (!tenantName) {
            this.tenantNameInput.addClass('is-invalid');
            errors.push('Friendly Name is required');
        }

        if (errors.length > 0) {
            consoleManager.error(errors.join(', '));
            return null;
        }

        return {
            id: this.generateUUID(),
            tenantUrl: gatewayUrl,
            tenantId: tenantId,
            tenantSecret: tenantSecret,
            tenantName: tenantName
        };
    }

    saveAndActivate() {
        const tenantConfig = this.validateInputs();
        if (!tenantConfig) return;

        // Add to saved tenants
        this.savedTenants.push(tenantConfig);
        this.saveTenants();
        this.updateSavedTenantsList();
        
        // Activate
        this.activateTenant(tenantConfig);
        this.clearForm();
        
        consoleManager.success(`Tenant "${tenantConfig.tenantName}" saved and activated`);
    }

    activateWithoutSaving() {
        const tenantConfig = this.validateInputs();
        if (!tenantConfig) return;

        this.activateTenant(tenantConfig);
        consoleManager.info(`Tenant "${tenantConfig.tenantName}" activated (not saved)`);
    }

    activateSavedTenant() {
        const selectedId = this.savedTenantList.val();
        const tenant = this.savedTenants.find(t => t.id === selectedId);
        
        if (tenant) {
            this.activateTenant(tenant);
            consoleManager.success(`Activated tenant: ${tenant.tenantName}`);
        }
    }

    activateTenant(tenant) {
        this.activeTenant = tenant;
        
        // Update active configuration display
        $('#active-tenant-name').text(tenant.tenantName);
        $('#active-tenant-id').text(tenant.tenantId);
        $('#active-gateway-url').text(tenant.tenantUrl);
        this.activeConfigDisplay.show();

        // TODO: Implement actual token generation and gateway validation
        // For now, just show the configuration
    }

    forgetTenant() {
        const selectedId = this.savedTenantList.val();
        const tenant = this.savedTenants.find(t => t.id === selectedId);
        
        if (tenant && confirm(`Are you sure you want to forget "${tenant.tenantName}"?`)) {
            this.savedTenants = this.savedTenants.filter(t => t.id !== selectedId);
            this.saveTenants();
            this.updateSavedTenantsList();
            consoleManager.warning(`Tenant "${tenant.tenantName}" forgotten`);
        }
    }

    clearForm() {
        this.gatewayDropdown.val('extapigwservice-demo.cidemo.sas.com');
        this.gatewayCustom.val('').prop('disabled', true);
        this.tenantIdInput.val('');
        this.tenantSecretInput.val('');
        this.tenantNameInput.val('');
        $('.form-control, .form-select').removeClass('is-invalid');
    }

    saveTenants() {
        const data = { tenants: this.savedTenants };
        Utils.setStorageItem(this.storageKey, JSON.stringify(data));
    }

    generateUUID() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = Math.random() * 16 | 0;
            const v = c === 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    }
}

// Network Table Manager
class NetworkTableManager {
    constructor() {
        this.rowCounter = 0;
        this.dataTable = null;
        this.isRecording = true; // Start with recording enabled
        this.initializeTable();
        this.initializeToolbar();
        this.initializeNetworkListener();
    }
    
    initializeToolbar() {
        // Clear button
        $('#network-clear-btn').on('click', () => {
            this.clearTable();
        });
        
        // Start/Stop toggle button
        $('#network-toggle-btn').on('click', () => {
            this.toggleRecording();
        });
    }
    
    clearTable() {
        if (this.dataTable) {
            this.dataTable.clear().draw();
            this.rowCounter = 0;
            consoleManager.info('Network table cleared');
        }
    }
    
    toggleRecording() {
        this.isRecording = !this.isRecording;
        const btn = $('#network-toggle-btn');
        const networkTab = $('[data-tab="network"]');
        
        if (this.isRecording) {
            btn.removeClass('btn-success').addClass('btn-warning');
            btn.attr('data-recording', 'true');
            btn.attr('title', 'Stop recording');
            btn.html('<i class="bi bi-pause-fill"></i> Stop');
            networkTab.addClass('recording-active');
            consoleManager.success('Network recording started');
        } else {
            btn.removeClass('btn-warning').addClass('btn-success');
            btn.attr('data-recording', 'false');
            btn.attr('title', 'Start recording');
            btn.html('<i class="bi bi-play-fill"></i> Start');
            networkTab.removeClass('recording-active');
            consoleManager.warning('Network recording stopped');
        }
    }
    
    initializeTable() {
        // Initialize DataTables
        this.dataTable = $('#networkTable').DataTable({
            dom: '<"top-controls"<"network-toolbar-container">f>rtip',
            pageLength: 50,
            lengthChange: false, // Hide page length dropdown
            order: [[7, "desc"]], // Sort by Req# descending (newest first)
            language: {
                search: "_INPUT_",
                searchPlaceholder: "Search...",
            },
            initComplete: function() {
                // Insert toolbar buttons into the toolbar container
                $('div.network-toolbar-container').html(
                    '<button class="btn btn-sm btn-danger" id="network-clear-btn" title="Clear all network events">' +
                    '<i class="bi bi-x-circle"></i> Clear</button>' +
                    '<button class="btn btn-sm btn-warning" id="network-toggle-btn" title="Stop recording" data-recording="true">' +
                    '<i class="bi bi-pause-fill"></i> Stop</button>'
                );
                
                // Add column search inputs
                this.api().columns([1, 3, 4, 5, 6]).every(function() {
                    const column = this;
                    const title = $(column.header()).text();
                    
                    // Create input element
                    const input = $('<input type="text" placeholder="Search ' + title + '" class="form-control form-control-sm" />')
                        .appendTo($(column.header()))
                        .on('click', function(e) {
                            e.stopPropagation(); // Prevent sorting when clicking input
                        })
                        .on('keyup change clear', function() {
                            if (column.search() !== this.value) {
                                column.search(this.value).draw();
                            }
                        });
                });
            },
            columns: [
                {
                    className: "details-control",
                    orderable: false,
                    data: null,
                    defaultContent: '<i class="bi bi-chevron-right expand-icon"></i>',
                    width: "30px"
                },
                { data: "event" },
                { data: "event_json", visible: false }, // Hidden column
                { data: "quickInfo" },
                { 
                    data: "timestamp",
                    render: (data) => {
                        if (!data) return '-';
                        // Handle both numeric timestamps (milliseconds) and ISO strings
                        let date;
                        if (typeof data === 'string' && !isNaN(data)) {
                            // String number like "1763015089243"
                            date = new Date(parseInt(data, 10));
                        } else if (typeof data === 'number') {
                            // Numeric timestamp
                            date = new Date(data);
                        } else {
                            // ISO string or other format
                            date = new Date(data);
                        }
                        
                        // Check if date is valid
                        if (isNaN(date.getTime())) {
                            return data; // Return original value if can't parse
                        }
                        
                        // Format: DD MMM YYYY : hh:mm:ss.SSS
                        const day = String(date.getDate()).padStart(2, '0');
                        const month = date.toLocaleString('en-US', { month: 'short' });
                        const year = date.getFullYear();
                        const hours = String(date.getHours()).padStart(2, '0');
                        const minutes = String(date.getMinutes()).padStart(2, '0');
                        const seconds = String(date.getSeconds()).padStart(2, '0');
                        const milliseconds = String(date.getMilliseconds()).padStart(3, '0');
                        return `${day} ${month} ${year} : ${hours}:${minutes}:${seconds}.${milliseconds}`;
                    }
                },
                { 
                    data: "status",
                    render: (data) => {
                        const statusClass = data >= 200 && data < 300 ? 'success' : data >= 400 ? 'danger' : 'warning';
                        return `<span class="badge bg-${statusClass} status-badge">${data}</span>`;
                    }
                },
                { data: "datahub_id" },
                { data: "row_num" }
            ],
            columnDefs: [
                { targets: [0], width: "30px" },
                { targets: [5, 7], width: "60px" }
            ]
        });
        
        // Add event listener for opening and closing details
        $('#networkTable tbody').on('click', 'td.details-control', (e) => {
            const tr = $(e.currentTarget).closest('tr');
            const row = this.dataTable.row(tr);
            
            if (row.child.isShown()) {
                // This row is already open - close it
                row.child.hide();
                tr.removeClass('shown');
                tr.find('.expand-icon').removeClass('expanded');
            } else {
                // Open this row
                row.child(this.formatDetails(row.data())).show();
                tr.addClass('shown');
                tr.find('.expand-icon').addClass('expanded');
            }
        });
    }
    
    addRow(eventData) {
        this.rowCounter++;
        
        // Keep original timestamp from event data, don't modify it
        const timestamp = eventData.timestamp || new Date().getTime();
        const status = eventData.status || 200;
        
        const rowData = {
            event: eventData.event || '-',
            event_json: JSON.stringify(eventData),
            quickInfo: eventData.quickInfo || this.getQuickInfo(eventData),
            timestamp: timestamp,
            status: status,
            datahub_id: eventData.datahub_id || '-',
            row_num: this.rowCounter
        };
        
        // Add row at the top (index 0)
        this.dataTable.row.add(rowData).draw(false);
        
        // Log to console
        window.consoleManager?.info(`SAS Tag: Event captured: ${eventData.event}`);
    }
    
    formatDetails(rowData) {
        const eventJson = rowData.event_json;
        return `
            <div class="detail-content-wrapper">
                ${this.formatJSONAsTable(eventJson)}
            </div>
        `;
    }
    
    formatJSONAsTable(jsonString) {
        try {
            const data = JSON.parse(jsonString);
            
            // Remove internal fields that shouldn't be displayed
            const filteredData = { ...data };
            delete filteredData.row_num;
            
            // Sort keys alphabetically
            const sortedKeys = Object.keys(filteredData).sort((a, b) => a.localeCompare(b));
            
            let rows = '';
            
            // Create rows with 2 key-value pairs each (4 columns total)
            for (let i = 0; i < sortedKeys.length; i += 2) {
                const key1 = sortedKeys[i];
                const value1 = filteredData[key1];
                
                const key2 = i + 1 < sortedKeys.length ? sortedKeys[i + 1] : null;
                const value2 = key2 ? filteredData[key2] : null;
                
                rows += `
                    <div class="detail-row">
                        <div class="detail-cell detail-key">${this.escapeHtml(key1)}</div>
                        <div class="detail-cell detail-value">
                            <div class="detail-value-content">${this.formatValue(value1)}</div>
                        </div>
                        ${key2 ? `
                            <div class="detail-cell detail-key">${this.escapeHtml(key2)}</div>
                            <div class="detail-cell detail-value">
                                <div class="detail-value-content">${this.formatValue(value2)}</div>
                            </div>
                        ` : '<div class="detail-cell"></div><div class="detail-cell"></div>'}
                    </div>
                `;
            }
            
            // Add raw JSON at the bottom (no indentation) - outside the grid
            const rawJson = JSON.stringify(filteredData);
            
            return `
                <div class="detail-table">
                    ${rows}
                </div>
                <div class="raw-json-section">
                    <div class="raw-json-header">Raw JSON:</div>
                    <div class="raw-json">${this.escapeHtml(rawJson)}</div>
                </div>
            `;
        } catch (e) {
            return `<div class="text-danger">Error parsing JSON: ${e.message}</div>`;
        }
    }
    
    formatValue(value) {
        if (value === null) {
            return '<span class="text-muted">null</span>';
        } else if (typeof value === 'boolean') {
            return `<span class="text-info">${value}</span>`;
        } else if (typeof value === 'number') {
            return `<span class="text-success">${value}</span>`;
        } else if (typeof value === 'object') {
            return `<span class="text-warning">${this.escapeHtml(JSON.stringify(value))}</span>`;
        } else if (typeof value === 'string') {
            return this.escapeHtml(value);
        }
        return this.escapeHtml(String(value));
    }
    
    getQuickInfo(eventData) {
        if (eventData.eventname && eventData.eventDesignedName && eventData.eventDesignedName !== eventData.eventname) {
            return `Event Name: ${eventData.eventname} / Design Name: ${eventData.eventDesignedName}`;
        }
        if (eventData.eventname && eventData.eventname !== eventData.event) {
            return `Event Name: ${eventData.eventname}`;
        }
        if (eventData.page_title) {
            return `Page: ${eventData.page_title}`;
        }
        if (eventData.page_path) {
            return `Path: ${eventData.page_path}`;
        }
        return '-';
    }
    
    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
    
    clearTable() {
        if (this.dataTable) {
            this.dataTable.clear().draw();
        }
        this.rowCounter = 0;
        window.consoleManager?.info('Network table cleared');
    }
    
    // Initialize Chrome DevTools network listener
    initializeNetworkListener() {
        if (chrome && chrome.devtools && chrome.devtools.network) {
            chrome.devtools.network.onRequestFinished.addListener((request) => {
                this.handleNetworkRequest(request);
            });
            window.consoleManager?.success('Network listener initialized');
        } else {
            window.consoleManager?.warn('Chrome DevTools API not available');
        }
    }
    
    handleNetworkRequest(request) {
        // Check if recording is enabled
        if (!this.isRecording) {
            return;
        }
        
        // Check if request is a CI360 event
        const isCi360Request = this.isCi360Request(request);
        
        if (!isCi360Request) {
            return;
        }
        
        // Process POST requests (events)
        if (request.request.method === 'POST' && request.request.postData && request.request.postData.params) {
            this.processPostRequest(request);
        }
        // Process OPTIONS or DELETE (detachIdentity)
        else if (request.request.method === 'OPTIONS' || request.request.method === 'DELETE') {
            this.processDetachIdentity(request);
        }
    }
    
    isCi360Request(request) {
        // Check URL patterns
        if (request.request && 
            (request.request.url.includes('/t/e/') || request.request.url.includes('/t/s/'))) {
            return true;
        }
        
        // Check initiator stack
        if (request._initiator && request._initiator.stack && request._initiator.stack.parent) {
            const initiatorUrl = request._initiator.stack.parent.callFrames[0]?.url || '';
            return initiatorUrl.includes('/t/e/') || 
                   initiatorUrl.includes('/t/s/') ||
                   initiatorUrl.includes('ot-min') ||
                   initiatorUrl.includes('ot-api') ||
                   initiatorUrl.includes('ot-all') ||
                   initiatorUrl.includes('ot2.min');
        }
        
        return false;
    }
    
    processPostRequest(request) {
        const params = {};
        const status = request.response.status === 0 ? '0 (cancelled)' : request.response.status;
        
        params.status = status;
        params.row_num = this.rowCounter + 1;
        
        // Parse POST parameters
        request.request.postData.params.forEach(param => {
            params[param.name] = decodeURIComponent(param.value);
        });
        
        // Add default fields
        if (!params.event) {
            if (params.login_event_type && params.login_event) {
                params.event = '*Inferred Event: attachIdentity';
            } else {
                params.event = 'unidentified event';
            }
        }
        
        // Only add request timestamp if event doesn't have one
        if (!params.timestamp) {
            params.timestamp = request.startedDateTime;
        }
        
        params.datahub_id = params.datahub_id || '';
        
        this.addRow(params);
    }
    
    processDetachIdentity(request) {
        const params = {
            status: request.response.status === 0 ? '0 (cancelled)' : request.response.status,
            event: `*Inferred Event: detachIdentity (method: ${request.request.method})`,
            row_num: this.rowCounter + 1,
            timestamp: request.startedDateTime,
            datahub_id: ''
        };
        
        this.addRow(params);
    }
}

// Initialize Application
document.addEventListener('DOMContentLoaded', () => {
    console.log('Snowy CI360 Monitor - Starting fresh...');
    
    // Initialize managers
    window.consoleManager = new ConsoleManager();
    window.themeManager = new ThemeManager();
    window.tabManager = new TabManager();
    window.displayManager = new DisplayManager();
    window.infoManager = new InfoManager();
    window.tenantConfigManager = new TenantConfigManager();
    window.networkTableManager = new NetworkTableManager();
    
    // Log initialization
    window.consoleManager.info('Theme Manager initialized');
    window.consoleManager.info('Tab Manager initialized');
    window.consoleManager.info('Display Manager initialized');
    window.consoleManager.info('Tenant Config Manager initialized');
    window.consoleManager.info('Network Table Manager initialized');
    window.consoleManager.success(`Identity ID: ${window.displayManager.identityId}`);
    
    console.log('Application initialized successfully');
    console.log('Identity ID generated:', window.displayManager.identityId);
    
    // Add recording animation to network tab after DOM is fully ready
    setTimeout(() => {
        const networkTab = $('[data-tab="network"]');
        if (window.networkTableManager.isRecording) {
            networkTab.addClass('recording-active');
            console.log('Recording animation added to network tab');
        }
    }, 100);
});