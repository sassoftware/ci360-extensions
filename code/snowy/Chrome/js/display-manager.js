// Snowy CI360 Monitor - Display Manager

class DisplayManager {
    constructor() {
        // Identity ID comes from network traffic only - never stored or generated
        this.identityId = null;
        
        // Initialize visibility settings with proper defaults
        this.identityIdVisible = this.getStoredBoolean('showIdentityId', true);
        this.tenantIdVisible = this.getStoredBoolean('showTenantId', true);
        this.agentVisible = this.getStoredBoolean('showActiveAgent', true);
        
        console.log('DisplayManager initialized:', {
            identityIdVisible: this.identityIdVisible,
            tenantIdVisible: this.tenantIdVisible,
            agentVisible: this.agentVisible
        });
        
        this.setupEventListeners();
        this.updateDisplay();
    }

    setIdentityIdVisibility(visible) {
        this.identityIdVisible = visible;
        Utils.setStorageItem(CONFIG.STORAGE_KEYS.showIdentityId, visible.toString());
        console.log('Identity ID visibility set to:', visible);
        this.updateDisplay();
    }

    setTenantIdVisibility(visible) {
        this.tenantIdVisible = visible;
        Utils.setStorageItem(CONFIG.STORAGE_KEYS.showTenantId, visible.toString());
        console.log('Tenant ID visibility set to:', visible);
        this.updateDisplay();
    }

    setAgentVisibility(visible) {
        this.agentVisible = visible;
        Utils.setStorageItem(CONFIG.STORAGE_KEYS.showActiveAgent, visible.toString());
        console.log('Agent visibility set to:', visible);
        this.updateDisplay();
    }

    updateDisplay() {
        // Update Identity ID visibility
        const identitySection = document.getElementById('identity-id-section');
        if (identitySection) {
            identitySection.style.display = this.identityIdVisible ? 'flex' : 'none';
        }
        
        // Update Tenant ID visibility
        const tenantSection = document.getElementById('tenant-id-section');
        if (tenantSection) {
            tenantSection.style.display = this.tenantIdVisible ? 'flex' : 'none';
        }
        
        // Update separator visibility (hide if both are hidden or only one is visible)
        const separator = document.getElementById('identity-tenant-separator');
        if (separator) {
            const showSeparator = this.identityIdVisible && this.tenantIdVisible;
            separator.style.display = showSeparator ? 'block' : 'none';
        }
        
        // Update entire Row 2 visibility (hide if both Identity and Tenant are hidden)
        const identityRow = document.getElementById('identity-info');
        if (identityRow) {
            const showRow = this.identityIdVisible || this.tenantIdVisible;
            identityRow.style.display = showRow ? 'flex' : 'none';
        }
        
        // Update identity text
        const identityText = document.getElementById('identity-text');
        if (identityText) {
            identityText.textContent = this.identityId || 'Not detected';
        }

        // Update Active Agent display
        const agentInfo = document.getElementById('agent-info');
        if (agentInfo) {
            agentInfo.style.display = this.agentVisible ? 'flex' : 'none';
        }
    }

    setupEventListeners() {
        // Set initial state of checkboxes and setup listeners
        setTimeout(() => {
            // Identity ID toggle
            const identityIdCheckbox = document.getElementById('show-identity-id');
            if (identityIdCheckbox) {
                identityIdCheckbox.checked = this.identityIdVisible;
                console.log('Identity ID checkbox initialized to:', this.identityIdVisible);
                identityIdCheckbox.addEventListener('change', (e) => {
                    this.setIdentityIdVisibility(e.target.checked);
                });
            }

            // Tenant ID toggle
            const tenantIdCheckbox = document.getElementById('show-tenant-id');
            if (tenantIdCheckbox) {
                tenantIdCheckbox.checked = this.tenantIdVisible;
                console.log('Tenant ID checkbox initialized to:', this.tenantIdVisible);
                tenantIdCheckbox.addEventListener('change', (e) => {
                    this.setTenantIdVisibility(e.target.checked);
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
        this.identityIdVisible = this.getStoredBoolean('showIdentityId', true);
        this.tenantIdVisible = this.getStoredBoolean('showTenantId', true);
        this.agentVisible = this.getStoredBoolean('showActiveAgent', true);
        
        // Update checkboxes
        const identityIdCheckbox = document.getElementById('show-identity-id');
        const tenantIdCheckbox = document.getElementById('show-tenant-id');
        const agentCheckbox = document.getElementById('show-active-agent');
        
        if (identityIdCheckbox) identityIdCheckbox.checked = this.identityIdVisible;
        if (tenantIdCheckbox) tenantIdCheckbox.checked = this.tenantIdVisible;
        if (agentCheckbox) agentCheckbox.checked = this.agentVisible;
        
        this.updateDisplay();
    }

    // Helper method to get boolean from storage
    getStoredBoolean(key, defaultValue) {
        return Utils.getStorageBoolean(CONFIG.STORAGE_KEYS[key], defaultValue);
    }

    // Update identity ID when detected from network traffic (not stored)
    setIdentityId(identityId) {
        if (identityId && identityId !== '-') {
            // Only log if this is a new/different identity
            const isNewIdentity = this.identityId !== identityId;
            this.identityId = identityId;
            this.updateDisplay();
            
            if (isNewIdentity) {
                window.consoleManager?.success(`Identity ID detected: ${this.identityId}`);
            }
        }
    }

    // Clear identity (for session reset)
    clearIdentity() {
        this.identityId = null;
        this.updateDisplay();
    }
}
