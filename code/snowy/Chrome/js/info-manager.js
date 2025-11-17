// Snowy CI360 Monitor - Info Manager

class InfoManager {
    constructor() {
        this.identityId = '';
        this.identityStatus = 'disconnected';
        this.tenantId = 'Not Connected';
        this.tenantStatus = 'disconnected';
        this.activeAgent = 'Not Connected';
        this.agentStatus = 'disconnected';
        this.updateDisplay();
    }

    updateIdentityId(identityId, status = 'connected') {
        this.identityId = identityId || '';
        this.identityStatus = status;
        
        // Also update DisplayManager if identity is detected
        if (identityId && identityId !== '-' && window.displayManager) {
            window.displayManager.setIdentityId(identityId);
        }
        
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
        // Update identity display
        const identityText = document.getElementById('identity-text');
        const identityStatus = document.getElementById('identity-status');
        
        if (identityText && identityStatus) {
            identityText.textContent = this.identityId;
            
            // Remove all status classes
            identityStatus.className = 'status-indicator';
            identityText.className = 'identity-uuid';
            
            // Add appropriate status class
            switch (this.identityStatus) {
                case 'connected':
                    identityStatus.classList.add('status-connected');
                    identityText.classList.add('text-connected');
                    break;
                case 'warning':
                    identityStatus.classList.add('status-warning');
                    identityText.classList.add('text-warning');
                    break;
                case 'disconnected':
                default:
                    identityStatus.classList.add('status-disconnected');
                    identityText.classList.add('text-disconnected');
                    break;
            }
        }
        
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
                case 'connected':
                    agentStatus.classList.add('status-connected');
                    agentText.classList.add('text-connected');
                    break;
                case 'warning':
                    agentStatus.classList.add('status-warning');
                    agentText.classList.add('text-warning');
                    break;
                case 'disconnected':
                    agentStatus.classList.add('status-disconnected');
                    agentText.classList.add('text-disconnected');
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
