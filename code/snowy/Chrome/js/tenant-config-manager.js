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
        // First try to migrate legacy data if present
        this.migrateLegacyTenants();
        
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

    migrateLegacyTenants() {
        // Try to get legacy data from Chrome sync storage
        if (typeof chrome !== 'undefined' && chrome.storage && chrome.storage.sync) {
            chrome.storage.sync.get('ci360SavedTenantList_prop', (data) => {
                if (data.ci360SavedTenantList_prop && data.ci360SavedTenantList_prop.tenants) {
                    const legacyTenants = data.ci360SavedTenantList_prop;
                    
                    consoleManager.info(`Found ${legacyTenants.tenants.length} tenant(s) in legacy storage`);
                    
                    // Get existing data if any
                    const existingData = Utils.getStorageItem(this.storageKey);
                    let existingTenants = { tenants: [] };
                    
                    if (existingData) {
                        try {
                            existingTenants = JSON.parse(existingData);
                        } catch (e) {
                            consoleManager.error('Failed to parse existing tenant data');
                        }
                    }
                    
                    // Merge legacy tenants with existing, avoiding duplicates based on tenant ID
                    const existingIds = new Set(existingTenants.tenants.map(t => t.id));
                    const newTenants = legacyTenants.tenants.filter(t => !existingIds.has(t.id));
                    
                    if (newTenants.length > 0) {
                        existingTenants.tenants.push(...newTenants);
                        Utils.setStorageItem(this.storageKey, JSON.stringify(existingTenants));
                        consoleManager.info(`Migrated ${newTenants.length} new tenant(s) from legacy storage`);
                    } else {
                        consoleManager.info('No new tenants to migrate (all already exist)');
                    }
                    
                    // Remove legacy data after successful migration
                    chrome.storage.sync.remove('ci360SavedTenantList_prop', () => {
                        consoleManager.info('Removed legacy storage');
                    });
                    
                    // Reload the tenants
                    this.savedTenants = existingTenants.tenants || [];
                    this.updateSavedTenantsList();
                }
            });
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

        consoleManager.info('Validating gateway configuration...');
        this.validateGatewayAndActivate(tenantConfig, true);
    }

    activateWithoutSaving() {
        const tenantConfig = this.validateInputs();
        if (!tenantConfig) return;

        consoleManager.info('Validating gateway configuration...');
        this.validateGatewayAndActivate(tenantConfig, false);
    }

    activateSavedTenant() {
        const selectedId = this.savedTenantList.val();
        const tenant = this.savedTenants.find(t => t.id === selectedId);
        
        if (tenant) {
            // Check if it's a diagnostic access point
            if (tenant.type && tenant.type.toLowerCase() === 'diag') {
                consoleManager.info('Diagnostics Access Point detected.');
                this.activateGateway(tenant);
            } else {
                // Warn user about non-diagnostic access point
                this.showConfirmModal(
                    'This is not a Diagnostics Access Point. Do you want to activate?',
                    () => this.activateGateway(tenant),
                    () => consoleManager.warning('Activation cancelled by user')
                );
            }
        }
    }

    validateGatewayAndActivate(tenantConfig, shouldSave) {
        // Log configuration details
        consoleManager.info(`Gateway URL: ${tenantConfig.tenantUrl}`);
        consoleManager.info(`Tenant ID: ${tenantConfig.tenantId}`);
        
        // Generate JWT token
        const token = this.generateToken(tenantConfig.tenantId, tenantConfig.tenantSecret);
        consoleManager.info('JWT token generated');

        // Call CI360 API to validate gateway
        const apiUrl = `https://${tenantConfig.tenantUrl}/marketingGateway/configuration`;
        
        $.ajax({
            url: apiUrl,
            method: 'GET',
            timeout: 10000,
            headers: {
                'Authorization': `Bearer ${token}`
            }
        })
        .done((response) => {
            if (response && response.agentName && response.type) {
                consoleManager.success(`Access Point Name: ${response.agentName}`);
                consoleManager.info(`Access Point Type: ${response.type}`);
                
                // Update tenant config with API response
                tenantConfig.type = response.type.toLowerCase();
                tenantConfig.token = token;
                
                // Append agent name if different from tenant name
                if (response.agentName.toLowerCase().trim() !== tenantConfig.tenantName.toLowerCase().trim()) {
                    tenantConfig.tenantName = `${tenantConfig.tenantName} (${response.agentName})`;
                }
                
                // Check if diagnostic access point
                if (response.type.toLowerCase() === 'diag') {
                    consoleManager.info('Diagnostics Access Point detected.');
                    this.saveAndActivateGateway(tenantConfig, shouldSave);
                } else {
                    // Warn user about non-diagnostic access point
                    const message = shouldSave 
                        ? 'This is not a Diagnostics Access Point. Do you want to save and activate?'
                        : 'This is not a Diagnostics Access Point. Do you want to activate?';
                    
                    this.showConfirmModal(
                        message,
                        () => this.saveAndActivateGateway(tenantConfig, shouldSave),
                        () => consoleManager.warning('Activation cancelled by user')
                    );
                }
            } else {
                consoleManager.error('Unable to get CI 360 Access Point configurations');
                this.showAlertModal('Unable to get CI 360 Access Point configurations. Please check the External gateway address, Tenant Id and Client Secret.');
            }
        })
        .fail((error) => {
            consoleManager.error('Error connecting to CI 360 Access Point');
            console.error(error);
            
            let errorMsg = 'Error connecting to CI 360 Access Point. Please check the External gateway address, Tenant Id and Client Secret.';
            if (error.responseJSON && error.responseJSON.message) {
                errorMsg += `\n\n${error.responseJSON.message}`;
            }
            this.showAlertModal(errorMsg);
        });
    }

    saveAndActivateGateway(tenantConfig, shouldSave) {
        if (shouldSave) {
            consoleManager.info('Saving gateway configuration...');
            this.savedTenants.push(tenantConfig);
            this.saveTenants();
            this.updateSavedTenantsList();
            consoleManager.success(`Tenant information saved. Total: ${this.savedTenants.length}`);
            this.clearForm();
        }
        
        this.activateGateway(tenantConfig);
    }

    activateGateway(tenant) {
        consoleManager.info('Setting this tenant as the active tenant');
        consoleManager.info(`Activating Gateway: ${tenant.tenantUrl}`);
        consoleManager.info(`Activating Tenant ID: ${tenant.tenantId}`);
        this.activeTenant = tenant;
        
        // Update active configuration display
        $('#active-tenant-name').text(tenant.tenantName);
        $('#active-tenant-id').text(tenant.tenantId);
        $('#active-gateway-url').text(tenant.tenantUrl);
        this.activeConfigDisplay.show();

        // Update top bar with tenant ID and agent name
        if (window.infoManager) {
            infoManager.updateTenantId(tenant.tenantId, 'connected');
            
            // Extract agent name if it exists in tenantName (format: "Name (AgentName)")
            const agentMatch = tenant.tenantName.match(/\(([^)]+)\)$/);
            const agentName = agentMatch ? agentMatch[1] : tenant.tenantName;
            infoManager.updateActiveAgent(agentName, 'connected');
        }

        // Dispatch event for event stream manager
        window.dispatchEvent(new CustomEvent('tenantActivated', { detail: tenant }));

        consoleManager.success('Gateway configuration activated');
        this.showAlertModal('New gateway configuration is activated. You might want to stop and start the data collection under SAS Tag Monitor tab.');
    }

    generateToken(tenantId, clientSecret) {
        // Generate JWT token using KJUR library (jsrsasign)
        if (typeof KJUR === 'undefined') {
            consoleManager.error('KJUR library not loaded - cannot generate token');
            return null;
        }

        const header = { alg: 'HS256', typ: 'JWT' };
        const payload = { clientID: tenantId };
        const secret = btoa(clientSecret);
        
        const token = KJUR.jws.JWS.sign('HS256', JSON.stringify(header), JSON.stringify(payload), secret);
        return token;
    }

    forgetTenant() {
        const selectedId = this.savedTenantList.val();
        const tenant = this.savedTenants.find(t => t.id === selectedId);
        
        if (tenant) {
            this.showConfirmModal(
                `Are you sure you want to forget "${tenant.tenantName}"?`,
                () => {
                    this.savedTenants = this.savedTenants.filter(t => t.id !== selectedId);
                    this.saveTenants();
                    this.updateSavedTenantsList();
                    consoleManager.warning(`Tenant "${tenant.tenantName}" forgotten`);
                }
            );
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

    showConfirmModal(message, onYes, onNo) {
        const modal = new bootstrap.Modal(document.getElementById('confirmModal'));
        const bodyEl = document.getElementById('confirmModalBody');
        const yesBtn = document.getElementById('confirmModalYes');
        
        // Set message
        bodyEl.innerHTML = `<p>${message}</p>`;
        
        // Track whether Yes was clicked
        let yesClicked = false;
        
        // Remove old event listeners by cloning
        const newYesBtn = yesBtn.cloneNode(true);
        yesBtn.parentNode.replaceChild(newYesBtn, yesBtn);
        
        // Add new event listener for Yes button
        newYesBtn.addEventListener('click', () => {
            yesClicked = true;
            modal.hide();
            if (onYes) onYes();
        });
        
        // Handle No button and close - only call onNo if Yes was NOT clicked
        const handleNo = () => {
            if (!yesClicked && onNo) onNo();
        };
        
        document.getElementById('confirmModal').addEventListener('hidden.bs.modal', handleNo, { once: true });
        
        modal.show();
    }

    showAlertModal(message) {
        const modal = new bootstrap.Modal(document.getElementById('alertModal'));
        const bodyEl = document.getElementById('alertModalBody');
        
        // Set message (convert \n to <br>)
        bodyEl.innerHTML = `<p>${message.replace(/\n/g, '<br>')}</p>`;
        
        modal.show();
    }
}
