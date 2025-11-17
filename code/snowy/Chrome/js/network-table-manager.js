class NetworkTableManager {
    constructor() {
        this.rowCounter = 0;
        this.dataTable = null;
        this.isRecording = true; // Start with recording enabled
        this.devToolsAvailable = true; // Track DevTools API availability
        this.metadataCache = {}; // Cache for task metadata
        this.displayName = 'SAS Tag - Web Traffic'; // Display name for console messages
        this.quickInfoConfig = CONFIG.QUICK_INFO_CONFIG; // Load Quick Info configuration
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
            consoleManager.info(`${this.displayName}: Table cleared`);
        }
    }
    
    toggleRecording() {
        // Don't allow toggling if DevTools is not available
        if (!this.devToolsAvailable) {
            window.consoleManager?.error('Network monitoring unavailable - Chrome DevTools API not accessible');
            return;
        }
        
        this.isRecording = !this.isRecording;
        const btn = $('#network-toggle-btn');
        const networkTab = $('[data-tab="network"]');
        
        if (this.isRecording) {
            btn.removeClass('btn-success').addClass('btn-warning');
            btn.attr('data-recording', 'true');
            btn.attr('title', 'Stop recording');
            btn.html('<i class="bi bi-pause-fill"></i> Stop');
            networkTab.addClass('recording-active');
            consoleManager.success(`${this.displayName}: Recording started`);
        } else {
            btn.removeClass('btn-warning').addClass('btn-success');
            btn.attr('data-recording', 'false');
            btn.attr('title', 'Start recording');
            btn.html('<i class="bi bi-play-fill"></i> Start');
            networkTab.removeClass('recording-active');
            consoleManager.warning(`${this.displayName}: Recording stopped`);
        }
    }
    
    initializeTable() {
        // Initialize DataTables
        this.dataTable = $('#networkTable').DataTable({
            dom: '<"top-controls"<"network-toolbar-container">Bf>rtip',
            pageLength: 50,
            lengthChange: false, // Hide page length dropdown
            order: [[7, "desc"]], // Sort by Req# descending (newest first)
            language: {
                search: "_INPUT_",
                searchPlaceholder: "Search...",
            },
            buttons: [
                {
                    extend: 'copyHtml5',
                    text: '<i class="bi bi-clipboard"></i> Copy',
                    titleAttr: 'Copy to clipboard',
                    className: 'btn btn-sm btn-secondary',
                    exportOptions: {
                        columns: [1, 2, 3, 4, 5, 6, 7] // Include event_json (column 2)
                    }
                },
                {
                    extend: 'excelHtml5',
                    text: '<i class="bi bi-file-earmark-excel"></i> Excel',
                    titleAttr: 'Export to Excel',
                    className: 'btn btn-sm btn-success',
                    title: 'Snowy_Network_Events',
                    exportOptions: {
                        columns: [1, 2, 3, 4, 5, 6, 7] // Include event_json (column 2)
                    }
                },
                {
                    extend: 'csvHtml5',
                    text: '<i class="bi bi-filetype-csv"></i> CSV',
                    titleAttr: 'Export to CSV',
                    className: 'btn btn-sm btn-info',
                    title: 'Snowy_Network_Events',
                    exportOptions: {
                        columns: [1, 2, 3, 4, 5, 6, 7] // Include event_json (column 2)
                    }
                }
            ],
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
            session: eventData.session || '-',
            visitor: eventData.visitor || '-',
            row_num: this.rowCounter
        };
        
        // Add row at the top (index 0)
        this.dataTable.row.add(rowData).draw(false);
        
        // Update Identity ID in top bar if datahub_id is present and not empty
        if (eventData.datahub_id && eventData.datahub_id !== '-' && window.infoManager) {
            window.infoManager.updateIdentityId(eventData.datahub_id, 'connected');
        }
        
        // Update profile manager with identity information
        if (window.profileManager) {
            const profileData = {
                ...eventData,
                row_num: this.rowCounter,
                timestamp: timestamp  // Pass original timestamp, let profile manager handle conversion
            };
            console.log('Network Table: Sending to profile manager', profileData);
            window.profileManager.checkAndUpdateProfile(profileData);
        }
        
        // Log to console
        window.consoleManager?.info(`${this.displayName}: Event captured: ${eventData.event}`);
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
                            <div class="detail-value-content">${this.formatValueWithMetadata(key1, value1)}</div>
                        </div>
                        ${key2 ? `
                            <div class="detail-cell detail-key">${this.escapeHtml(key2)}</div>
                            <div class="detail-cell detail-value">
                                <div class="detail-value-content">${this.formatValueWithMetadata(key2, value2)}</div>
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
    
    formatValueWithMetadata(key, value) {
        const baseValue = this.formatValue(value);
        
        // Check if this is a task_id field
        if (key === 'task_id' && value && typeof value === 'string') {
            const metaId = `meta-${value}-${Date.now()}`;
            
            // Fetch task metadata
            this.fetchTaskMetadata(value, metaId);
            
            return `${baseValue} <span id="${metaId}" class="text-warning" style="cursor:pointer;font-size:0.65rem;">(Loading...)</span>`;
        }
        
        return baseValue;
    }
    
    fetchTaskMetadata(taskId, htmlId) {
        // Check cache first
        if (this.metadataCache[taskId]) {
            this.displayTaskMetadata(this.metadataCache[taskId], htmlId);
            return;
        }
        
        // Check if we have an active tenant configuration
        if (!window.tenantConfigManager?.activeTenant) {
            setTimeout(() => {
                $(`#${htmlId}`).html('(No active tenant)');
                $(`#${htmlId}`).css('cursor', 'default');
            }, 100);
            return;
        }
        
        const tenant = window.tenantConfigManager.activeTenant;
        
        // Check if we have a token
        if (!tenant.token) {
            setTimeout(() => {
                $(`#${htmlId}`).html('(Token not available)');
                $(`#${htmlId}`).css('cursor', 'default');
            }, 100);
            return;
        }
        
        const url = `https://${tenant.tenantUrl}/marketingDesign/tasks/${taskId}`;
        
        $.ajax({
            url: url,
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${tenant.token}`
            },
            timeout: 5000
        })
        .done((response) => {
            this.metadataCache[taskId] = response;
            this.displayTaskMetadata(response, htmlId);
        })
        .fail((error) => {
            consoleManager.error(`Failed to fetch task metadata for ${taskId}`);
            setTimeout(() => {
                $(`#${htmlId}`).html('(Unable to fetch)');
                $(`#${htmlId}`).removeClass('text-warning').addClass('text-danger');
            }, 100);
        });
    }
    
    displayTaskMetadata(taskData, htmlId) {
        const displayText = `(Task: ${taskData.name} - Click for details)`;
        setTimeout(() => {
            const $element = $(`#${htmlId}`);
            $element.html(displayText);
            $element.removeClass('text-warning').addClass('text-info');
            $element.css('cursor', 'pointer');
            $element.off('click').on('click', () => {
                this.showTaskDetailsModal(taskData);
            });
        }, 100);
    }
    
    showTaskDetailsModal(taskData) {
        const modal = new bootstrap.Modal(document.getElementById('alertModal'));
        const bodyEl = document.getElementById('alertModalBody');
        const titleEl = document.getElementById('alertModalLabel');
        const modalDialog = document.querySelector('#alertModal .modal-dialog');
        
        // Make modal wider
        modalDialog.style.maxWidth = '70%';
        
        titleEl.textContent = 'Task Details';
        
        // Create a table-like layout similar to expanded row
        const fields = [
            { key: 'Task Name', value: taskData.name },
            { key: 'Task Code', value: taskData.taskCode },
            { key: 'Task ID', value: taskData.taskId || taskData.id || taskData.task_id },
            { key: 'State', value: taskData.state },
            { key: 'Channel', value: taskData.channel },
            { key: 'Task Type', value: taskData.taskType },
            { key: 'Number of Spots', value: taskData.numSpots },
            { key: 'Priority', value: taskData.priority },
            { key: 'Delivery Type', value: taskData.deliveryType },
            { key: 'Business Context', value: taskData.businessContextName },
            { key: 'Last Modified By', value: taskData.lastModifiedBy },
            { key: 'Last Modified', value: taskData.lastModifiedTimeStamp },
            { key: 'Last Published By', value: taskData.lastPublishedBy },
            { key: 'Last Published', value: taskData.lastPublishedTimeStamp },
            { key: 'Version', value: taskData.version },
            { key: 'Trigger Criteria', value: taskData.triggerCriteria }
        ];
        
        let rows = '';
        for (let i = 0; i < fields.length; i += 2) {
            const field1 = fields[i];
            const field2 = i + 1 < fields.length ? fields[i + 1] : null;
            
            // Skip row if both fields are empty
            if (!field1.value && (!field2 || !field2.value)) continue;
            
            rows += `
                <div class="detail-row">
                    <div class="detail-cell detail-key">${this.escapeHtml(field1.key)}</div>
                    <div class="detail-cell detail-value">
                        <div class="detail-value-content">${field1.value ? this.escapeHtml(String(field1.value)) : '<span class="text-muted">-</span>'}</div>
                    </div>
                    ${field2 ? `
                        <div class="detail-cell detail-key">${this.escapeHtml(field2.key)}</div>
                        <div class="detail-cell detail-value">
                            <div class="detail-value-content">${field2.value ? this.escapeHtml(String(field2.value)) : '<span class="text-muted">-</span>'}</div>
                        </div>
                    ` : '<div class="detail-cell"></div><div class="detail-cell"></div>'}
                </div>
            `;
        }
        
        const details = `
            <div class="detail-content-wrapper">
                <div class="detail-table">
                    ${rows}
                </div>
                <div class="raw-json-section" style="margin-top: 1rem;">
                    <div class="raw-json-header">Raw JSON:</div>
                    <div class="raw-json">${this.escapeHtml(JSON.stringify(taskData, null, 2))}</div>
                </div>
            </div>
        `;
        
        bodyEl.innerHTML = details;
        modal.show();
        
        // Reset modal width when closed
        document.getElementById('alertModal').addEventListener('hidden.bs.modal', () => {
            modalDialog.style.maxWidth = '';
        }, { once: true });
    }
    
    getQuickInfo(eventData) {
        // Get event name from eventData
        const eventName = eventData.event || eventData.eventname || '';
        
        // Find matching config key (case-insensitive)
        let fieldPaths = null;
        const eventNameLower = eventName.toLowerCase();
        
        for (const configKey in this.quickInfoConfig) {
            if (configKey.toLowerCase() === eventNameLower) {
                fieldPaths = this.quickInfoConfig[configKey];
                break;
            }
        }
        
        // If no match found, use DEFAULT
        if (!fieldPaths) {
            fieldPaths = this.quickInfoConfig['DEFAULT'];
        }
        
        const values = [];
        
        // Extract values from configured field paths
        for (const fieldName of fieldPaths) {
            const value = eventData[fieldName];
            if (value && value !== '' && value !== '-' && value !== 'null' && value !== 'undefined') {
                // Truncate very long values
                const strValue = String(value);
                const truncated = strValue.length > 100 ? strValue.substring(0, 97) + '...' : strValue;
                values.push(truncated);
            }
        }
        
        // If we found values, join them with delimiter
        if (values.length > 0) {
            return values.join(' | ');
        }
        
        // Legacy fallback logic if config doesn't produce results
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
        
        return eventName || '-';
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
            
            // Enable Identity Profile since network monitoring is available
            this.enableIdentityProfile();
            
            if (window.consoleManager) {
                window.consoleManager.success(`${this.displayName}: Listener initialized`);
            }
        } else {
            // DevTools API not available - disable recording
            this.isRecording = false;
            this.devToolsAvailable = false; // Track that DevTools is not available
            
            // Update UI to reflect disabled state
            const toggleBtn = $('#network-toggle-btn');
            toggleBtn.removeClass('btn-warning').addClass('btn-secondary');
            toggleBtn.attr('disabled', true);
            toggleBtn.attr('title', 'Network monitoring unavailable - DevTools API not accessible');
            toggleBtn.html('<i class="bi bi-x-circle"></i> Unavailable');
            
            // Remove recording animation from tab
            const networkTab = $('[data-tab="network"]');
            networkTab.removeClass('recording-active');
            
            // Disable Identity Profile offcanvas
            this.disableIdentityProfile();
            
            if (window.consoleManager) {
                window.consoleManager.warning(`${this.displayName}: Chrome DevTools API not available - monitoring disabled`);
            }
        }
    }
    
    disableIdentityProfile() {
        // Disable the identity clickable area
        const identityClickable = $('.identity-clickable');
        identityClickable.addClass('identity-disabled');
        identityClickable.attr('title', 'Identity profile unavailable - Network monitoring is disabled');
        identityClickable.removeAttr('data-bs-toggle');
        identityClickable.removeAttr('data-bs-target');
        identityClickable.attr('role', '');
        identityClickable.css('cursor', 'not-allowed');
        
        // Update identity text to show disabled state
        $('#identity-text').text('Unavailable').removeClass('text-connected').addClass('text-muted');
        $('#identity-status').removeClass('status-connected').addClass('status-disconnected');
    }
    
    enableIdentityProfile() {
        // Enable the identity clickable area
        const identityClickable = $('.identity-clickable');
        identityClickable.removeClass('identity-disabled');
        identityClickable.attr('title', 'Click to view identity profile history');
        identityClickable.attr('data-bs-toggle', 'offcanvas');
        identityClickable.attr('data-bs-target', '#identityOffcanvas');
        identityClickable.attr('role', 'button');
        identityClickable.css('cursor', 'pointer');
        
        // Reset identity text
        $('#identity-text').text('Not detected').removeClass('text-muted').addClass('text-disconnected');
        $('#identity-status').addClass('status-disconnected');
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
        
        // Extract tenant ID from URL
        const tenantId = this.extractTenantId(request.request.url);
        if (tenantId) {
            this.updateNetworkTenantId(tenantId);
        }
        
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
        
        // Extract tenant ID from URL
        const tenantId = this.extractTenantId(request.request.url);
        if (tenantId) {
            this.updateNetworkTenantId(tenantId);
        }
        
        this.addRow(params);
    }
    
    /**
     * Extract tenant ID from CI360 URL
     * Patterns: /t/e/{tenantId} or /t/s/d/{tenantId}/{sessionId}
     */
    extractTenantId(url) {
        // Pattern 1: /t/e/{tenantId}
        const pattern1 = /\/t\/e\/([a-f0-9]{24})/;
        const match1 = url.match(pattern1);
        if (match1) {
            return match1[1];
        }
        
        // Pattern 2: /t/s/d/{tenantId}/{sessionId}
        const pattern2 = /\/t\/s\/d\/([a-f0-9]{24})/;
        const match2 = url.match(pattern2);
        if (match2) {
            return match2[1];
        }
        
        return null;
    }
    
    /**
     * Update tenant ID from network traffic and check for mismatch
     */
    updateNetworkTenantId(tenantId) {
        if (!window.infoManager) return;
        
        // Check if there's an active tenant from agent activation
        const activeTenant = window.tenantConfigManager?.activeTenant;
        
        if (activeTenant && activeTenant.tenantId) {
            // Compare network tenant ID with activated agent tenant ID
            if (tenantId !== activeTenant.tenantId) {
                // Mismatch detected - show warning
                window.infoManager.updateTenantId(tenantId, 'warning');
                window.consoleManager?.warning(
                    `Tenant ID Mismatch! Network traffic: ${tenantId}, Active Agent: ${activeTenant.tenantId}`
                );
                
                // Show visual alert
                this.showTenantMismatchAlert(tenantId, activeTenant.tenantId);
            } else {
                // Match - show as connected
                window.infoManager.updateTenantId(tenantId, 'connected');
            }
        } else {
            // No active agent - just display the network tenant ID
            window.infoManager.updateTenantId(tenantId, 'connected');
        }
    }
    
    /**
     * Show visual alert for tenant ID mismatch
     */
    showTenantMismatchAlert(networkTenantId, agentTenantId) {
        // Create alert element if it doesn't exist
        let alertDiv = $('#tenant-mismatch-alert');
        if (alertDiv.length === 0) {
            alertDiv = $(`
                <div id="tenant-mismatch-alert" class="alert alert-warning alert-dismissible fade show position-fixed" 
                     style="top: 60px; right: 20px; z-index: 9999; max-width: 400px; box-shadow: 0 4px 6px rgba(0,0,0,0.3);">
                    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
                    <div class="d-flex align-items-start">
                        <i class="bi bi-exclamation-triangle-fill text-warning me-2" style="font-size: 1.5rem;"></i>
                        <div>
                            <h6 class="alert-heading mb-2">Tenant ID Mismatch!</h6>
                            <p class="mb-1"><strong>Network Traffic:</strong></p>
                            <p class="mb-2 font-monospace small">${networkTenantId}</p>
                            <p class="mb-1"><strong>Active Agent:</strong></p>
                            <p class="mb-0 font-monospace small">${agentTenantId}</p>
                        </div>
                    </div>
                </div>
            `);
            $('body').append(alertDiv);
            
            // Auto-dismiss after 10 seconds
            setTimeout(() => {
                alertDiv.fadeOut(() => alertDiv.remove());
            }, 10000);
        }
    }
}
