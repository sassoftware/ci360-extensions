class EventStreamTableManager {
    constructor() {
        this.table = null;
        this.websocket = null;
        this.isConnected = false;
        this.firstMessage = true;
        this.activeTenant = null;
        this.initialized = false;
        this.metadataCache = {}; // Cache for task metadata
        this.diagFilterApplied = false; // Track if diag filter has been applied
        this.displayName = 'Agent: Event Stream'; // Display name for console messages
        
        // Critical: Use exact same defaults as legacy
        this.DIAG_VERSION = 'diag21(2508)';
        this.SDK_VERSION = 'sdk22(2508)';
        this.LookupSDK_version_from_server = true;
        
        // Quick Info Configuration - loaded from CONFIG (shared with Network Table)
        this.quickInfoConfig = CONFIG.QUICK_INFO_CONFIG;
        
        // Load versions from remote server
        this.loadVersionsFromServer();
        
        this.setupEventListeners();
        this.setupTabActivation();
    }

    loadVersionsFromServer() {
        if (!this.LookupSDK_version_from_server) {
            return;
        }
        
        const settings = {
            url: 'https://static.cidemo.sas.com/snowy/version-remote.json?dt=' + Date.now(),
            method: 'GET',
            timeout: 5000,
            headers: {
                'Access-Control-Allow-Origin': 'static.cidemo.sas.com'
            }
        };
        
        $.ajax(settings)
            .done((response) => {
                if (response && response.DIAG_VERSION && response.SDK_VERSION) {
                    this.DIAG_VERSION = response.DIAG_VERSION;
                    this.SDK_VERSION = response.SDK_VERSION;
                    consoleManager?.info(`Versions loaded from server - DIAG: ${this.DIAG_VERSION}, SDK: ${this.SDK_VERSION}`);
                }
            })
            .fail((error) => {
                consoleManager?.warning('Unable to load versions from server, using defaults');
            });
    }

    setupTabActivation() {
        // Initialize table when Event Stream tab is first activated
        const eventsTab = $('[data-tab="events"]');
        
        eventsTab.on('click', () => {
            if (!this.initialized) {
                setTimeout(() => {
                    this.initializeTable();
                    this.initialized = true;
                }, 100);
            } else {
                consoleManager.info(`${this.displayName}: Already initialized`);
            }
        });
        
        // Also try to initialize immediately if we're already on the events tab
        if ($('#events-content').hasClass('active')) {
            consoleManager.info(`${this.displayName}: Tab already active, initializing now`);
            setTimeout(() => {
                if (!this.initialized) {
                    this.initializeTable();
                    this.initialized = true;
                }
            }, 300);
        }
    }

    initializeTable() {
        consoleManager.info(`${this.displayName}: Initializing table...`);
        
        if ($('#eventStreamTable').length === 0) {
            consoleManager.error(`${this.displayName}: Table element not found`);
            return;
        }

        if ($.fn.DataTable.isDataTable('#eventStreamTable')) {
            consoleManager.warning(`${this.displayName}: Table already initialized`);
            return;
        }

        this.table = $('#eventStreamTable').DataTable({
            dom: '<"top-controls"<"network-toolbar-container">Bf>rtip',
            pageLength: 50,
            deferRender: true,
            order: [[5, 'desc']], // Order by timestamp descending
            language: {
                search: '_INPUT_',
                searchPlaceholder: 'Search events...',
            },
            buttons: [
                {
                    extend: 'copyHtml5',
                    text: '<i class="bi bi-clipboard"></i> Copy',
                    titleAttr: 'Copy to clipboard',
                    className: 'btn btn-sm btn-secondary',
                    exportOptions: {
                        columns: [1, 2, 3, 4, 5, 6, 7] // Include eventJson (column 4)
                    }
                },
                {
                    extend: 'excelHtml5',
                    text: '<i class="bi bi-file-earmark-excel"></i> Excel',
                    titleAttr: 'Export to Excel',
                    className: 'btn btn-sm btn-success',
                    title: 'Snowy_Event_Stream',
                    exportOptions: {
                        columns: [1, 2, 3, 4, 5, 6, 7] // Include eventJson (column 4)
                    }
                },
                {
                    extend: 'csvHtml5',
                    text: '<i class="bi bi-filetype-csv"></i> CSV',
                    titleAttr: 'Export to CSV',
                    className: 'btn btn-sm btn-info',
                    title: 'Snowy_Event_Stream',
                    exportOptions: {
                        columns: [1, 2, 3, 4, 5, 6, 7] // Include eventJson (column 4)
                    }
                }
            ],
            initComplete: (settings, json) => {
                // Insert toolbar into the toolbar container
                this.createToolbar();
                
                // Add column search inputs
                const api = new $.fn.dataTable.Api(settings);
                api.columns().every(function(index) {
                    const column = this;
                    
                    // Skip the first column (expand icon)
                    if (index === 0) {
                        return;
                    }
                    
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
                    className: 'details-control',
                    orderable: false,
                    data: null,
                    width: '20px',
                    defaultContent: '<i class="bi bi-chevron-right expand-icon"></i>',
                },
                { data: 'eventName', title: 'Event Name' },
                { data: 'quickInfo', title: 'Quick Info' },
                { data: 'identityId', title: 'Identity ID' },
                { data: 'eventJson', visible: false },
                { 
                    data: 'timestamp', 
                    title: 'Timestamp',
                    render: (data) => {
                        if (!data) return '-';
                        const date = new Date(parseInt(data));
                        return date.toLocaleString('en-GB', {
                            day: '2-digit',
                            month: 'short',
                            year: 'numeric',
                            hour: '2-digit',
                            minute: '2-digit',
                            second: '2-digit',
                            fractionalSecondDigits: 3
                        }).replace(',', ' :');
                    }
                },
                { data: 'sessionId', title: 'Session ID' },
                { data: 'channelUserId', title: 'Channel User ID' },
            ],
        });

        consoleManager.success(`${this.displayName}: DataTable created`);

        this.setupRowExpansion();
        
        consoleManager.success(`${this.displayName}: Table initialized`);
    }

    createToolbar() {
        const toolbar = `
            <button class="btn btn-danger btn-sm" id="event-stream-clear" title="Clear all events">
                <i class="bi bi-x-circle"></i> Clear
            </button>
            <button class="btn btn-success btn-sm" id="event-stream-toggle" title="Start event stream">
                <i class="bi bi-play-fill"></i> Start
            </button>
            <span class="badge bg-secondary ms-2" id="event-stream-status">Disconnected</span>
            <span class="badge bg-info ms-2" id="event-stream-filter" style="display: none;">
                <i class="bi bi-funnel"></i> <span id="event-stream-filter-text"></span>
            </span>
        `;
        
        // Insert into the toolbar container created by DataTables
        $('.event-stream-table-container div.network-toolbar-container').html(toolbar);
        
        // Attach event handlers
        $('#event-stream-clear').off('click').on('click', () => {
            consoleManager.info(`${this.displayName}: Clear button clicked`);
            this.clearTable();
        });
        
        $('#event-stream-toggle').off('click').on('click', () => {
            consoleManager.info(`${this.displayName}: Toggle button clicked`);
            this.toggleConnection();
        });
        
        // Disable start button if no tenant is active
        if (!this.activeTenant) {
            consoleManager.info(`${this.displayName}: No active tenant, disabling start button`);
            this.disableStartButton();
            this.updateStatus('no-agent');
        } else {
            consoleManager.info(`${this.displayName}: Active tenant found, button enabled`);
        }
        
        consoleManager.success(`${this.displayName}: Toolbar created`);
    }

    setupRowExpansion() {
        $('#eventStreamTable tbody').on('click', 'td.details-control', (e) => {
            const tr = $(e.target).closest('tr');
            const row = this.table.row(tr);

            if (row.child.isShown()) {
                row.child.hide();
                tr.removeClass('shown');
            } else {
                row.child(this.formatRowDetails(row.data())).show();
                tr.addClass('shown');
            }
        });
    }

    formatRowDetails(data) {
        if (!data) return 'No data available';

        let eventData;
        let rawJson;
        try {
            if (typeof data.eventJson === 'string') {
                rawJson = data.eventJson; // Store original untouched string
                eventData = JSON.parse(data.eventJson);
            } else {
                rawJson = JSON.stringify(data.eventJson); // Store as is, no formatting
                eventData = data.eventJson;
            }
        } catch (e) {
            eventData = data.eventJson;
            rawJson = String(data.eventJson);
        }

        // Check if this is a connector event and process it
        const connectorData = window.ConnectorUtils ? window.ConnectorUtils.processConnectorEvent(eventData) : null;
        const isConnectorEvent = connectorData && connectorData.isConnectorEvent;

        // Flatten the entire event payload
        const flattenedData = this.flattenObject(eventData);
        
        // Sort the flattened keys alphabetically
        const sortedKeys = Object.keys(flattenedData).sort();
        
        // Create detail HTML with flattened, sorted data in 4-column tabular format
        let detailHtml = '<div class="detail-content-wrapper">';
        detailHtml += '<div class="detail-table">';
        
        // Add each flattened property as a row with 4 columns (2 key-value pairs per row)
        for (let i = 0; i < sortedKeys.length; i += 2) {
            detailHtml += '<div class="detail-row">';
            
            // First key-value pair (columns 1 and 2)
            const key1 = sortedKeys[i];
            const value1 = flattenedData[key1];
            detailHtml += `<div class="detail-cell detail-key">${this.escapeHtml(key1)}</div>`;
            detailHtml += `<div class="detail-cell detail-value">`;
            detailHtml += `<div class="detail-value-content">${this.formatValueWithMetadata(key1, value1)}</div>`;
            detailHtml += '</div>';
            
            // Second key-value pair (columns 3 and 4) - if exists
            if (i + 1 < sortedKeys.length) {
                const key2 = sortedKeys[i + 1];
                const value2 = flattenedData[key2];
                detailHtml += `<div class="detail-cell detail-key">${this.escapeHtml(key2)}</div>`;
                detailHtml += `<div class="detail-cell detail-value">`;
                detailHtml += `<div class="detail-value-content">${this.formatValueWithMetadata(key2, value2)}</div>`;
                detailHtml += '</div>';
            } else {
                // Fill empty cells if odd number of properties
                detailHtml += '<div class="detail-cell detail-key"></div>';
                detailHtml += '<div class="detail-cell detail-value"></div>';
            }
            
            detailHtml += '</div>';
        }

        detailHtml += '</div>'; // Close detail-table

        // Connector Payload Section - if this is a connector event with decoded payload
        if (isConnectorEvent && connectorData.decodedPayload) {
            if (connectorData.isPayloadJson) {
                // Display formatted JSON with paths in table format
                detailHtml += window.ConnectorUtils.formatConnectorJSONWithPaths(connectorData.decodedPayload);
            } else {
                // Display raw decoded payload
                detailHtml += '<div class="connector-payload-viewer">';
                detailHtml += '<div class="connector-payload-header">📦 Connector Payload (Decoded)</div>';
                detailHtml += '<div class="connector-payload-content">';
                detailHtml += '<pre>' + this.escapeHtml(connectorData.decodedPayload) + '</pre>';
                detailHtml += '</div></div>';
            }
        }

        // Raw JSON section - untouched, no formatting, no indentation
        detailHtml += '<div class="raw-json-section">';
        detailHtml += '<div class="raw-json-header">RAW EVENT JSON</div>';
        detailHtml += '<pre class="raw-json">' + this.escapeHtml(rawJson) + '</pre>';
        detailHtml += '</div>';
        detailHtml += '</div>'; // Close detail-content-wrapper

        return detailHtml;
    }

    // Flatten nested objects into dot-notation paths
    flattenObject(obj, prefix = '') {
        const flattened = {};
        
        for (const key in obj) {
            if (!obj.hasOwnProperty(key)) continue;
            
            const value = obj[key];
            const newKey = prefix ? `${prefix}.${key}` : key;
            
            if (value === null || value === undefined) {
                flattened[newKey] = value;
            } else if (typeof value === 'object' && !Array.isArray(value)) {
                // Recursively flatten nested objects
                Object.assign(flattened, this.flattenObject(value, newKey));
            } else if (Array.isArray(value)) {
                // Handle arrays
                if (value.length === 0) {
                    flattened[newKey] = '[]';
                } else {
                    value.forEach((item, index) => {
                        const arrayKey = `${newKey}[${index}]`;
                        if (typeof item === 'object' && item !== null) {
                            Object.assign(flattened, this.flattenObject(item, arrayKey));
                        } else {
                            flattened[arrayKey] = item;
                        }
                    });
                }
            } else {
                flattened[newKey] = value;
            }
        }
        
        return flattened;
    }

    escapeHtml(text) {
        if (text === null || text === undefined) return '-';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }

    setupEventListeners() {
        // Listen for tenant activation
        window.addEventListener('tenantActivated', (e) => {
            this.activeTenant = e.detail;
            this.clearFilterBadge(); // Clear filter when new tenant is activated
            this.updateStatus('disconnected'); // Update to show disconnected (not "no agent")
            this.enableStartButton(); // Enable start button when tenant is activated
            consoleManager?.info(`${this.displayName}: Tenant available for connection`);
        });

        window.addEventListener('tenantDeactivated', () => {
            this.disconnect();
            this.activeTenant = null;
            this.updateStatus('no-agent'); // Show "No Agent Activated" status
            this.disableStartButton(); // Disable start button when tenant is deactivated
        });
    }

    toggleConnection() {
        if (this.isConnected) {
            this.disconnect();
        } else {
            this.connect();
        }
    }

    connect() {
        if (!this.activeTenant) {
            consoleManager.error(`${this.displayName}: No active tenant configured`);
            modalManager.showAlert('No Active Tenant', 'Please configure and activate a tenant in Settings before connecting to the event stream.');
            return;
        }

        const wsUrl = `wss://${this.activeTenant.tenantUrl}/marketingGateway/agent/stream`;
        consoleManager.info(`${this.displayName}: Connecting to ${wsUrl}`);

        try {
            this.websocket = new WebSocket(wsUrl);
            this.firstMessage = true;

            this.websocket.onerror = (event) => {
                consoleManager.error(`${this.displayName}: WebSocket error occurred`);
                this.updateStatus('error');
            };

            this.websocket.onopen = (event) => {
                consoleManager.success(`${this.displayName}: Connected to CI360`);
                this.isConnected = true;
                this.updateStatus('connected');

                // Send authentication
                const version = this.activeTenant.type === 'diag' ? this.DIAG_VERSION : this.SDK_VERSION;
                const authMessage = `${this.activeTenant.token}\n${version}`;
                this.websocket.send(authMessage);
                consoleManager.info(`${this.displayName}: Sent auth with version: ${version}`);
            };

            this.websocket.onmessage = (event) => {
                if (this.firstMessage) {
                    this.firstMessage = false;
                    consoleManager.success(`${this.displayName}: Ready to receive events`);
                    
                    // Check if diagnostics agent needs filters
                    if (this.activeTenant.type === 'diag') {
                        consoleManager.info(`${this.displayName}: Diagnostics agent - showing filter dialog`);
                        this.showDiagnosticsFilterDialog();
                    }
                } else {
                    if (event.data && event.data.includes('ping')) {
                        consoleManager.info(`${this.displayName}: Received ping`);
                    }
                }

                // Process event data
                if (event.data && (event.data.includes('rowKey') || event.data.includes('connector-agent-proxy-event'))) {
                    try {
                        const eventJson = JSON.parse(event.data);
                        this.addEvent(eventJson);
                    } catch (e) {
                        consoleManager.error(`${this.displayName}: Failed to parse event data`);
                    }
                }

                // Send acknowledgment
                this.send('ack');
            };

            this.websocket.onclose = (event) => {
                if(event && event.reason){
                    if(event.reason.toString().toLocaleLowerCase().includes("exception") || event.reason.toString().toLocaleLowerCase().includes("error"))
                    {
                        consoleManager.warning(`${this.displayName}: Connection closed with error - ${event.reason}`);
                        this.updateStatus('error');
                        return;
                    }
                    else
                    {
                        consoleManager.info(`${this.displayName}: Reason from server - ${event.reason}`);
                    }

                } 
                consoleManager.info(`${this.displayName}: Connection closed`);
                this.isConnected = false;
                this.updateStatus('disconnected');
            };

        } catch (error) {
            consoleManager.error(`${this.displayName}: Failed to connect - ${error.message}`);
            this.updateStatus('error');
        }
    }

    disconnect() {
        if (this.websocket) {
            if (this.activeTenant && this.activeTenant.type === 'diag') {
                this.send('DIAGNOSTIC_AGENT: {"operation":"clear"}');
            }
            this.websocket.close();
            this.websocket = null;
        }
        this.isConnected = false;
        this.diagFilterApplied = false;
        this.clearFilterBadge();
        this.updateStatus('disconnected');
        consoleManager.info(`${this.displayName}: Disconnected`);
    }

    send(message) {
        if (this.websocket && this.websocket.readyState === WebSocket.OPEN) {
            this.websocket.send(message);
        }
    }

    updateStatus(status) {
        const statusBadge = $('#event-stream-status');
        const toggleBtn = $('#event-stream-toggle');
        const eventsTab = $('[data-tab="events"]');

        switch (status) {
            case 'connected':
                statusBadge.removeClass('bg-secondary bg-danger bg-warning').addClass('bg-success');
                statusBadge.text('Connected');
                toggleBtn.removeClass('btn-success').addClass('btn-warning');
                toggleBtn.html('<i class="bi bi-pause-fill"></i> Stop');
                toggleBtn.attr('title', 'Stop event stream');
                // Add spinning animation to events tab icon
                eventsTab.addClass('recording-active');
                break;
            case 'disconnected':
                statusBadge.removeClass('bg-success bg-danger bg-warning').addClass('bg-secondary');
                statusBadge.text('Disconnected');
                toggleBtn.removeClass('btn-warning').addClass('btn-success');
                toggleBtn.html('<i class="bi bi-play-fill"></i> Start');
                toggleBtn.attr('title', 'Start event stream');
                // Remove spinning animation from events tab icon
                eventsTab.removeClass('recording-active');
                break;
            case 'no-agent':
                statusBadge.removeClass('bg-success bg-danger bg-secondary').addClass('bg-warning');
                statusBadge.text('No Agent Activated');
                toggleBtn.removeClass('btn-warning').addClass('btn-success');
                toggleBtn.html('<i class="bi bi-play-fill"></i> Start');
                toggleBtn.attr('title', 'Start event stream');
                // Remove spinning animation from events tab icon
                eventsTab.removeClass('recording-active');
                break;
            case 'error':
                statusBadge.removeClass('bg-success bg-secondary bg-warning').addClass('bg-danger');
                statusBadge.text('Error');
                toggleBtn.removeClass('btn-warning').addClass('btn-success');
                toggleBtn.html('<i class="bi bi-play-fill"></i> Start');
                // Remove spinning animation from events tab icon
                eventsTab.removeClass('recording-active');
                break;
        }
    }

    addEvent(eventJson) {
        if (!eventJson || !eventJson.attributes) {
            return;
        }

        const attrs = eventJson.attributes;
        
        // Extract data with fallbacks
        const eventName = attrs.eventName || '';
        const identityId = attrs.datahub_id || '';
        const sessionId = attrs.sessionId || '';
        const channelUserId = attrs.channel_user_id || '';
        const timestamp = attrs.timestamp || Date.now().toString();
        
        // Generate quick info using configuration
        const quickInfo = this.generateQuickInfo(eventName, eventJson);

        const row = {
            eventName,
            quickInfo,
            identityId,
            eventJson: JSON.stringify(eventJson),
            timestamp,
            sessionId,
            channelUserId,
        };

        this.table.row.add(row).draw(false);
        consoleManager.info(`${this.displayName}: Event received - ${eventName || 'Unknown'}`);

        // Update identity ID in info panel if present
        if (identityId && identityId !== '-') {
            if (window.infoManager) {
                window.infoManager.updateIdentityId(identityId);
            }
        }
    }

    /**
     * Generate quick info based on event name and configured field mappings
     * @param {string} eventName - The name of the event
     * @param {object} eventJson - The full event JSON object
     * @returns {string} - Formatted quick info string
     */
    generateQuickInfo(eventName, eventJson) {
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
        
        // Extract values from configured field paths (need to prefix with 'attributes.' for event stream)
        for (const fieldName of fieldPaths) {
            const path = `attributes.${fieldName}`;
            const value = this.getNestedValue(eventJson, path);
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
        
        // Final fallback - use event name or 'Unknown'
        return eventName || 'Unknown';
    }

    /**
     * Get nested value from object using dot notation path
     * @param {object} obj - The object to search
     * @param {string} path - Dot notation path (e.g., 'attributes.page_path')
     * @returns {*} - The value at the path, or null if not found
     */
    getNestedValue(obj, path) {
        if (!obj || !path) return null;
        
        const keys = path.split('.');
        let current = obj;
        
        for (const key of keys) {
            if (current && typeof current === 'object' && key in current) {
                current = current[key];
            } else {
                return null;
            }
        }
        
        return current;
    }

    clearTable() {
        this.table.clear().draw();
        consoleManager.info(`${this.displayName}: Table cleared`);
    }

    formatValueWithMetadata(key, value) {
        const baseValue = this.escapeHtml(String(value));
        
        // Check if this is a task_id or taskId field
        if ((key === 'task_id' || key === 'taskId' || key.endsWith('.task_id') || key.endsWith('.taskId')) && value && typeof value === 'string') {
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
            consoleManager.error(`${this.displayName}: Failed to fetch task metadata for ${taskId}`);
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
        
        let html = '<div class="detail-content-wrapper">';
        html += '<div class="detail-table">';
        
        for (let i = 0; i < fields.length; i += 2) {
            html += '<div class="detail-row">';
            
            // First pair
            html += `<div class="detail-cell detail-key">${fields[i].key}</div>`;
            html += `<div class="detail-cell detail-value"><div class="detail-value-content">${this.escapeHtml(fields[i].value || '-')}</div></div>`;
            
            // Second pair
            if (i + 1 < fields.length) {
                html += `<div class="detail-cell detail-key">${fields[i + 1].key}</div>`;
                html += `<div class="detail-cell detail-value"><div class="detail-value-content">${this.escapeHtml(fields[i + 1].value || '-')}</div></div>`;
            } else {
                html += '<div class="detail-cell detail-key"></div>';
                html += '<div class="detail-cell detail-value"></div>';
            }
            
            html += '</div>';
        }
        
        html += '</div>'; // Close detail-table
        
        // Raw JSON section
        html += '<div class="raw-json-section" style="margin-top: 1rem;">';
        html += '<div class="raw-json-header">RAW JSON</div>';
        html += `<pre class="raw-json">${this.escapeHtml(JSON.stringify(taskData, null, 2))}</pre>`;
        html += '</div>';
        html += '</div>'; // Close detail-content-wrapper
        
        bodyEl.innerHTML = html;
        modal.show();
        
        // Reset modal width when closed
        document.getElementById('alertModal').addEventListener('hidden.bs.modal', () => {
            modalDialog.style.maxWidth = '';
        }, { once: true });
    }

    showDiagnosticsFilterDialog() {
        const modal = new bootstrap.Modal(document.getElementById('diagFilterModal'));
        
        // Get network traffic data if available
        const hasNetworkData = window.networkTableManager && window.networkTableManager.dataTable && window.networkTableManager.dataTable.rows().count() > 0;
        
        let datahubId = '';
        let sessionId = '';
        let channelUserId = '';
        
        if (hasNetworkData) {
            // Get the most recent row from network table (row 0 is newest)
            const firstRow = window.networkTableManager.dataTable.row(0).data();
            if (firstRow) {
                datahubId = firstRow.datahub_id || '';
                sessionId = firstRow.session || '';
                channelUserId = firstRow.visitor || '';
            }
        }
        
        // Populate values and enable/disable options
        $('#diagFilterDatahubIdValue').text(datahubId || 'Not detected');
        $('#diagFilterSessionValue').text(sessionId || 'Not detected');
        $('#diagFilterChannelIdValue').text(channelUserId || 'Not detected');
        
        // Enable/disable auto-detect options based on whether values exist
        $('#diagFilterDatahubId').prop('disabled', !datahubId || datahubId === '-');
        $('#diagFilterSession').prop('disabled', !sessionId || sessionId === '-');
        $('#diagFilterChannelId').prop('disabled', !channelUserId || channelUserId === '-');
        
        // Default selection - prefer datahub_id, then session, then channel_id, else custom
        if (datahubId && datahubId !== '-') {
            $('#diagFilterDatahubId').prop('checked', true);
            $('#diagFilterCustomInputs').hide();
        } else if (sessionId && sessionId !== '-') {
            $('#diagFilterSession').prop('checked', true);
            $('#diagFilterCustomInputs').hide();
        } else if (channelUserId && channelUserId !== '-') {
            $('#diagFilterChannelId').prop('checked', true);
            $('#diagFilterCustomInputs').hide();
        } else {
            // No network data available - force custom input
            $('#diagFilterCustom').prop('checked', true);
            $('#diagFilterCustomInputs').show();
            // Disable all auto-detect options
            $('#diagFilterDatahubId').prop('disabled', true);
            $('#diagFilterSession').prop('disabled', true);
            $('#diagFilterChannelId').prop('disabled', true);
        }
        
        // Handle custom filter toggle
        $('input[name="diagFilterType"]').off('change').on('change', function() {
            if ($(this).val() === 'custom') {
                $('#diagFilterCustomInputs').show();
            } else {
                $('#diagFilterCustomInputs').hide();
            }
        });
        
        // Handle apply button
        $('#diagFilterApply').off('click').on('click', () => {
            this.applyDiagnosticsFilter();
        });
        
        modal.show();
    }
    
    applyDiagnosticsFilter() {
        const selectedFilter = $('input[name="diagFilterType"]:checked').val();
        let filterType = '';
        let filterValue = '';
        
        if (!selectedFilter) {
            consoleManager.error(`${this.displayName}: Please select a filter option`);
            return;
        }
        
        if (selectedFilter === 'custom') {
            filterType = $('#diagFilterCustomType').val();
            filterValue = $('#diagFilterCustomValue').val().trim();
            
            if (!filterValue) {
                consoleManager.error(`${this.displayName}: Please enter a filter value`);
                $('#diagFilterCustomValue').addClass('is-invalid');
                return;
            }
            $('#diagFilterCustomValue').removeClass('is-invalid');
        } else if (selectedFilter === 'datahub_id') {
            filterType = 'datahub_id';
            filterValue = $('#diagFilterDatahubIdValue').text();
        } else if (selectedFilter === 'session_id') {
            filterType = 'session_id';
            filterValue = $('#diagFilterSessionValue').text();
        } else if (selectedFilter === 'channel_id') {
            filterType = 'channel_id';
            filterValue = $('#diagFilterChannelIdValue').text();
        }
        
        if (!filterValue || filterValue === 'Not detected') {
            consoleManager.error(`${this.displayName}: Invalid filter value`);
            return;
        }
        
        // Send filter to WebSocket
        const filterMessage = `DIAGNOSTIC_AGENT: {"operation":"filter","attribute":"${filterType}", "value":"${filterValue}"}`;
        const showMessage = 'DIAGNOSTIC_AGENT: {"operation":"show"}';
        
        consoleManager.info(`${this.displayName}: Applying filter ${filterType} = ${filterValue}`);
        this.send(filterMessage);
        this.send(showMessage);
        
        this.diagFilterApplied = true;
        
        // Update filter badge
        this.updateFilterBadge(filterType, filterValue);
        
        // Close modal
        bootstrap.Modal.getInstance(document.getElementById('diagFilterModal')).hide();
        
        consoleManager.success(`${this.displayName}: Diagnostics filter applied: ${filterType} = ${filterValue}`);
    }

    updateFilterBadge(filterType, filterValue) {
        const badge = $('#event-stream-filter');
        const badgeText = $('#event-stream-filter-text');
        
        if (filterType && filterValue) {
            // Format filter type for display
            const displayType = filterType.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
            badgeText.text(`${displayType}: ${filterValue}`);
            badge.show();
        } else {
            badge.hide();
        }
    }

    clearFilterBadge() {
        $('#event-stream-filter').hide();
        $('#event-stream-filter-text').text('');
    }

    enableStartButton() {
        const toggleBtn = $('#event-stream-toggle');
        if (toggleBtn.length) {
            toggleBtn.prop('disabled', false);
            toggleBtn.attr('title', 'Start event stream');
        }
    }

    disableStartButton() {
        const toggleBtn = $('#event-stream-toggle');
        if (toggleBtn.length) {
            toggleBtn.prop('disabled', true);
        }
    }
}
