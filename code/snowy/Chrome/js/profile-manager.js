/**
 * Profile Manager - Manages identity profile history
 * Tracks datahub IDs, sessions, visitors, and PII information from network traffic
 */

class ProfileManager {
    constructor() {
        this.lastProfile = {
            touched: false,
            timestamp: '',
            session: '',
            visitor: '',
            datahub_id: '',
            pii: '',
            event: '',
            row_num: ''
        };
        
        this.profileHistory = [];
        this.maxHistorySize = 50; // Keep last 50 identity changes
    }

    /**
     * Check and update profile based on event data
     * @param {Object} eventData - Event data from network or event stream
     */
    checkAndUpdateProfile(eventData) {
        console.log('ProfileManager: Checking profile update', eventData);
        
        const currentProfile = {
            timestamp: eventData.timestamp || Date.now(),
            session: eventData.session || eventData.sessionId || '',
            visitor: eventData.visitor || eventData.channel_id || eventData.channelUserId || '',
            datahub_id: eventData.datahub_id || eventData.identityId || '',
            pii: '',
            event: this.formatEventName(eventData),
            row_num: eventData.row_num || ''
        };

        // Extract PII from identity events
        if (this.isIdentityEvent(eventData)) {
            currentProfile.pii = this.extractPII(eventData);
        }
        
        console.log('ProfileManager: Current profile', currentProfile);
        console.log('ProfileManager: Last profile touched?', this.lastProfile.touched);

        // Check if this is the first profile or if identity changed
        if (!this.lastProfile.touched) {
            console.log('ProfileManager: First profile - adding');
            this.lastProfile.touched = true;
            this.copyProfile(currentProfile);
            this.addProfile();
        } else {
            console.log('ProfileManager: Checking for changes...');
            // Update if session, visitor, datahub_id, or PII changed
            if (this.hasProfileChanged(currentProfile)) {
                console.log('ProfileManager: Profile changed - adding new entry');
                this.copyProfile(currentProfile);
                this.addProfile();
            } else {
                console.log('ProfileManager: No changes detected');
            }
        }
    }

    /**
     * Check if the event is an identity event
     */
    isIdentityEvent(eventData) {
        const event = eventData.event || '';
        const eventLower = event.toLowerCase();
        return eventLower === 'identityevent' || 
               event.includes('Inferred Event: attachIdentity') ||
               event.includes('Inferred Event: detachIdentity') ||
               eventLower.includes('identity');
    }

    /**
     * Extract PII information from event data
     */
    extractPII(eventData) {
        // Check for login_event_type and login_event (legacy format)
        if (eventData.login_event_type && (eventData.login_event || eventData.User_ID_Attribute)) {
            const idValue = eventData.login_event || eventData.User_ID_Attribute;
            try {
                return `${decodeURIComponent(eventData.login_event_type)}: ${decodeURIComponent(idValue)}`;
            } catch (e) {
                return `${eventData.login_event_type}: ${idValue}`;
            }
        }
        
        // Check for identities in event attributes (new format)
        if (eventData.identities && Array.isArray(eventData.identities) && eventData.identities.length > 0) {
            const identity = eventData.identities[0];
            return `${identity.type || 'unknown'}: ${identity.value || ''}`;
        }
        
        return '';
    }

    /**
     * Format event name for display
     */
    formatEventName(eventData) {
        const event = eventData.event || '';
        const eventName = eventData.eventname || eventData.event_name || eventData.eventName || '';
        return eventName ? `${event} / ${eventName}` : event;
    }

    /**
     * Check if profile has changed from last profile
     */
    hasProfileChanged(currentProfile) {
        return (currentProfile.session && currentProfile.session !== this.lastProfile.session) ||
               (currentProfile.visitor && currentProfile.visitor !== this.lastProfile.visitor) ||
               (currentProfile.datahub_id && currentProfile.datahub_id !== this.lastProfile.datahub_id) ||
               (currentProfile.pii && currentProfile.pii !== '' && currentProfile.pii !== this.lastProfile.pii);
    }

    /**
     * Copy current profile to last profile
     */
    copyProfile(profile) {
        Object.assign(this.lastProfile, profile);
    }

    /**
     * Add profile to history and update UI
     */
    addProfile() {
        console.log('ProfileManager: Adding profile to history', this.lastProfile);
        
        // Add to history array
        this.profileHistory.unshift({...this.lastProfile});
        
        // Limit history size
        if (this.profileHistory.length > this.maxHistorySize) {
            this.profileHistory.pop();
        }

        console.log('ProfileManager: History size now:', this.profileHistory.length);

        // Update current identity summary
        this.updateCurrentIdentity();
        
        // Update profile table
        this.updateProfileTable();
    }

    /**
     * Update current identity summary in offcanvas
     */
    updateCurrentIdentity() {
        console.log('ProfileManager: Updating current identity display');
        $('#current-datahub-id').text(this.lastProfile.datahub_id || '-');
        $('#current-pii').text(this.lastProfile.pii || '-');
        $('#current-event').text(this.lastProfile.event || '-');
        
        // Update header identity display
        if (this.lastProfile.datahub_id) {
            $('#identity-text').text(this.lastProfile.datahub_id);
            $('#identity-text').removeClass('text-disconnected').addClass('text-connected');
            $('#identity-status').removeClass('status-disconnected').addClass('status-connected');
        }
    }

    /**
     * Update profile table in offcanvas
     */
    updateProfileTable() {
        console.log('ProfileManager: Updating profile table, history count:', this.profileHistory.length);
        
        const tbody = $('#profileTable tbody');
        tbody.empty();
        
        // Update count badge
        $('#profile-count').text(this.profileHistory.length);
        
        if (this.profileHistory.length === 0) {
            tbody.append(`
                <tr>
                    <td colspan="5" class="text-center text-muted">
                        No identity data available
                    </td>
                </tr>
            `);
            return;
        }

        // Add rows for each profile in history
        this.profileHistory.forEach((profile, index) => {
            const row = $('<tr>');
            
            // Highlight the most recent (current) profile
            if (index === 0) {
                row.attr('title', 'Current identity');
            }
            
            // Highlight datahub ID changes
            const datahubChanged = index > 0 && profile.datahub_id !== this.profileHistory[index - 1].datahub_id;
            const datahubClass = datahubChanged ? 'datahub-changed' : '';
            
            row.append(`
                <td>${profile.row_num || '-'}</td>
                <td>${this.formatTimestamp(profile.timestamp)}</td>
                <td class="${datahubClass}">${this.escapeHtml(profile.datahub_id)}</td>
                <td>${this.escapeHtml(profile.pii)}</td>
                <td>${this.escapeHtml(profile.event)}</td>
            `);
            
            tbody.append(row);
        });
    }

    /**
     * Format timestamp for display (matching DataTable format)
     */
    formatTimestamp(timestamp) {
        if (!timestamp) return '-';
        try {
            // Handle different timestamp formats
            let date;
            if (typeof timestamp === 'number') {
                date = new Date(timestamp);
            } else if (typeof timestamp === 'string') {
                // Try to parse as ISO string or as number
                if (timestamp.includes('T') || timestamp.includes('-')) {
                    date = new Date(timestamp);
                } else {
                    date = new Date(parseInt(timestamp));
                }
            } else {
                date = new Date(timestamp);
            }
            
            if (isNaN(date.getTime())) {
                return '-';
            }
            
            // Format: DD MMM YYYY : hh:mm:ss.SSS (matching DataTable)
            const day = String(date.getDate()).padStart(2, '0');
            const month = date.toLocaleString('en-US', { month: 'short' });
            const year = date.getFullYear();
            const hours = String(date.getHours()).padStart(2, '0');
            const minutes = String(date.getMinutes()).padStart(2, '0');
            const seconds = String(date.getSeconds()).padStart(2, '0');
            const milliseconds = String(date.getMilliseconds()).padStart(3, '0');
            return `${day} ${month} ${year} : ${hours}:${minutes}:${seconds}.${milliseconds}`;
        } catch (e) {
            return '-';
        }
    }

    /**
     * Truncate text with ellipsis
     */
    truncateText(text, maxLength) {
        if (!text) return '-';
        if (text.length <= maxLength) return this.escapeHtml(text);
        return this.escapeHtml(text.substring(0, maxLength)) + '...';
    }

    /**
     * Escape HTML to prevent XSS
     */
    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    /**
     * Clear all profile data
     */
    clearProfiles() {
        this.lastProfile = {
            touched: false,
            timestamp: '',
            session: '',
            visitor: '',
            datahub_id: '',
            pii: '',
            event: '',
            row_num: ''
        };
        this.profileHistory = [];
        this.updateCurrentIdentity();
        this.updateProfileTable();
        
        // Reset header display
        $('#identity-text').text('');
        $('#identity-text').removeClass('text-connected').addClass('text-disconnected');
        $('#identity-status').removeClass('status-connected').addClass('status-disconnected');
    }
    
    /**
     * Setup event listeners
     */
    setupEventListeners() {
        // Clear profile history button
        $('#clearProfileHistory').on('click', () => {
            this.clearProfiles();
            if (window.consoleManager) {
                window.consoleManager.info('Profile Manager: Identity history cleared');
            }
        });

        // Attach Identity button
        $('#attachIdentityBtn').on('click', () => {
            this.attachIdentity();
        });

        // Detach Identity button
        $('#detachIdentityBtn').on('click', () => {
            this.detachIdentity();
        });

        // Clear validation errors on input
        $('#identityType, #identityValue, #identityObfuscate').on('change input', function() {
            $(this).removeClass('is-invalid');
        });

        // Allow Enter key on identity value field to trigger attach
        $('#identityValue').on('keypress', (e) => {
            if (e.which === 13) { // Enter key
                e.preventDefault();
                this.attachIdentity();
            }
        });
    }

    /**
     * Attach identity event via CI360 JavaScript API
     */
    attachIdentity() {
        const identityType = $('#identityType').val();
        const identityValue = $('#identityValue').val().trim();
        const shouldObfuscate = $('#identityObfuscate').val();

        // Reset validation
        $('#identityType, #identityValue, #identityObfuscate').removeClass('is-invalid');

        // Validate inputs
        let isValid = true;

        if (!identityType) {
            $('#identityType').addClass('is-invalid');
            isValid = false;
        }

        if (!identityValue) {
            $('#identityValue').addClass('is-invalid');
            isValid = false;
        }

        if (!isValid) {
            if (window.consoleManager) {
                window.consoleManager.warning('Profile Manager: Please fill in all required fields');
            }
            return;
        }

        // Build the CI360 JavaScript API call
        let jsAPI;
        if (shouldObfuscate === 'yes') {
            jsAPI = `ci360('attachIdentity', { 'loginId': '${identityValue}', 'loginEventType': '${identityType}', 'obfuscateFields': '["loginId"]' });`;
        } else {
            jsAPI = `ci360('attachIdentity', { 'loginId': '${identityValue}', 'loginEventType': '${identityType}' });`;
        }

        // Execute the API on the inspected page
        try {
            chrome.devtools.inspectedWindow.eval(jsAPI, (result, isException) => {
                if (isException) {
                    if (window.consoleManager) {
                        window.consoleManager.error(`Profile Manager: Failed to fire attach identity event - ${isException.value}`);
                    }
                } else {
                    if (window.consoleManager) {
                        window.consoleManager.success(`Profile Manager: Attach identity event fired - Type: ${identityType}, Value: ${identityValue}${shouldObfuscate === 'yes' ? ' (obfuscated)' : ''}`);
                        window.consoleManager.info(`API Call: ${jsAPI}`);
                    }

                    // Clear form
                    $('#identityValue').val('');
                    $('#identityType').val('');
                    $('#identityObfuscate').val('no');
                }
            });
        } catch (error) {
            if (window.consoleManager) {
                window.consoleManager.error(`Profile Manager: Error firing attach identity event - ${error.message}`);
            }
        }
    }

    /**
     * Detach identity event via CI360 JavaScript API
     */
    detachIdentity() {
        const jsAPI = "ci360('detachIdentity');";

        // Execute the API on the inspected page
        try {
            chrome.devtools.inspectedWindow.eval(jsAPI, (result, isException) => {
                if (isException) {
                    if (window.consoleManager) {
                        window.consoleManager.error(`Profile Manager: Failed to fire detach identity event - ${isException.value}`);
                    }
                } else {
                    if (window.consoleManager) {
                        window.consoleManager.success('Profile Manager: Detach identity event fired successfully');
                        window.consoleManager.info(`API Call: ${jsAPI}`);
                    }
                }
            });
        } catch (error) {
            if (window.consoleManager) {
                window.consoleManager.error(`Profile Manager: Error firing detach identity event - ${error.message}`);
            }
        }
    }
}

// Create global instance and attach to window
window.profileManager = new ProfileManager();

// Initialize event listeners when DOM is ready
$(document).ready(() => {
    window.profileManager.setupEventListeners();
});
