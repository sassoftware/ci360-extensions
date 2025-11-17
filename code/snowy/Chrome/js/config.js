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
        identityId: 'identityId',
        hasVisitedBefore: 'hasVisitedBefore'
    },
    // Event Stream Quick Info Configuration
    // Maps event names to arrays of field paths (dot notation) for displaying Quick Info
    // Fields are tried in order; first non-empty value is used
    // This configuration is shared by both Network Table and Event Stream Table
    QUICK_INFO_CONFIG: {
        'Load': ['page_path', 'event_channel'],
        'PageView': ['page_title','page_path', 'event_channel'],
        'Click': ['anchor_href','targetInnerText','target_text', 'target_id', 'page_path'],
        'FormSubmit': ['form_name', 'form_id', 'page_path'],
        'FormAbandonment': ['form_name', 'form_id', 'page_path'],
        'VideoPlay': ['video_name', 'video_id'],
        'VideoComplete': ['video_name', 'video_id'],
        'Download': ['download_url', 'download_name'],
        'EmailOpen': ['subject_id', 'campaign_id'],
        'EmailClick': ['subject_id', 'campaign_id'],
        'Search': ['search_term', 'page_path'],
        'ProductView': ['product_name', 'product_id'],
        'AddToCart': ['product_name', 'product_id'],
        'RemoveFromCart': ['product_name', 'product_id'],
        'Purchase': ['order_id', 'total_amount'],
        'connector-agent-proxy-event': ['body', 'event_channel'],
        // Default fallback for any event not specifically configured
        'DEFAULT': ['event_channel', 'domain']
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
            return defaultValue;
        }
        return stored === 'true';
    },
    
    formatTimestamp() {
        const now = new Date();
        const hours = String(now.getHours()).padStart(2, '0');
        const minutes = String(now.getMinutes()).padStart(2, '0');
        const seconds = String(now.getSeconds()).padStart(2, '0');
        return `${hours}:${minutes}:${seconds}`;
    },
    
    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
};
