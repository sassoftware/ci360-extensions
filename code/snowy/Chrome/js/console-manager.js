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
        
        // Filter state - all enabled by default
        this.filters = {
            info: true,
            success: true,
            warning: true,
            error: true
        };
        
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
            // Don't toggle if clicking buttons
            if (e.target.closest('button') || e.target.closest('.console-filters')) return;
            this.toggleCollapse();
        });

        // Clear console
        this.consoleClear.addEventListener('click', (e) => {
            e.stopPropagation();
            this.clear();
        });
        
        // Filter buttons
        const filterButtons = document.querySelectorAll('.console-filters button');
        filterButtons.forEach(button => {
            button.addEventListener('click', (e) => {
                e.stopPropagation();
                const filter = button.dataset.filter;
                this.toggleFilter(filter, button);
            });
        });
    }
    
    toggleFilter(filterType, button) {
        this.filters[filterType] = !this.filters[filterType];
        button.classList.toggle('active', this.filters[filterType]);
        this.applyFilters();
    }
    
    applyFilters() {
        const logs = this.consoleContent.querySelectorAll('.console-log');
        logs.forEach(log => {
            let shouldShow = false;
            
            // Check which filter matches this log
            for (const [type, enabled] of Object.entries(this.filters)) {
                if (log.classList.contains(type) && enabled) {
                    shouldShow = true;
                    break;
                }
            }
            
            log.style.display = shouldShow ? 'flex' : 'none';
        });
        
        // Update visible count
        this.updateVisibleCount();
    }
    
    updateVisibleCount() {
        const visibleLogs = this.consoleContent.querySelectorAll('.console-log[style="display: flex;"], .console-log:not([style*="display: none"])');
        this.consoleCount.textContent = visibleLogs.length;
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
        
        // Apply filter immediately
        logEntry.style.display = this.filters[type] ? 'flex' : 'none';
        
        // Insert at the top instead of bottom
        this.consoleContent.insertBefore(logEntry, this.consoleContent.firstChild);
        this.logCount++;
        
        // Update count to show only visible logs
        this.updateVisibleCount();
        
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
        
        // Also log to browser console
        console.log(`[Snowy Console] ${message}`);
        
        // No need to scroll since latest is at top
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
