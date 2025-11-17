// Snowy CI360 Monitor - Tab Manager

class TabManager {
    constructor() {
        this.activeTab = this.determineInitialTab();
        this.setupEventListeners();
    }

    determineInitialTab() {
        // Check if this is the first time user is opening Snowy
        const hasVisitedBefore = Utils.getStorageBoolean(CONFIG.STORAGE_KEYS.hasVisitedBefore, false);
        
        if (!hasVisitedBefore) {
            // First time user - show Info tab
            Utils.setStorageItem(CONFIG.STORAGE_KEYS.hasVisitedBefore, 'true');
            return 'info';
        }
        
        // Returning user - show Network tab
        return 'network';
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
        
        // Activate the initial tab after DOM is ready
        setTimeout(() => {
            this.switchTab(this.activeTab);
        }, 100);
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
