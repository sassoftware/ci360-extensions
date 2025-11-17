// Initialize Application
document.addEventListener('DOMContentLoaded', () => {
    try {
        // Initialize managers
        window.consoleManager = new ConsoleManager();
        window.themeManager = new ThemeManager();
        window.versionManager = new VersionManager();
        window.tabManager = new TabManager();
        window.displayManager = new DisplayManager();
        window.infoManager = new InfoManager();
        window.tenantConfigManager = new TenantConfigManager();
        window.networkTableManager = new NetworkTableManager();
        window.eventStreamTableManager = new EventStreamTableManager();
        
        // Log initialization
        window.consoleManager.info('All managers initialized successfully');
        if (window.displayManager.identityId) {
            window.consoleManager.success(`Identity ID: ${window.displayManager.identityId}`);
        }
        
        // Add recording animation to network tab after DOM is fully ready
        setTimeout(() => {
            const networkTab = $('[data-tab="network"]');
            if (window.networkTableManager.isRecording) {
                networkTab.addClass('recording-active');
            }
        }, 100);
        
        // Check for Updates button handler
        $('#check-updates-btn').on('click', () => {
            if (window.versionManager) {
                window.consoleManager.info('Checking for updates...');
                window.versionManager.manualCheck = true;
                window.versionManager.checkForUpdates();
            }
        });
        
    } catch (error) {
        console.error('Error initializing application:', error);
    }
});
