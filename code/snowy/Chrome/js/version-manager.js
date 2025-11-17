class VersionManager {
    constructor() {
        this.currentVersion = null;
        this.remoteVersion = null;
        this.versionCheckUrl = 'https://static.cidemo.sas.com/snowy/version-remote.json';
        this.displayName = 'Version Manager';
        this.manualCheck = false;
        
        this.init();
    }
    
    init() {
        // Try to get current version from Chrome runtime manifest
        if (chrome && chrome.runtime && chrome.runtime.getManifest()) {
            this.currentVersion = chrome.runtime.getManifest().version;
            consoleManager?.info(`${this.displayName}: Current version ${this.currentVersion} (from Chrome runtime)`);
            this.checkForUpdates();
        } else {
            // Fallback: Read from manifest.json file
            consoleManager?.info(`${this.displayName}: Chrome runtime not available, reading from manifest.json`);
            this.loadVersionFromFile();
        }
    }
    
    loadVersionFromFile() {
        $.ajax({
            url: 'manifest.json',
            method: 'GET',
            dataType: 'json',
            cache: false
        })
        .done((manifest) => {
            if (manifest && manifest.version) {
                this.currentVersion = manifest.version;
                consoleManager?.info(`${this.displayName}: Current version ${this.currentVersion} (from manifest.json)`);
                this.checkForUpdates();
            } else {
                consoleManager?.warning(`${this.displayName}: Could not read version from manifest.json`);
            }
        })
        .fail((error) => {
            consoleManager?.error(`${this.displayName}: Failed to load manifest.json - ${error.statusText}`);
        });
    }
    
    checkForUpdates() {
        const settings = {
            url: `${this.versionCheckUrl}?dt=${Date.now()}`,
            method: 'GET',
            timeout: 5000,
            headers: {
                'Access-Control-Allow-Origin': 'static.cidemo.sas.com'
            }
        };
        
        $.ajax(settings)
            .done((response) => {
                if (response && response.latest_version) {
                    this.remoteVersion = response.latest_version;
                    this.handleVersionCheck(response);
                }
            })
            .fail((error) => {
                consoleManager?.warning(`${this.displayName}: Unable to check for updates`);
                if (this.manualCheck) {
                    this.showNoUpdateNotification('Unable to check for updates. Please check your internet connection.');
                    this.manualCheck = false;
                }
            });
    }
    
    handleVersionCheck(remoteData) {
        if (!this.currentVersion || !remoteData.latest_version) {
            return;
        }
        
        const currentVer = parseFloat(this.currentVersion);
        const latestVer = parseFloat(remoteData.latest_version);
        
        consoleManager?.info(`${this.displayName}: Local: ${currentVer}, Remote: ${latestVer}`);
        
        if (latestVer > currentVer) {
            // New version available
            consoleManager?.success(`${this.displayName}: Update available - v${remoteData.latest_version}`);
            this.showUpdateNotification(remoteData);
        } else if (latestVer === currentVer) {
            consoleManager?.success(`${this.displayName}: You're running the latest version`);
            if (this.manualCheck) {
                this.showNoUpdateNotification(`You're running the latest version (v${this.currentVersion})`);
                this.manualCheck = false;
            }
        } else {
            consoleManager?.info(`${this.displayName}: You're running a newer version than public release`);
            if (this.manualCheck) {
                this.showNoUpdateNotification(`You're running a development version (v${this.currentVersion}) newer than the public release (v${remoteData.latest_version})`);
                this.manualCheck = false;
            }
        }
        
        // Update version info in header
        this.updateVersionDisplay(currentVer, latestVer, remoteData);
    }
    
    showUpdateNotification(remoteData) {
        // Create notification element
        const notification = $(`
            <div class="version-update-notification" id="version-notification">
                <div class="version-update-content">
                    <div class="version-update-header">
                        <i class="bi bi-gift-fill"></i>
                        <span class="version-update-title">New Version Available!</span>
                        <button class="version-update-close" id="version-notification-close">
                            <i class="bi bi-x-lg"></i>
                        </button>
                    </div>
                    <div class="version-update-body">
                        <div class="version-update-version">
                            <strong>Version ${remoteData.latest_version}</strong>
                            ${remoteData.date ? `<span class="version-update-date">(${remoteData.date})</span>` : ''}
                        </div>
                        <div class="version-update-message">${remoteData.message || 'A new version is available with improvements and bug fixes.'}</div>
                        <div class="version-update-actions">
                            <a href="https://github.com/sassoftware/ci360-extensions" target="_blank" class="btn btn-sm btn-primary">
                                <i class="bi bi-github"></i> Download from GitHub
                            </a>
                            <button class="btn btn-sm btn-outline-secondary" id="version-notification-later">
                                <i class="bi bi-clock"></i> Remind Later
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `);
        
        // Add to page
        $('body').append(notification);
        
        // Show notification with animation
        setTimeout(() => {
            notification.addClass('show');
        }, 500);
        
        // Auto-hide after 15 seconds
        const autoHideTimer = setTimeout(() => {
            this.hideNotification();
        }, 15000);
        
        // Close button
        $('#version-notification-close').on('click', () => {
            clearTimeout(autoHideTimer);
            this.hideNotification();
        });
        
        // Remind later button
        $('#version-notification-later').on('click', () => {
            clearTimeout(autoHideTimer);
            this.hideNotification();
            consoleManager?.info(`${this.displayName}: Update reminder postponed`);
        });
    }
    
    hideNotification() {
        const notification = $('#version-notification');
        notification.removeClass('show');
        setTimeout(() => {
            notification.remove();
        }, 300);
    }
    
    showNoUpdateNotification(message) {
        // Create notification element
        const notification = $(`
            <div class="version-update-notification version-no-update" id="version-notification">
                <div class="version-update-content">
                    <div class="version-update-header">
                        <i class="bi bi-check-circle-fill"></i>
                        <span class="version-update-title">Version Check</span>
                        <button class="version-update-close" id="version-notification-close">
                            <i class="bi bi-x-lg"></i>
                        </button>
                    </div>
                    <div class="version-update-body">
                        <div class="version-update-message">${message}</div>
                    </div>
                </div>
            </div>
        `);
        
        // Add to page
        $('body').append(notification);
        
        // Show notification with animation
        setTimeout(() => {
            notification.addClass('show');
        }, 100);
        
        // Auto-hide after 5 seconds
        const autoHideTimer = setTimeout(() => {
            this.hideNotification();
        }, 5000);
        
        // Close button
        $('#version-notification-close').on('click', () => {
            clearTimeout(autoHideTimer);
            this.hideNotification();
        });
    }
    
    updateVersionDisplay(currentVer, latestVer, remoteData) {
        // Add version info to header if element exists
        const versionDisplay = $('#version-display');
        if (versionDisplay.length) {
            if (latestVer > currentVer) {
                versionDisplay.html(`
                    <i class="bi bi-brilliance version-icon spin-icon"></i>
                    <span class="version-current">v${currentVer}</span>
                    <i class="bi bi-arrow-right version-arrow"></i>
                    <span class="version-latest blink-text">v${latestVer}</span>
                `);
                versionDisplay.addClass('update-available');
            } else {
                versionDisplay.html(`
                    <i class="bi bi-check-circle version-icon"></i>
                    <span class="version-current">v${currentVer}</span>
                `);
                versionDisplay.removeClass('update-available');
            }
        }
    }
}
