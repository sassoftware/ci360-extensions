// Snowy CI360 Monitor - Theme Manager

class ThemeManager {
    constructor() {
        // Initialize theme from localStorage or browser preference
        this.currentTheme = this.getInitialTheme();
        this.colorPreference = Utils.getStorageItem(CONFIG.STORAGE_KEYS.colorPreference, CONFIG.THEME_DEFAULTS.colorPreference);
        this.applyTheme();
        this.applyColorPreference();
        this.setupEventListeners();
    }

    getInitialTheme() {
        const savedTheme = Utils.getStorageItem(CONFIG.STORAGE_KEYS.theme);
        if (savedTheme) return savedTheme;
        
        // Check browser preference
        if (window.matchMedia?.('(prefers-color-scheme: dark)').matches) {
            return 'dark';
        }
        
        return CONFIG.THEME_DEFAULTS.theme;
    }

    applyTheme() {
        document.body.setAttribute('data-bs-theme', this.currentTheme);
        Utils.setStorageItem(CONFIG.STORAGE_KEYS.theme, this.currentTheme);
        
        // Update the theme toggle switch if it exists
        const themeToggle = document.getElementById('theme-toggle-switch');
        if (themeToggle) {
            themeToggle.checked = this.currentTheme === 'dark';
        }
    }

    applyColorPreference() {
        document.body.setAttribute('data-user-colors', this.colorPreference);
        Utils.setStorageItem(CONFIG.STORAGE_KEYS.colorPreference, this.colorPreference);
    }

    toggleTheme() {
        this.currentTheme = this.currentTheme === 'light' ? 'dark' : 'light';
        this.applyTheme();
        console.log('Theme switched to:', this.currentTheme);
    }

    setColorPreference(preference) {
        this.colorPreference = preference;
        this.applyColorPreference();
        console.log('Color preference set to:', preference);
    }

    setupEventListeners() {
        // Setup theme toggle switch
        setTimeout(() => {
            const themeToggle = document.getElementById('theme-toggle-switch');
            if (themeToggle) {
                themeToggle.checked = this.currentTheme === 'dark';
                themeToggle.addEventListener('change', (e) => {
                    this.currentTheme = e.target.checked ? 'dark' : 'light';
                    this.applyTheme();
                });
            }
        }, 100);

        // Listen for browser theme changes
        if (window.matchMedia) {
            window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
                if (!localStorage.getItem('theme')) {
                    this.currentTheme = e.matches ? 'dark' : 'light';
                    this.applyTheme();
                    // Update the switch to reflect the change
                    const themeToggle = document.getElementById('theme-toggle-switch');
                    if (themeToggle) {
                        themeToggle.checked = this.currentTheme === 'dark';
                    }
                    console.log('Browser theme changed, updated to:', this.currentTheme);
                }
            });
        }

        // Setup color preference controls
        this.setupColorPreferenceControls();
    }

    setupColorPreferenceControls() {
        // Set initial state of radio buttons and setup listeners - same pattern as DisplayManager
        setTimeout(() => {
            const currentPreferenceRadio = document.getElementById(`color-${this.colorPreference}`);
            if (currentPreferenceRadio) {
                currentPreferenceRadio.checked = true;
                console.log('Color preference radio initialized to:', this.colorPreference);
            } else {
                console.warn('Color preference radio not found:', `color-${this.colorPreference}`);
            }
            
            // Listen for color preference changes
            document.querySelectorAll('input[name="colorPreference"]').forEach(radio => {
                radio.addEventListener('change', (e) => {
                    if (e.target.checked) {
                        this.setColorPreference(e.target.value);
                    }
                });
            });
        }, 100);
    }

    // Method to cycle through color preferences for demo
    cycleColorPreferences() {
        const preferences = ['default', 'high-contrast', 'colorblind-friendly', 'soft'];
        const currentIndex = preferences.indexOf(this.colorPreference);
        const nextIndex = (currentIndex + 1) % preferences.length;
        this.setColorPreference(preferences[nextIndex]);
        return preferences[nextIndex];
    }
}
