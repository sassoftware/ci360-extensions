# Snowy - SAS CI 360 Developer Extension

<div align="center">
  <img src="Chrome/images/snowy.png" alt="Snowy Logo" width="128">
  
  **A powerful browser extension for monitoring and troubleshooting SAS Customer Intelligence 360**
  
  [![GitHub](https://img.shields.io/badge/Chromium-Browser%20Extension-blue?logo=github)](https://github.com/sassoftware/ci360-extensions)
  
</div>

---

## 📋 Overview

Snowy is a comprehensive developer tool designed to help technical consultants and developers troubleshoot SAS CI 360 implementations. It provides real-time monitoring of CI 360 events, network traffic inspection, and event streaming capabilities directly in your browser's DevTools.

### Key Capabilities

- **Network Traffic Monitoring** - Auto-capture and inspect SAS CI 360 tag events from web pages
- **Event Stream Integration** - Act as a General Agent or Diagnostic Agent to stream events from any channel
- **Identity Profile Tracking** - Monitor identity changes and fire identity events for testing
- **Connector Payload Inspection** - Capture and analyze JSON payloads sent to Connector Endpoints
- **Real-time Analysis** - Search, filter, and view complete event JSON in developer-friendly format

---

## ✨ Features

### 🌐 SAS Tag - Web Traffic Monitoring
- Auto-captures SAS CI360 tag events in real-time
- Advanced search and filtering capabilities across all event attributes
- Click any row to view complete event JSON.
- Identity profile history with change detection
- Fire identity events (attach/detach) for testing purposes
- Visual status indicators for connection and identity states

### 📡 Agent: Event Stream
- Connect to Event Stream via CI 360 Agent framework
- Support for both General and Diagnostic Access Points
- Start/Stop controls with visual recording indicators
- Identity-based filtering for Diagnostic Access Points
- Automatic payload inspection for Connector-associated Access Points
- JSON path calculation for easy Connector configuration

### ⚙️ Settings & Configuration
- **Agent Configuration**: Store and manage multiple Access Point credentials
- **Theme & Display**: Light/dark mode with automatic system preference detection
- **Flexible UI**: Toggle header visibility options for customized workspace

### 🎨 Modern User Interface
- Responsive design with Bootstrap 5
- Clean, intuitive tabbed interface
- Real-time console with log level filtering (info, success, warning, error)
- Color-coded status indicators and visual feedback
- Smooth animations and transitions

---

## 🚀 Installation

### Chrome

1. **Download the Extension**
   ```bash
   # Clone or download the repository
   git clone https://github.com/sassoftware/ci360-extensions.git
   cd ci360-extensions/snowy/Chrome
   ```

2. **Load in Chrome**
   - Open Chrome and navigate to `chrome://extensions/`
   - Enable **Developer Mode** (toggle in top-right corner)
   - Click **Load unpacked**
   - Select the `code > snowy > Chrome` folder from the downloaded repository
   - The extension icon will appear in your browser toolbar

3. **Configure Extension** _(Optional)_
   - Click **Details** on the Snowy extension
   - Set **Site access** to "On click" (recommended)
   - Enable **Allow in incognito** if you test in incognito mode

### Microsoft Edge

1. **Download the Extension**
   ```bash
   # Clone or download the repository
   git clone https://github.com/sassoftware/ci360-extensions.git
   cd ci360-extensions/snowy/Chrome
   ```

2. **Load in Edge**
   - Open Edge and navigate to `edge://extensions/`
   - Enable **Developer Mode** (toggle in bottom-left corner)
   - Click **Load unpacked**
   - Select the `code > snowy > Chrome` folder from the downloaded repository
   - The extension icon will appear in your browser toolbar

3. **Configure Extension** _(Optional)_
   - Click **Details** on the Snowy extension
   - Set **Site access** to "On click" (recommended)
   - Enable **Allow in InPrivate** if you test in InPrivate mode

---

## 📖 Usage Guide

### Monitoring Network Traffic from CI 360 Tag

1. Navigate to the website you want to inspect
2. Open **Developer Tools** (`F12` or `Ctrl+Shift+I` / `Cmd+Option+I`)
3. Select the **CI 360** tab in DevTools
4. Switch to the **SAS Tag: Web Traffic** tab within Snowy
5. Browse your website - all CI 360 network traffic will be captured automatically
6. Use the search box to filter events by any attribute
7. Click any row to view the complete event JSON

**Pro Tips:**
- Use column-specific search boxes for targeted filtering
- Click **Identity ID** in the header to view identity profile history
- The **Clear** button removes all captured events
- Click **Stop** to pause event capture temporarily

### Streaming Events via Agent

> ⚠️ **Warning**: When activating an Access Point in Snowy, ensure no other Agent instance is running with the same credentials to avoid conflicts.

1. Open **Developer Tools** and navigate to the **CI 360** tab
2. Click the **Settings** tab (gear icon in top navigation)
3. Enter your Access Point credentials:
   - **Gateway URL**: Your CI 360 gateway (e.g., `https://extapigwservice-<geo>.ci360.sas.com`)
   - **Tenant ID**: Your CI 360 tenant identifier
   - **Client Secret**: Access Point client secret
4. Click **Save & Activate** (or **Activate without saving** for one-time use)
5. Switch to the **Agent: Event Stream** tab
6. Click **Start** to begin streaming events
7. For Diagnostic Access Points, select identity filter when prompted

**Saved Configurations:**
- Previously saved credentials appear in the dropdown
- Click **Activate** to quickly reuse saved configurations
- Use **Forget** to remove saved credentials

### Identity Profile Management

1. Capture some network events to detect an identity
2. Click the **Identity ID** in the header bar
3. The Identity Profile panel opens showing:
   - Current identity information
   - Change history with timestamps
   - Identity event firing controls

**Fire Identity Events:**
- Select identity type (login_id, customer_id, subject_id)
- Enter identity value
- Choose whether to obfuscate
- Click **Attach Identity** or **Detach Identity**
- Events are fired via CI360 JavaScript API on the inspected page

---

## 🔄 Updating Snowy

Since Snowy is distributed as an unpacked extension, follow these steps to update:

### Manual Update Process

1. **Download Latest Version**
   ```bash
   # Pull latest changes if using git
   cd ci360-extensions/snowy
   git pull origin main
   
   # Or download the latest ZIP from GitHub
   ```

2. **Reload Extension**
   - Open `chrome://extensions/` or `edge://extensions/`
   - Find the Snowy extension
   - Click the **Reload** button or icon
   - Refresh any open DevTools panels

### Automatic Update Checking

- Snowy automatically checks for updates on startup
- Click **Check for Updates** in the Info tab for manual check
- Update notifications appear when new versions are available
- Download link directs to the GitHub repository

---

## 🔒 Security Notice

> **⚠️ CRITICAL SECURITY WARNINGS**

### Credential Safety
- **NEVER** use credentials from production Access Points
- **NEVER** use Access Points integrated with external systems (Connectors, DMPs, etc.)
- **ONLY** use Diagnostic Access Point credentials for monitoring purposes
- All credentials are stored **unencrypted** in browser's localStorage

### Data Privacy
- **DO NOT** use real customer identity values from production environments
- Identity events fired through Snowy are sent to your CI 360 tenant
- Be mindful of data privacy regulations (GDPR, CCPA, etc.)

### User Responsibility
You are solely responsible for:
- Securing your Access Point credentials
- Controlling access to saved configurations
- Ensuring compliance with your organization's security policies
- Understanding the impact of firing test events

---

## ℹ️ Important Disclaimers

### Purpose & Scope
- Snowy is a **developer tool** for troubleshooting CI 360 implementations
- Designed for **technical consultants** and **developers**
- Not intended for end-user demonstrations or production monitoring

### Data Accuracy
- Displayed data is captured in **real-time from browser context**
- May not reflect the **exact data processed** by SAS CI 360 servers
- CI 360 performs additional processing (enrichment, validation, etc.) after data capture
- For authoritative data verification, use:
  - Official SAS CI 360 APIs
  - CI 360 Reports and Dashboards  
  - Certified SAS CI 360 Agent streaming

### Support & Warranty
- This is a **community project**, not an official SAS product
- **NOT covered** by SAS support agreements
- Provided **"as-is"** without warranties of any kind
- Community support via GitHub Issues

### Impact on Production
- Activating Event Stream in Snowy **temporarily breaks** any integration using that Access Point
- **Always deactivate** Snowy's Agent before reconnecting production systems
- Use dedicated test/development Access Points

---

## 🛠️ Technical Details

### Architecture
- **Frontend**: Vanilla JavaScript, jQuery, Bootstrap 5
- **Chrome DevTools Integration**: Chrome Extension Manifest V3
- **Data Tables**: DataTables.js for advanced table features
- **Theming**: CSS custom properties with light/dark mode support
- **Storage**: Browser localStorage for settings and credentials

### Browser Compatibility
- ✅ Google Chrome (90+)
- ✅ Microsoft Edge (90+)
- ✅ Chromium-based browsers

### Key Technologies
- Bootstrap Icons for UI iconography
- WebSocket for Event Stream connections
- Chrome DevTools Protocol for network monitoring
- Offcanvas panels for side-panel interfaces

---

## 📚 Additional Resources

- [SAS CI 360 Documentation](https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintwlcm/home.htm)
- [SAS CI 360 Community](https://communities.sas.com/t5/SAS-Communities-Library/Using-Snowy-to-View-Events-in-Customer-Intelligence-360/ta-p/936270)
- [GitHub Repository](https://github.com/sassoftware/ci360-extensions)


<div align="center">
  
**Built with ❤️ for the SAS CI 360 Developer Community**

</div> 