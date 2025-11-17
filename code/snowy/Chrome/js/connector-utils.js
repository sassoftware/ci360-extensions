/**
 * Connector Payload Utilities
 * Handles Z85 decoding, GZIP decompression, and JSON path extraction for connector events
 */

class Z85 {
    static encodeTable = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+^!/\\*?&<>()[]{}@%$#";

    static decodeTable = [
        0, 68, 0, 84, 83, 82, 72, 0, 75, 76, 70, 65, 0, 63, 62, 69, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 64, 0, 73, 66, 74, 71, 81, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 77, 0, 78, 67, 0, 0, 10,
        11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 79, 0, 80, 0, 0,
    ];

    static decoder = {
        decodeBytes: (string) => {
            let remainder = string.length % 5;
            let padding = 5 - (remainder === 0 ? 5 : remainder);

            for (let p = 0; p < padding; ++p) {
                string += Z85.encodeTable[Z85.encodeTable.length - 1];
            }

            let length = string.length;
            let bytes = new Uint8Array((length * 4) / 5 - padding);
            let value = 0;
            let index = 0;

            for (let i = 0; i < length; i++) {
                let code = string.charCodeAt(i) - 32;
                value = value * 85 + Z85.decodeTable[code];
                if ((i + 1) % 5 === 0) {
                    let divisor = 256 * 256 * 256;
                    while (divisor >= 1) {
                        if (index < bytes.length) {
                            bytes[index++] = Math.floor((value / divisor) % 256);
                        }
                        divisor /= 256;
                    }
                    value = 0;
                }
            }
            return bytes;
        },
    };
}

function isValidGzip(data) {
    // Check if the first two bytes are the GZIP magic number (0x1F 0x8B)
    return data[0] === 0x1f && data[1] === 0x8b;
}

// Use pako for decompression
function ungzip(bytes) {
    try {
        // Use pako to ungzip the bytes array (pako library must be loaded)
        const decompressedBytes = pako.ungzip(bytes);
        // Return the decompressed bytes as an ArrayBuffer
        return decompressedBytes.buffer;
    } catch (err) {
        console.error('Failed to ungzip data', err);
        throw err;
    }
}

function decodeZ85(z85EncodedJSON) {
    try {
        const decodedBytes = Z85.decoder.decodeBytes(z85EncodedJSON);
        
        // Check if the decoded data is valid GZIP
        if (!isValidGzip(decodedBytes)) {
            throw new Error("Decoded data is not in valid GZIP format.");
        }

        // Decompress the GZIP encoded data using pako
        const decompressedArrayBuffer = ungzip(decodedBytes);

        // Convert the decompressed ArrayBuffer into a string (UTF-8)
        const jsonString = new TextDecoder("utf-8").decode(decompressedArrayBuffer);

        console.log(`Z85 decode - input length: ${z85EncodedJSON.length}, decoded JSON length: ${jsonString.length}`);
        return jsonString;
    } catch (error) {
        console.error('Failed to deserialize z85(gzip(json-string)):', error);
        return null;
    }
}

function isJson(str) {
    try {
        JSON.parse(str);
    } catch (e) {
        return false;
    }
    return true;
}

/**
 * Extract JSON paths from parsed JSON object
 * @param {Object} obj - Parsed JSON object
 * @param {String} currentPath - Current path prefix
 * @param {Array} paths - Accumulated paths array
 */
function extractJSONPaths(obj, currentPath = '', paths = []) {
    for (const key in obj) {
        if (!obj.hasOwnProperty(key)) continue;
        
        const newPath = currentPath ? `${currentPath}.${key}` : key;
        const value = obj[key];
        
        if (value === null || value === undefined) {
            paths.push({ path: `$.${newPath}`, key, value, type: 'null' });
        } else if (Array.isArray(value)) {
            if (value.length === 0) {
                paths.push({ path: `$.${newPath}`, key, value: '[]', type: 'array' });
            } else {
                value.forEach((item, index) => {
                    const arrayPath = `${newPath}[${index}]`;
                    if (typeof item === 'object' && item !== null) {
                        extractJSONPaths(item, arrayPath, paths);
                    } else {
                        paths.push({ path: `$.${arrayPath}`, key: `[${index}]`, value: item, type: typeof item });
                    }
                });
            }
        } else if (typeof value === 'object') {
            extractJSONPaths(value, newPath, paths);
        } else {
            paths.push({ path: `$.${newPath}`, key, value, type: typeof value });
        }
    }
    
    return paths;
}

/**
 * Format connector JSON with JSON paths displayed in a table
 * @param {String} jsonString - JSON string to format
 * @returns {String} HTML formatted output with JSON paths in table
 */
function formatConnectorJSONWithPaths(jsonString) {
    try {
        const parsedJSON = JSON.parse(jsonString);
        const paths = extractJSONPaths(parsedJSON);
        
        let html = '<div class="connector-payload-viewer">';
        html += '<div class="connector-payload-header">📦 Connector Payload (Decoded & Decompressed)</div>';
        html += '<div class="connector-payload-info">💡 JSON paths (blue column) can be used in Connector Custom Body variable definitions</div>';
        
        // Table layout
        html += '<div class="connector-payload-table-wrapper">';
        html += '<table class="connector-payload-table">';
        html += '<thead>';
        html += '<tr>';
        html += '<th>JSON Path</th>';
        html += '<th>Value</th>';
        html += '</tr>';
        html += '</thead>';
        html += '<tbody>';
        
        // Add each path as a table row
        paths.forEach(item => {
            html += '<tr>';
            html += `<td class="json-path-cell">${escapeHtml(item.path)}</td>`;
            html += `<td class="json-value-cell">${escapeHtml(String(item.value))}</td>`;
            html += '</tr>';
        });
        
        html += '</tbody>';
        html += '</table>';
        html += '</div>';
        
        // Formatted JSON section
        html += '<div class="connector-payload-section">';
        html += '<div class="connector-payload-section-title">📄 Formatted JSON</div>';
        html += '<div class="connector-raw-payload">';
        html += escapeHtml(JSON.stringify(parsedJSON, null, 2));
        html += '</div>';
        html += '</div>';
        
        html += '</div>';
        return html;
    } catch (error) {
        console.error('Error formatting connector JSON:', error);
        return `<div class="connector-payload-error">Error parsing connector JSON: ${error.message}</div>`;
    }
}

function isNumberOrBoolean(str) {
    if (!str) return false;
    str = str.trim();
    
    // Check if it's a number
    if (!isNaN(str) && str !== '') {
        return true;
    }
    
    // Check if it's a boolean
    if (str.toLowerCase() === 'true' || str.toLowerCase() === 'false') {
        return true;
    }
    
    return false;
}

function escapeHtml(text) {
    if (text === null || text === undefined) return '';
    const div = document.createElement('div');
    div.textContent = String(text);
    return div.innerHTML;
}

/**
 * Process connector event and decode payload
 * @param {Object} eventJson - Event JSON object
 * @returns {Object} Processed connector data { isConnectorEvent, decodedPayload, isPayloadJson, connectorJSON, rawPayload }
 */
function processConnectorEvent(eventJson) {
    const result = {
        isConnectorEvent: false,
        decodedPayload: null,
        isPayloadJson: false,
        connectorJSON: null,
        rawPayload: null
    };
    
    if (!eventJson || !eventJson.attributes) {
        return result;
    }
    
    // Check if this is a connector event
    if (eventJson.attributes['connector-agent-proxy-event']) {
        result.isConnectorEvent = true;
        
        try {
            const connectorData = JSON.parse(eventJson.attributes['connector-agent-proxy-event']);
            
            if (connectorData.body) {
                // Store raw encoded payload
                result.rawPayload = connectorData.body;
                
                // Decode Z85 + GZIP payload
                const decoded = decodeZ85(connectorData.body);
                
                if (decoded) {
                    result.decodedPayload = decoded;
                    result.isPayloadJson = isJson(decoded);
                    
                    if (result.isPayloadJson) {
                        result.connectorJSON = JSON.parse(decoded);
                    }
                }
            }
        } catch (error) {
            console.error('Error processing connector event:', error);
        }
    }
    
    return result;
}

// Export for use in other files
if (typeof window !== 'undefined') {
    window.ConnectorUtils = {
        decodeZ85,
        isJson,
        extractJSONPaths,
        formatConnectorJSONWithPaths,
        processConnectorEvent,
        isNumberOrBoolean,
        escapeHtml
    };
}
