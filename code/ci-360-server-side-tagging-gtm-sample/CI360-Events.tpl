___INFO___

{
  "type": "TAG",
  "id": "cvt_temp_public_id",
  "version": 1,
  "securityGroups": [],
  "displayName": "[CI 360] Events API v2",
  "description": "Posts a single event to the CI 360 Server-Side Events API. Supports all identity types (_ci360_id, visitor_id, customer_id, login_id, subject_id). Builds correct nested payload per event type.",
  "containerContexts": [
    "SERVER"
  ]
}


___TEMPLATE_PARAMETERS___

[
  {
    "type": "TEXT",
    "name": "endpoint",
    "displayName": "CI 360 Endpoint",
    "simpleValueType": true,
    "help": "Required HTTPS origin. Configure matching send_http permissions before use.",
    "valueValidators": [
      {
        "type": "NON_EMPTY",
        "errorMessage": "Endpoint is required."
      }
    ]
  },
  {
    "type": "TEXT",
    "name": "tenantId",
    "displayName": "External Tenant ID",
    "simpleValueType": true,
    "help": "Required. 24-character hex tenant ID.",
    "valueValidators": [
      {
        "type": "NON_EMPTY",
        "errorMessage": "Tenant ID is required."
      }
    ]
  },
  {
    "type": "TEXT",
    "name": "authHeader",
    "displayName": "Authorization Header",
    "simpleValueType": true,
    "help": "Required full Bearer credential. Placeholders are rejected."
  },
  {
    "type": "TEXT",
    "name": "identityType",
    "displayName": "Identity Type",
    "simpleValueType": true,
    "help": "_ci360_id (default) | visitor_id | customer_id | login_id | subject_id",
    "valueValidators": [
      {
        "type": "NON_EMPTY",
        "errorMessage": "Identity Type is required."
      }
    ]
  },
  {
    "type": "TEXT",
    "name": "identityValue",
    "displayName": "Identity Value",
    "simpleValueType": true,
    "help": "Required non-empty string for known identity types; otherwise cookie lookup is used."
  },
  {
    "type": "TEXT",
    "name": "cookieName",
    "displayName": "Cookie Name Override",
    "simpleValueType": true,
    "help": "Optional. Default: _ci360_id or _SI_VID_1.<tenantId> for visitor_id."
  },
  {
    "type": "TEXT",
    "name": "eventName",
    "displayName": "Event Name",
    "simpleValueType": true,
    "help": "Required. load | click | impressionViewable | clickThrough | custom | etc.",
    "valueValidators": [
      {
        "type": "NON_EMPTY",
        "errorMessage": "Event Name is required."
      }
    ]
  },
  {
    "type": "TEXT",
    "name": "apiEventKey",
    "displayName": "API Event Key",
    "simpleValueType": true,
    "help": "Required for custom, cart, cartAction, internalSearch, productView, promotion. Must match CI 360 UI event key."
  },
  {
    "type": "TEXT",
    "name": "uri",
    "displayName": "URI",
    "simpleValueType": true,
    "help": "Required. Page URL for the event.",
    "valueValidators": [
      {
        "type": "NON_EMPTY",
        "errorMessage": "URI is required."
      }
    ]
  },
  {
    "type": "TEXT",
    "name": "sessionId",
    "displayName": "Session ID (augmented)",
    "simpleValueType": true,
    "help": "Optional. From ci360('info').sid. Merges event into existing SAS tag session."
  },
  {
    "type": "TEXT",
    "name": "loadId",
    "displayName": "Load ID (augmented)",
    "simpleValueType": true,
    "help": "Optional. Required when sessionId is set. From ci360('info').loadId."
  },
  {
    "type": "TEXT",
    "name": "pageTitle",
    "displayName": "Page Title",
    "simpleValueType": true,
    "help": "load events only. -> properties.pageTitle"
  },
  {
    "type": "TEXT",
    "name": "pageReferrer",
    "displayName": "Page Referrer",
    "simpleValueType": true,
    "help": "load events only. -> properties.pageReferrer"
  },
  {
    "type": "TEXT",
    "name": "ipAddress",
    "displayName": "IP Address",
    "simpleValueType": true,
    "help": "load events only. -> properties.ip. For geo-IP lookup."
  },
  {
    "type": "TEXT",
    "name": "userAgent",
    "displayName": "User Agent",
    "simpleValueType": true,
    "help": "load events only. -> properties.userAgent. For device detection."
  },
  {
    "type": "TEXT",
    "name": "targetUri",
    "displayName": "Target URI",
    "simpleValueType": true,
    "help": "click events only. -> click.targetUri"
  },
  {
    "type": "TEXT",
    "name": "targetInnerText",
    "displayName": "Target Inner Text",
    "simpleValueType": true,
    "help": "click events only. -> click.targetInnerText"
  },
  {
    "type": "TEXT",
    "name": "spotKey",
    "displayName": "Spot Key",
    "simpleValueType": true,
    "help": "impressionViewable / clickThrough only."
  },
  {
    "type": "TEXT",
    "name": "taskId",
    "displayName": "Task ID",
    "simpleValueType": true,
    "help": "impressionViewable / clickThrough only. From Content Request API response."
  },
  {
    "type": "TEXT",
    "name": "creativeId",
    "displayName": "Creative ID",
    "simpleValueType": true,
    "help": "impressionViewable / clickThrough only. From Content Request API response."
  },
  {
    "type": "TEXT",
    "name": "variantId",
    "displayName": "Variant ID",
    "simpleValueType": true,
    "help": "impressionViewable / clickThrough only. From Content Request API response."
  },
  {
    "type": "TEXT",
    "name": "requestId",
    "displayName": "Request ID",
    "simpleValueType": true,
    "help": "impressionViewable / clickThrough only. From Content Request API response."
  },
  {
    "type": "SIMPLE_TABLE",
    "name": "additionalProperties",
    "displayName": "Additional Properties",
    "simpleTableColumns": [
      {
        "defaultValue": "",
        "displayName": "Name",
        "name": "name",
        "type": "TEXT"
      },
      {
        "defaultValue": "",
        "displayName": "Value",
        "name": "value",
        "type": "TEXT"
      }
    ],
    "help": "For custom events. Flat key-value pairs added to the payload."
  },
  {
    "type": "TEXT",
    "name": "consentState",
    "displayName": "CI 360 consent state",
    "simpleValueType": true,
    "help": "Must equal granted. Map only from the approved website consent decision."
  }
]


___SANDBOXED_JS_FOR_SERVER___

const sendHttpRequest = require('sendHttpRequest');
const getCookieValues = require('getCookieValues');
const setCookie = require('setCookie');
const logToConsole = require('logToConsole');
const encodeUriComponent = require('encodeUriComponent');
const parseUrl = require('parseUrl');
const getType = require('getType');
const queryPermission = require('queryPermission');
const JSON = require('JSON');

const identityType = data.identityType || '_ci360_id';
const COOKIE_MAX_AGE = 63072000;
var endpoint = '';
var finished = false;

function text(value) {
  return getType(value) === 'string' ? value.trim() : '';
}
function finish(ok, reason) {
  if (finished) { return; }
  finished = true;
  // Never log identity-bearing URLs, credentials, payloads or response bodies.
  logToConsole('[CI 360]', reason);
  if (ok) { data.gtmOnSuccess(); } else { data.gtmOnFailure(); }
}
function isHexTenant(value) {
  if (getType(value) !== 'string' || value.length !== 24) { return false; }
  for (var i = 0; i < value.length; i++) {
    if ('0123456789abcdefABCDEF'.indexOf(value.charAt(i)) < 0) { return false; }
  }
  return true;
}
function validateConfig() {
  if (data.consentState !== 'granted') {
    finish(false, 'Skipped: CI 360 consent not granted'); return false;
  }
  var url = parseUrl(text(data.endpoint));
  if (!url || url.protocol !== 'https:' || url.username || url.password ||
      url.search || url.hash || (url.pathname && url.pathname !== '/')) {
    finish(false, 'Invalid endpoint: configure an HTTPS origin'); return false;
  }
  endpoint = url.origin;
  if (!isHexTenant(data.tenantId)) {
    finish(false, 'Invalid or unconfigured tenant ID'); return false;
  }
  var auth = text(data.authHeader);
  if (auth.indexOf('Bearer ') !== 0 || auth.length <= 7 ||
      auth.indexOf('PLACEHOLDER') >= 0 || auth.indexOf('REPLACE_') >= 0 ||
      auth.indexOf('\r') >= 0 || auth.indexOf('\n') >= 0) {
    finish(false, 'Missing or placeholder Bearer credential'); return false;
  }
  if (['_ci360_id', 'visitor_id', 'customer_id', 'login_id', 'subject_id'].indexOf(identityType) < 0) {
    finish(false, 'Unsupported identity type'); return false;
  }
  return true;
}
function headers(contentType) {
  return {'Content-Type': contentType, 'Authorization': text(data.authHeader)};
}
function identityPath(idValue) {
  return '/id_type=' + encodeUriComponent(identityType) + '/id_value=' + encodeUriComponent(idValue);
}
function request(url, options, body, onSuccess) {
  if (!queryPermission('send_http', url)) {
    finish(false, 'HTTP destination not permitted'); return;
  }
  sendHttpRequest(url, options, body).then(function(result) {
    if (result && result.statusCode >= 200 && result.statusCode < 300) {
      onSuccess(result);
    } else {
      finish(false, 'Upstream HTTP failure');
    }
  }).catch(function() {
    finish(false, 'Network, timeout or response processing failure');
  });
}
function resolveIdentity(callback) {
  var value = text(data.identityValue);
  var known = ['customer_id', 'login_id', 'subject_id'].indexOf(identityType) >= 0;
  if (known && !value) { finish(false, 'Known identity value is required'); return; }
  if (value) { callback(value); return; }
  var cookie = text(data.cookieName) ||
    (identityType === 'visitor_id' ? '_SI_VID_1.' + data.tenantId : '_ci360_id');
  if (!queryPermission('get_cookies', cookie)) {
    finish(false, 'Identity cookie is not permitted'); return;
  }
  var cookies = getCookieValues(cookie) || [];
  var stored = text(cookies[0]);
  if (stored) { callback(stored); return; }
  if (identityType !== 'visitor_id') {
    finish(false, 'Identity cookie is not available'); return;
  }
  var options = {domain: 'auto', path: '/', 'max-age': COOKIE_MAX_AGE,
    secure: true, httpOnly: true, sameSite: 'lax'};
  if (!queryPermission('set_cookies', cookie, options)) {
    finish(false, 'Visitor cookie write is not permitted'); return;
  }
  request(endpoint + '/t/events/i/' + encodeUriComponent(data.tenantId),
    {method: 'GET', headers: headers('application/json'), timeout: 5000}, '', function(result) {
      var response = JSON.parse(result.body);
      if (getType(response) !== 'object') {
        finish(false, 'Invalid visitor identity response'); return;
      }
      var id = text(response.visitorId) || text(response.visitor_id) ||
        text(response.id_value) || text(response.idValue);
      if (!id) { finish(false, 'Empty visitor identity response'); return; }
      setCookie(cookie, id, options);
      callback(id);
    });
}

const getTimestampMillis = require('getTimestampMillis');
function validateEvent() {
  var name = text(data.eventName);
  if (!name || name !== data.eventName || !text(data.uri)) { finish(false, 'Event name and URI are required'); return false; }
  if ((text(data.sessionId) && !text(data.loadId)) || (!text(data.sessionId) && text(data.loadId))) {
    finish(false, 'Session ID and load ID must be supplied together'); return false;
  }
  if (name === 'impressionViewable' || name === 'clickThrough') {
    if (!text(data.spotKey) || !text(data.taskId) || !text(data.creativeId) ||
        !text(data.variantId) || !text(data.requestId)) {
      finish(false, 'Spot response correlation fields are required'); return false;
    }
  } else if (name !== 'load' && name !== 'click') {
    if (!text(data.apiEventKey)) { finish(false, 'API event key is required'); return false; }
  }
  var rows = data.additionalProperties || [];
  if (getType(rows) !== 'array') { finish(false, 'Invalid additional properties'); return false; }
  var reserved = ['eventName', 'channel', 'clientTimestamp', 'uri', 'sessionId', 'loadId',
    'apiEventKey', 'properties', 'click', 'spotKey', 'taskId', 'creativeId', 'variantId',
    'requestId', '__proto__', 'prototype', 'constructor'];
  for (var i = 0; i < rows.length; i++) {
    if (getType(rows[i]) !== 'object' || !text(rows[i].name) ||
        reserved.indexOf(rows[i].name) >= 0 || text(rows[i].name) !== rows[i].name ||
        ['string', 'number', 'boolean'].indexOf(getType(rows[i].value)) < 0) {
      finish(false, 'Invalid or reserved additional property'); return false;
    }
  }
  return true;
}
function buildPayload() {
  var eventObj = {
    eventName: text(data.eventName),
    channel: 'web',
    clientTimestamp: getTimestampMillis(),
    uri: data.uri
  };

  if (data.sessionId) {
    eventObj.sessionId = data.sessionId;
    if (data.loadId) { eventObj.loadId = data.loadId; }
  }

  if (data.eventName === 'load') {
    var props = {};
    var hasProps = false;
    if (data.pageTitle)    { props.pageTitle    = data.pageTitle;    hasProps = true; }
    if (data.pageReferrer) { props.pageReferrer = data.pageReferrer; hasProps = true; }
    if (data.ipAddress)    { props.ip           = data.ipAddress;    hasProps = true; }
    if (data.userAgent)    { props.userAgent    = data.userAgent;    hasProps = true; }
    if (hasProps) { eventObj.properties = props; }

  } else if (data.eventName === 'click') {
    var clickObj = {};
    var hasClick = false;
    if (data.targetUri)       { clickObj.targetUri       = data.targetUri;       hasClick = true; }
    if (data.targetInnerText) { clickObj.targetInnerText = data.targetInnerText; hasClick = true; }
    if (hasClick) { eventObj.click = clickObj; }

  } else if (data.eventName === 'impressionViewable' || data.eventName === 'clickThrough') {
    if (data.spotKey)    { eventObj.spotKey    = data.spotKey; }
    if (data.taskId)     { eventObj.taskId     = data.taskId; }
    if (data.creativeId) { eventObj.creativeId = data.creativeId; }
    if (data.variantId)  { eventObj.variantId  = data.variantId; }
    if (data.requestId)  { eventObj.requestId  = data.requestId; }

  } else {
    if (data.apiEventKey) { eventObj.apiEventKey = data.apiEventKey; }
    if (data.additionalProperties) {
      for (var i = 0; i < data.additionalProperties.length; i++) {
        var row = data.additionalProperties[i];
        if (row.name) { eventObj[row.name] = row.value; }
      }
    }
  }

  return JSON.stringify(eventObj);
}
if (validateConfig() && validateEvent()) {
  resolveIdentity(function(id) {
    var url = endpoint + '/t/events/e/' + encodeUriComponent(data.tenantId) + identityPath(id);
    request(url, {method: 'POST', headers: headers('application/x-ndjson'), timeout: 5000},
      buildPayload() + '\n', function() { finish(true, 'Event request accepted'); });
  });
}


___SERVER_PERMISSIONS___

[
  {
    "instance": {
      "key": {
        "publicId": "get_cookies",
        "versionId": "1"
      },
      "param": [
        {
          "key": "cookieAccess",
          "value": {
            "type": 1,
            "string": "specific"
          }
        },
        {
          "key": "cookieNames",
          "value": {
            "type": 2,
            "listItem": [
              {
                "type": 1,
                "string": "_ci360_id"
              },
              {
                "type": 1,
                "string": "_SI_VID_1.REPLACE_WITH_YOUR_CI360_TENANT_ID"
              }
            ]
          }
        }
      ]
    },
    "isRequired": true
  },
  {
    "instance": {
      "key": {
        "publicId": "set_cookies",
        "versionId": "1"
      },
      "param": [
        {
          "key": "allowedCookies",
          "value": {
            "type": 2,
            "listItem": [
              {
                "type": 3,
                "mapKey": [
                  {
                    "type": 1,
                    "string": "name"
                  },
                  {
                    "type": 1,
                    "string": "domain"
                  },
                  {
                    "type": 1,
                    "string": "path"
                  },
                  {
                    "type": 1,
                    "string": "secure"
                  },
                  {
                    "type": 1,
                    "string": "session"
                  }
                ],
                "mapValue": [
                  {
                    "type": 1,
                    "string": "_SI_VID_1.REPLACE_WITH_YOUR_CI360_TENANT_ID"
                  },
                  {
                    "type": 1,
                    "string": "*"
                  },
                  {
                    "type": 1,
                    "string": "/"
                  },
                  {
                    "type": 1,
                    "string": "any"
                  },
                  {
                    "type": 1,
                    "string": "any"
                  }
                ]
              }
            ]
          }
        }
      ]
    },
    "isRequired": true
  },
  {
    "instance": {
      "key": {
        "publicId": "send_http",
        "versionId": "1"
      },
      "param": [
        {
          "key": "allowedUrls",
          "value": {
            "type": 1,
            "string": "specific"
          }
        },
        {
          "key": "urls",
          "value": {
            "type": 2,
            "listItem": [
              {
                "type": 1,
                "string": "https://configure-your-tenant.example.invalid/t/*"
              }
            ]
          }
        }
      ]
    },
    "isRequired": true
  },
  {
    "instance": {
      "key": {
        "publicId": "logging",
        "versionId": "1"
      },
      "param": [
        {
          "key": "environments",
          "value": {
            "type": 1,
            "string": "debug"
          }
        }
      ]
    },
    "isRequired": true
  }
]


___TESTS___

scenarios: []


___NOTES___

Reviewed portable sample. Configure endpoint permissions and consent mapping.
Content response delivery is outside this package; dependent tags ship paused.

