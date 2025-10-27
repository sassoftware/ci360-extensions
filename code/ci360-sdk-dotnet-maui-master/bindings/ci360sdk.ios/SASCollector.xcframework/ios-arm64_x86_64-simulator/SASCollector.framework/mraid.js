(function() {

//// Event handling

	var Event = function(name) {
		this.name = name;
		var listeners = [];
    
		this.addListener = function(listener) {
			var i = listeners.indexOf(listener);
			if (i < 0)
				listeners.push(listener);
		};
    
		this.removeListener = function(listener) {
			if (!listener)
				listeners = [];
			else {
				var i = listeners.indexOf(listener);
				if (i < 0)
					listeners.splice(i,1);
			}
		};
    
		this.fire = function() {
			var args = Array.prototype.slice.call(arguments);
			trace("Fired event: "+this.name+"("+args.join()+")  "+listeners.length+" listeners");
			for (var i=0; i<listeners.length; i++) {
				listeners[i].apply(null, args);
			}
		};
	};


//// MRAID variables

	var VERSION = "2.0";
	var states = {
		loading: "loading",
		_default: "default", // ...default is reserved word
		expanded: "expanded",
		resized: "resized",
		hidden: "hidden"
	};
	var state = states.loading;

	var placementTypes = {
		inline: "inline",
		interstitial: "interstitial"
	};
	var placementType = placementTypes.inline;  // ...unless overridden by setReady...
	var isViewable = false;
	var isOnScreen = false;
	var isZeroSize = true;

	var events = {
		ready: new Event("ready"),
		error: new Event("error"),
		stateChange: new Event("stateChange"),
		viewableChange: new Event("viewableChange"),
		sizeChange: new Event("sizeChange")
	};

	var maxSize = { // Screen size, without incorporating orientation
		width: null,
		height: null
	};
	
	var screenSize = { // Screen size, incorporating orientation
		width: null,
		height: null
	};
	
	var defaultPosition = { // position when in default state
		x: null,
		y: null,
		width: null,
		height: null
	};
	
	var position = { // current position
		x: null,
		y: null,
		width: null,
		height: null
	};
	
	var expandProperties = {
		width: null,
		height: null,
		useCustomClose: false,
		isModal: true
	};

	var resizeProperties = {
		width: null,
		height: null,
		offsetX: 0,
		offsetY: 0,
		customClosePosition: "top-right",
		allowOffscreen: true
	};

	var orientationProperties = {
		allowOrientationChange: true,
		forceOrientation: "none"
	};

	var features = {
		SMS: false,
		tel: false,
		calendar: false,
		storePicture: false,
		inlineVideo: true
	};
	
//// Bridge for SDK-to-JS and JS-to-SDK functions

	var mraidBridge = window.mraidBridge = {};
	
	var sdkExecutionReady = false; // ...blocks SDK command execution until after sdkReady() is called
	var pendingSdkCommands = [];
	
	// tracing is null initially, prior to being ready to communicate with the SDK.
	// The SDK trace calls are queued up anyway, so that once everything is initialized, they will
	// go thru and SDK can decide for itself whether or not to display them, according to how the trace
	// option is set.  In this way, the tracing can capture everything prior to setTracing being called.
	var tracing = null;

	var trace = function(message) {
		if (tracing === false) return;
		mraidBridge.executeInSdk("trace",encodeURIComponent(message));
	};

	var traceAd = function(message) {
		if (tracing === false) return;
		mraidBridge.executeInSdk("traceAd",encodeURIComponent(message));
	};

	var traceAdCall = function() {
		if (tracing === false) return;
		var args = Array.prototype.slice.call(arguments);
		var mraidFunction = args.shift();
		var message = "Called "+mraidFunction+"("+args.join()+")";
		traceAd(message);
	};

	var traceAdCallWithReturn = function() {
		if (tracing === false) return;
		var args = Array.prototype.slice.call(arguments);
		var mraidFunction = args.shift();
		var returnValue = args.shift();
		var message = "Called "+mraidFunction+"("+args.join()+"), returning \""+returnValue+"\"";
		traceAd(message);
	};

	var error = function(mraidFunction, message) {
		trace("Error calling function "+mraidFunction+". "+message);
		events.error.fire(message, mraidFunction);
	};

	mraidBridge.error = function(mraidFunction, message) {
		error(mraidFunction,message);
	};
	
	mraidBridge.setTracing = function(newTracing) {
		tracing = newTracing;
	};

	mraidBridge.setDefaultPosition = function() {
		defaultPosition = position;
	};

	mraidBridge.setPosition = function(x,y,width,height) {
		var oldPosition = position;
		position = {x:x, y:y, width:width, height:height};

		// Fire size change if size changed AND past the "loading" state.
		var sizeChanged = (oldPosition.width != width) || (oldPosition.height != height);
		if ((state != states.loading) && sizeChanged)
			events.sizeChange.fire(width,height);

		isZeroSize = (width == 0) || (height == 0);
		mraidBridge.setIsViewable(isOnScreen); // in case size changes visibility...
	};

	mraidBridge.setState = function(newState) {
		// Fire state change if it's changing or "resize" is being re-fired.
		if ((newState != state) || (newState == states.resized)) {
			state = newState;
			events.stateChange.fire(state);
		}
	};

	mraidBridge.setIsViewable = function(newIsOnScreen) {
		// Race conditions can cause the SDK to set this visibility before the backing web view has had a chance to adopt a non-zero size.
		// This can mislead an ad listening to the viewableChange event.
		// So only consider the ad as visible if both "isOnScreen" is set and the size is non-zero.
		isOnScreen = newIsOnScreen;
		var wasViewable = isViewable;
		isViewable = (isOnScreen && !isZeroSize);
		if (isViewable != wasViewable)
			events.viewableChange.fire(isViewable);
	};

	mraidBridge.useCustomClose = function(newUseCustomClose) {
		if (newUseCustomClose != expandProperties.useCustomClose) {
			expandProperties.useCustomClose = newUseCustomClose;
			mraidBridge.executeInSdk("useCustomClose",newUseCustomClose);
		}
	};

	mraidBridge.setMaxSize = function(maxWidth,maxHeight, screenWidth,screenHeight) {
		maxSize = {width:maxWidth, height:maxHeight};
		screenSize = {width:screenWidth, height:screenHeight};
		expandProperties.width = screenWidth;
		expandProperties.height = screenHeight;
	};
	
	mraidBridge.sendToSdk = function(args) {
        var sdkCommand = "mraid://"+args.join("/");
        window.webkit.messageHandlers.mraidBridgeCommand.postMessage({cmd: args[0], args: args.slice(1, args.length)})
	};
	
	mraidBridge.executeInSdk = function() {
		var args = Array.prototype.slice.call(arguments);
		if (sdkExecutionReady) {
			sdkExecutionReady = false;
            mraidBridge.sendToSdk(args);
		} else
            pendingSdkCommands.push(args); // push array insteadof String
	};

	mraidBridge.resumeSdkExecution = function() {
		if (pendingSdkCommands.length == 0)
			sdkExecutionReady = true;
		else {
			var sdkCommand = pendingSdkCommands.shift();
			mraidBridge.sendToSdk(sdkCommand);
		}
	};

	mraidBridge.sdkExecutionDone = function() {
		this.resumeSdkExecution();
	};

	mraidBridge.sdkReady = function(customClose,isInterstitial,initialState,isVisible,
							supportSMSText,supportTelephone,supportCalendar,supportPicture) {
		expandProperties.useCustomClose = customClose;

		if (isInterstitial)
			placementType = placementTypes.interstitial;

		this.setState(initialState);
		this.setIsViewable(isVisible);

		features.sms = supportSMSText;
		features.tel = supportTelephone;
		features.calendar = supportCalendar;
		features.storePicture = supportPicture;

		events.ready.fire();
//		events.ready.fire(false,0,"SAS-MRAID"); // Hack to make the IAB "Resize with errors" and "Full page" ads log correctly.
	};
	
//// MRAID utility functions ////

	function isNumeric(n) {
		return !isNaN(parseFloat(n)) && isFinite(n);
	}

	function isBoolean(n) {
		return (n==true) || (n==false);
	}

	function validNumeric(func,properties,prop_name,required) {
		if (properties.hasOwnProperty(prop_name)) {
			if (!isNumeric(properties[prop_name])) {
				error(func, "Property \""+prop_name+"\" is not numeric.");
				return false;
			}
			return true;
	
		} else {
			if (required) {
				error(func, "Property \""+prop_name+"\" is required.");
				return false;
			}
			return true;
		}
	}
	
	function validBoolean(func,properties,prop_name,required) {
		if (properties.hasOwnProperty(prop_name)) {
			if (!isBoolean(properties[prop_name])) {
				error(func, "Property \""+prop_name+"\" is not boolean.");
				return false;
			}
			return true;
	
		} else {
			if (required) {
				error(func, "Property \""+prop_name+"\" is required.");
				return false;
			}
			return true;
		}
	}
	
//// MRAID API functions ////

	var mraid = window.mraid = {};
	
	mraid.getVersion = function() {
		traceAdCallWithReturn("getVersion",VERSION);
		return VERSION;
	};

	mraid.addEventListener = function(eventType, listener) {
		traceAdCall("addEventListener",eventType, listener);
	
		var event = events[eventType];
		if (!eventType)
			error("addEventListener", "No event was given.");
		else if (!event)
			error("addEventListener", "An unrecognized event type was given ("+eventType+").");
		else if (!listener)
			error("addEventListener", "No listener was given.");
		else
			event.addListener(listener);
	};

	mraid.removeEventListener = function(eventType, listener) {
		traceAdCall("removeEventListener",eventType, listener);
		
		var event = events[eventType];
		if (!eventType)
			error("removeEventListener", "No event was given.");
		else if (!event)
			error("removeEventListener", "An unrecognized event type was given ("+eventType+").");
		else
			event.removeListener(listener);
	};

	mraid.getPlacementType = function() {
		traceAdCallWithReturn("getPlacementType",placementType);
		return placementType;
	};

	mraid.isViewable = function() {
		traceAdCallWithReturn("isViewable",isViewable);
		return isViewable;
	};

	mraid.getMaxSize = function() {
		traceAdCallWithReturn("getMaxSize",JSON.stringify(maxSize));
		return maxSize;
	};

	mraid.getScreenSize = function() {
		traceAdCallWithReturn("getScreenSize",JSON.stringify(screenSize));
		return screenSize;
	};

	mraid.getDefaultPosition = function() {
		traceAdCallWithReturn("getDefaultPosition",JSON.stringify(defaultPosition));
		return defaultPosition;
	};

	mraid.getCurrentPosition = function() {
		traceAdCallWithReturn("getCurrentPosition",JSON.stringify(position));
		return position;
	};

	mraid.getState = function() {
		traceAdCallWithReturn("getState",state);
		return state;
	};

	mraid.expand = function(url) {
		traceAdCall("expand",url);
		
		if ((placementType == placementTypes.interstitial) || (state == states.loading) || (state == states.hidden))
			return;
	
		if (!url || (String(url).length==0))
			url = "x"; // Pass a single char as an indicator of no url.  Otherwise "//" is passed as part of the url and is misparsed by the SDK.
	
		mraidBridge.executeInSdk("expand", encodeURIComponent(url));
	};

	mraid.resize = function() {
		traceAdCall("resize");

		if ((placementType == placementTypes.interstitial) || (state == states.loading) || (state == states.hidden))
			return;
	
		if (state == states.expanded)
			error("resize", "Creative is currently expanded.");
		else
			mraidBridge.executeInSdk("resize");
	};

	mraid.close = function() {
		traceAdCall("close");
		
		if ((state == states.loading) || (state == states.hidden))
			return;
	
		mraidBridge.executeInSdk("close");
	};

	mraid.open = function(url) {
		traceAdCall("open",url);
		
		if (!url)
			error("open", "No URL was given.");
		else
			mraidBridge.executeInSdk("open", encodeURIComponent(url));
	};

	mraid.getExpandProperties = function() {
		traceAdCallWithReturn("getExpandProperties",JSON.stringify(expandProperties));
		return expandProperties;
	};

	mraid.setExpandProperties = function(properties) {
		traceAdCall("setExpandProperties",JSON.stringify(properties));

		if (!validNumeric("setExpandProperties",properties,"width", false)) return;
		if (!validNumeric("setExpandProperties",properties,"height", false)) return;
		if (!validBoolean("setExpandProperties",properties,"useCustomClose", false)) return;

		expandProperties = {width:screenSize.width, height:screenSize.height, useCustomClose:true, isModal:true};
		if (properties.hasOwnProperty("width")) expandProperties.width = properties.width;
		if (properties.hasOwnProperty("height")) expandProperties.height = properties.height;
		if (properties.hasOwnProperty("useCustomClose")) mraidBridge.useCustomClose(properties.useCustomClose);

		mraidBridge.executeInSdk("setExpandProperties", expandProperties.width, expandProperties.height, expandProperties.useCustomClose);
	};

	mraid.useCustomClose = function(useCustomClose) {
		traceAdCall("useCustomClose",useCustomClose);
		
		mraidBridge.useCustomClose(useCustomClose);
	};

	mraid.getOrientationProperties = function() {
		traceAdCallWithReturn("getOrientationProperties",JSON.stringify(orientationProperties));
		return orientationProperties;
	};

	mraid.setOrientationProperties = function(properties) {
		traceAdCall("setOrientationProperties",JSON.stringify(properties));
		
		if (!validBoolean("setOrientationProperties",properties,"allowOrientationChange", false)) return;

		orientationProperties = {allowOrientationChange:true, forceOrientation:"none"};
		if (properties.hasOwnProperty("allowOrientationChange")) orientationProperties.allowOrientationChange = properties.allowOrientationChange;
		if (properties.hasOwnProperty("forceOrientation")) orientationProperties.forceOrientation = properties.forceOrientation;
	
		mraidBridge.executeInSdk("setOrientationProperties",orientationProperties.allowOrientationChange,orientationProperties.forceOrientation);
	};

	mraid.getResizeProperties = function() {
		traceAdCallWithReturn("getResizeProperties",JSON.stringify(resizeProperties));
		return resizeProperties;
	};

	mraid.setResizeProperties = function(properties) {
		traceAdCall("setResizeProperties",JSON.stringify(properties));
		
		if (!validNumeric("setResizeProperties",properties,"width", true)) return;
		if (!validNumeric("setResizeProperties",properties,"height", true)) return;
		if (!validNumeric("setResizeProperties",properties,"offsetX", true)) return;
		if (!validNumeric("setResizeProperties",properties,"offsetY", true)) return;
		if (!validBoolean("setResizeProperties",properties,"allowOffscreen", false)) return;

		resizeProperties = {width:properties.width, height:properties.height, offsetX:properties.offsetX, offsetY:properties.offsetY,
						    customClosePosition:"top-right", allowOffscreen:true};
		if (properties.hasOwnProperty("customClosePosition")) resizeProperties.customClosePosition = properties.customClosePosition;
		if (properties.hasOwnProperty("allowOffscreen")) resizeProperties.allowOffscreen = properties.allowOffscreen;

		mraidBridge.executeInSdk("setResizeProperties",
			resizeProperties.offsetX,resizeProperties.offsetY,resizeProperties.width,resizeProperties.height,
			resizeProperties.customClosePosition, resizeProperties.allowOffscreen);
	};

 	mraid.supports = function(feature) {
		var supportsFeature = false;
		if (features.hasOwnProperty(feature))
			supportsFeature = features[feature];

		traceAdCallWithReturn("supports",supportsFeature,feature);

		return supportsFeature;
	};

	mraid.playVideo = function(url) {
		traceAdCall("playVideo",url);
		
		if (!url)
			error("playVideo", "No URL was given.");
		else
			mraidBridge.executeInSdk("playVideo", encodeURIComponent(url));
	};

	mraid.storePicture = function(url) {
		traceAdCall("storePicture",url);
		
		if (!url)
			error("storePicture", "No URL was given.");
		else
			mraidBridge.executeInSdk("storePicture", encodeURIComponent(url));
	};

	mraid.createCalendarEvent = function(calParms) {
		traceAdCall("createCalendarEvent",calParms);
		
		mraidBridge.executeInSdk("createCalendarEvent", encodeURIComponent(JSON.stringify(calParms)));
	};

	mraidBridge.executeInSdk("loaded"); // ...must be the first command back to SDK
	window.addEventListener("load",	function() {
		mraidBridge.resumeSdkExecution(); // ...allows pending commands to execute now
	}, false);
}());
