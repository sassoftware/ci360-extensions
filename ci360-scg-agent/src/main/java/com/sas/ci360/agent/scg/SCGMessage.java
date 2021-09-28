package com.sas.ci360.agent.scg;

import org.json.JSONObject;

public class SCGMessage {
	private static final String JSON_EVENT = "event";
	private static final String JSON_EVENT_ID = "event-id";
	private static final String JSON_FLD_VAL = "fld-val-list";
	private static final String JSON_EVT_TP = "evt-tp";
	private static final String JSON_MSG_ID = "message_id";
	private static final String JSON_NEW_STATE = "new_state";
	private static final String JSON_TO_ADDRESS = "to_address";
	private static final String JSON_FROM_ADDRESS = "from_address";
	private static final String JSON_MSG_BODY = "message_body";
	private static final String JSON_EXTERNAL_MSG_ID = "external_message_request_id";
	
	private static final String VAL_MO_MESSAGE = "mo_message_received";
	
	private String eventId;
	private String eventType;
	private String msgId;
	private String newState;
	private String toAddress;
	private String fromAddress;
	private String messageBody;
	private String externalMessageId;
	private boolean moMessage = false;
	
	public SCGMessage(String reqBody) {
		JSONObject webhookObj = new JSONObject(reqBody);
		JSONObject fldList = webhookObj.getJSONObject(JSON_EVENT).getJSONObject(JSON_FLD_VAL);
		eventType = webhookObj.getJSONObject(JSON_EVENT).getString(JSON_EVT_TP);
		
		if (eventType.equals(VAL_MO_MESSAGE)) {
			moMessage = true;
		}
		
		eventId = webhookObj.getString(JSON_EVENT_ID);
		msgId = fldList.getString(JSON_MSG_ID);
		if (fldList.has(JSON_TO_ADDRESS)) toAddress = fldList.getString(JSON_TO_ADDRESS);
		if (fldList.has(JSON_FROM_ADDRESS)) fromAddress = fldList.getString(JSON_FROM_ADDRESS);
		
		if (moMessage) {
			messageBody = fldList.getString(JSON_MSG_BODY);			
		}
		else {
			newState = fldList.getString(JSON_NEW_STATE);
			externalMessageId = fldList.getString(JSON_EXTERNAL_MSG_ID);
		}

	}

	
	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public String getMsgId() {
		return msgId;
	}

	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}

	public String getNewState() {
		return newState;
	}

	public void setNewState(String newState) {
		this.newState = newState;
	}

	public String getToAddress() {
		return toAddress;
	}

	public void setToAddress(String toAddress) {
		this.toAddress = toAddress;
	}

	public String getFromAddress() {
		return fromAddress;
	}

	public void setFromAddress(String fromAddress) {
		this.fromAddress = fromAddress;
	}

	public String getExternalMessageId() {
		return externalMessageId;
	}

	public void setExternalMessageId(String externalMessageId) {
		this.externalMessageId = externalMessageId;
	}

	public String getMessageBody() {
		return messageBody;
	}

	public void setMessageBody(String messageBody) {
		this.messageBody = messageBody;
	}
	
	public boolean isMoMessage() {
		return moMessage;
	}
	
	
}
