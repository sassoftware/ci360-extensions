package com.sas.ci360.agent.scg;

import java.util.ArrayList;
import java.util.List;

public class MessageRequest {	
	private String from;
	private List<String> to;
	private String body;
	private List<String> media_urls;
	private String content_type;
	private String id;
	private String state;
	private String external_id;
	
	
	public String getFrom() {
		return from;
	}
	public void setFrom(String from) {
		this.from = from;
	}
	public List<String> getTo() {
		return to;
	}
	public void setTo(List<String> to) {
		this.to = to;
	}
    public void setTo(String to) {
        ArrayList<String> toList = new ArrayList<>();
        toList.add(to);
        setTo(toList);
    }
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}
	public List<String> getMedia_urls() {
		return media_urls;
	}
	public void setMedia_urls(List<String> media_urls) {
		this.media_urls = media_urls;
	}
	public void setMedia_urls(String media_urls) {
        ArrayList<String> mediaList = new ArrayList<>();
        mediaList.add(media_urls);
        setMedia_urls(mediaList);
	}
	public String getContent_type() {
		return content_type;
	}
	public void setContent_type(String content_type) {
		this.content_type = content_type;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getExternal_id() {
		return external_id;
	}
	public void setExternal_id(String external_id) {
		this.external_id = external_id;
	}

}
