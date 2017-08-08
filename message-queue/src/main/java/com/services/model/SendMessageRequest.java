package com.services.model;

import java.util.List;

public class SendMessageRequest {
	private String queueUrl;
	private List<Message> messages;
	private int delaySeconds;
	
	public SendMessageRequest() {}
	
	public SendMessageRequest(String queueUrl, List<Message> messages, int delaySeconds) {
		
		this.queueUrl = queueUrl;
		this.messages = messages;
		this.delaySeconds = delaySeconds;
	}
	
	public int getDelaySeconds() {
		return delaySeconds;
	}
	
	public void setDelaySeconds(int delaySeconds) {
		this.delaySeconds = delaySeconds;
	}
	
	public String getQueueUrl() {
		return queueUrl;
	}
	
	public void setQueueUrl(String queueUrl) {
		this.queueUrl = queueUrl;
	}
	
	public List<Message> getAllMessages() {
		return messages;
	}
	
	public void setAllMessage(List<Message> messages) {
		this.messages = messages;
	}
}

