package com.services.model;

import java.util.List;

public class DeleteMessageRequest {
	private String queueUrl;
	private List<Message> messages;
		
	public DeleteMessageRequest(String queueUrl, List<Message> messages) {
		this.queueUrl = queueUrl;
		this.messages = messages;
	}
	
	public String getQueueUrl() {
		return queueUrl;
	}
	
	public void setQueueUrl(String queueUrl) {
		this.queueUrl = queueUrl;
	}
	
	public List<Message> getMessages() {
		return this.messages;
	}
	
	public void setMessages(List<Message> messages) {
		this.messages = messages;
	}
}