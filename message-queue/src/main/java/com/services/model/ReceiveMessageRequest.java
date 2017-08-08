package com.services.model;

public class ReceiveMessageRequest {
	private String queueUrl;
	private int requestBatchSize;
	private int visibilityTimeout;
	
	public ReceiveMessageRequest() {}
	
	public ReceiveMessageRequest(String queueUrl, int requestBatchSize, int visibilityTimeout) {
		this.queueUrl = queueUrl;
		this.requestBatchSize = requestBatchSize;
		this.visibilityTimeout = visibilityTimeout;
	}
	
	public int getRequestBatchSize() {
		return requestBatchSize;
	}
	
	public void setMessageBatchSize(int requestBatchSize) {
		this.requestBatchSize = requestBatchSize;
	}
	
	public String getQueueUrl() {
		return queueUrl;
	}
	
	public void setQueueUrl(String queueUrl) {
		this.queueUrl = queueUrl;
	}
	
	public int getVisibilityTimeout() {
		return visibilityTimeout;
	}
	
	public void setVisibilityTimeout(int visibilityTimeout) {
		this.visibilityTimeout = visibilityTimeout;
	}
}
