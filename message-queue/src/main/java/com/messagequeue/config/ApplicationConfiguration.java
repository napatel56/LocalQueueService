package com.messagequeue.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("import")
public class ApplicationConfiguration {
	 
	private String workerThreads;
	private String maxMessages;
	private String queueUrl;
	private String delayPublish;
	private String visibilityTimeout;
		
	public String getWorkerThreads() {
		return workerThreads;
	}
	
	public void setWorkerThreads(String workerThreads) {
		this.workerThreads = workerThreads;
	}
	
	public String getMaxMessages() {
		return maxMessages;
	}
	
	public void setMaxMessages(String maxMessages) {
		this.maxMessages = maxMessages;
	}
	
	public String getQueueUrl() {
		return queueUrl;
	}
	
	public void setQueueUrl(String queueUrl) {
		this.queueUrl = queueUrl;
	}
	public String getDelayPublish() {
		return delayPublish;
	}
	
	public void setDelayPublish(String delayPublish) {
		this.delayPublish = delayPublish;
	}
	
	public String getVisibilityTimeout() {
		return visibilityTimeout;
	}
	public void setVisibilityTimeout(String visibilityTimeout) {
		this.visibilityTimeout = visibilityTimeout;
	}
	
}
