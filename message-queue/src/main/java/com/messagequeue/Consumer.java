package com.messagequeue;

import java.util.List;

import com.messagequeue.services.QueueService;
import com.services.model.DeleteMessageRequest;
import com.services.model.Message;
import com.services.model.ReceiveMessageRequest;

public class Consumer implements Runnable {

	private QueueService queueService;
	private String queueUrl;
	private int visibilityTimeout;
	private int maxMessages;
	
	public Consumer(QueueService queueService, String queueUrl, int visibilityTimeout, int maxMessages) {
		this.queueService = queueService;
		this.queueUrl = queueUrl;
		this.visibilityTimeout = visibilityTimeout;
		this.maxMessages = maxMessages;
	}
	
	public void run() {
		
		// Generate Send Message Request.
		ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest(queueUrl,maxMessages, visibilityTimeout);
		
		try {
			List<Message> receivedMessages = (List<Message>)queueService.poll(receiveRequest);
						
			if(receivedMessages != null) {
				for(Message msg : receivedMessages) {
					System.out.println(msg.toString());
					
				}
				
				DeleteMessageRequest deleteRequest = new DeleteMessageRequest(queueUrl, receivedMessages);
				queueService.delete(deleteRequest);
			}
			
			
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

