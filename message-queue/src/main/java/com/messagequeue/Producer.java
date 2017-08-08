package com.messagequeue;


import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.messagequeue.services.QueueService;
import com.services.model.Message;
import com.services.model.MessageStatus;
import com.services.model.SendMessageRequest;

public class Producer implements Runnable {

	private QueueService queueService;
	private String queueUrl;
	private int delayPublish;
	private int maxMessages;
	
	public Producer(QueueService queueService, String queueUrl, int delayPublish, int maxMessages) {
		this.queueService = queueService;
		this.queueUrl = queueUrl;
		this.delayPublish = delayPublish;
		this.maxMessages = maxMessages;
	}
	
	public void run() {
		
		// Generate Send Message Request.
		SendMessageRequest request = GenerateSendMessage(maxMessages);
		
		// Place Request to push messages to the SQS Queue. 
		queueService.push(request);
	}
	
	// Generates Request Object with maximum number of Messages as configured for the Queue.
	private SendMessageRequest GenerateSendMessage(int messageVolume) {
		Random randomGenerator = new Random();
		List<Message> messages= new ArrayList<Message>();
		
		for(int i =0; i < messageVolume; i++) {
			String messageContent = "Amazon SQS - Message ID: " + Integer.toString(randomGenerator.nextInt(500));
			
			Message message = new Message();
			message.setMessageStatus(MessageStatus.NEW);
			message.setPriorAttempts(0);
			message.setMessage(messageContent);
			messages.add(message);
		}
		
		return new SendMessageRequest(queueUrl, messages, delayPublish);
	}
}

