package com.messagequeue.services;

import java.util.ArrayList;
import java.util.List;


import com.services.model.Message;
import com.services.model.DeleteMessageRequest;
import com.services.model.SendMessageRequest;
import com.services.model.ReceiveMessageRequest;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;


public class SqsQueueService implements QueueService {
	
	private final AmazonSQS sqs;
	private final Region usWest2;
	private final AWSCredentials credentials;
	
	public SqsQueueService() {
		
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        sqs = new AmazonSQSClient(credentials);
        usWest2 = Region.getRegion(Regions.US_WEST_2);
        sqs.setRegion(usWest2);
    }
	
	/// <summary>
    /// Sending batch of messages to in memory queue.
    /// Delay seconds will wait for the specified delay before pushing on to Queue. </summary>
	/// <parameter name ="request">Represents the Send Message Request.</parameter>
	@Override
	public void push(SendMessageRequest request) {
		
		List<SendMessageBatchRequestEntry> entries = new ArrayList<SendMessageBatchRequestEntry>();
		
		for(Message msg : request.getAllMessages()) {
			SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry();
			entry.setDelaySeconds(request.getDelaySeconds());
			entry.setMessageBody(msg.toString());
			entries.add(entry);
		}
		
		// Create the Send Message Batch request for Amazon sqs Send Operation.
		SendMessageBatchRequest sendBatchRequest = new SendMessageBatchRequest();
		sendBatchRequest.setEntries(entries);
		sendBatchRequest.setQueueUrl(request.getQueueUrl());
		sendBatchRequest.setRequestCredentials(credentials);
	
		sqs.sendMessageBatch(sendBatchRequest);
	}

	/// <summary>
    /// Retrieves one or more messages (up to max limit specified in the configuration), from the specified queue 
	/// </summary>
	/// <parameter name ="request">Represents the Receive Message Request.</parameter>
    ///<Returns>Returns the Queue Messages for processing.</Returns>
	@Override
	public List<Message> poll(ReceiveMessageRequest request) {
		
		com.amazonaws.services.sqs.model.ReceiveMessageRequest receiveRequest = 
														new com.amazonaws.services.sqs.model.ReceiveMessageRequest();
		receiveRequest.setRequestCredentials(credentials);
		receiveRequest.setQueueUrl(request.getQueueUrl());
		receiveRequest.setVisibilityTimeout(request.getVisibilityTimeout());
		
		List<com.amazonaws.services.sqs.model.Message> sqsMessages = sqs.receiveMessage(receiveRequest).getMessages();
		List<Message> messages = new ArrayList<Message>();
		
		for (com.amazonaws.services.sqs.model.Message msg : sqsMessages) {
			Message message = new Message();
			message.setMessage(msg.getBody());
			msg.setReceiptHandle(msg.getReceiptHandle());
			messages.add(message);
		}
		
		return messages;
	}

	/// <summary>
    /// Deletes the messages from Amazon SQS Queue. 
	/// </summary>
	/// <parameter name ="request">Represents the Receive Message Request.</parameter>
    @Override
	public void delete(DeleteMessageRequest request) {
		
		List<DeleteMessageBatchRequestEntry> entries = new ArrayList<DeleteMessageBatchRequestEntry>();
		DeleteMessageBatchRequest deleteRequest = new DeleteMessageBatchRequest();
		
		for(Message msg : request.getMessages()) {
			
			DeleteMessageBatchRequestEntry entry = new DeleteMessageBatchRequestEntry();
			entry.setReceiptHandle(Integer.toString(msg.getReceiptId()));
			entries.add(entry);
		}
		
		deleteRequest.setRequestCredentials(credentials);
		deleteRequest.setQueueUrl(request.getQueueUrl());
		deleteRequest.setEntries(entries);
		
		sqs.deleteMessageBatch(deleteRequest);
	}
    
    /// <summary>
    /// Must be called by the client before trying to send or pull queue to get the
    /// Queue URL associated with the Queue, the client is attempting to perform the opearation.
	/// </summary>
	/// <parameter name ="url">Represents the Queue URL.</parameter>
    /// <Returns>Returns the Queue URL associated with the Queue.</Returns>
	public String getOrCreateQueue(String url) {
		
		String myQueueUrl = null;
		
		if(!QueueExists(url)) {
			CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue");
			myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
		}
		
		return myQueueUrl;
	}
	
	/// <summary>
    /// Must be called by the client before trying to send or pull queue to get the
    /// Queue URL associated with the Queue, the client is attempting to perform the operation.
	/// </summary>
	/// <parameter name ="url">Represents the Queue URL.</parameter>
	private boolean QueueExists(String url){
		
		for (String queueUrl : sqs.listQueues().getQueueUrls()) {
		    if(queueUrl.equalsIgnoreCase(url)) {
		    	return true;
		    }
		}
		
		return false;
	}
}
