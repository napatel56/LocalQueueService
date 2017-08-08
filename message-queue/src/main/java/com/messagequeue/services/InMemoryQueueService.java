package com.messagequeue.services;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import com.utilities.DateTimeUtility;
import com.services.model.Message;
import com.services.model.DeleteMessageRequest;
import com.services.model.SendMessageRequest;
import com.services.model.ReceiveMessageRequest;

@Service
@Profile({"local"})
public class InMemoryQueueService implements QueueService {
	
	private final Map<String, ConcurrentLinkedQueue<Message>> queues = new HashMap<>();
	private final List<Message> inflightMessages = new ArrayList<>();
	private static AtomicInteger identity = new AtomicInteger();
	
	
    /// <summary>
    /// Sending batch of messages to in memory queue.
    /// Delay seconds will wait for the specified delay before pushing on to Queue. </summary>
	/// <parameter name ="request">Represents the Send Message Request.</parameter>
    @Override
	public void push(SendMessageRequest request) {
		final ConcurrentLinkedQueue<Message> queue = getOrCreateQueue(request.getQueueUrl());
		
		// Introduced the Delay Seconds to minimize the possibility of Consumer Starvation.
		int delay = request.getDelaySeconds();
		
		try {
			TimeUnit.SECONDS.sleep(delay);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}	
		
		for(Message message : request.getAllMessages()) {
			// Enqueues the message to the Queue.
			queue.offer(message);
			
		}
	}

	/// <summary>
    /// Retrieves one or more messages (up to max limit specified in the configuration), from the specified queue.
	/// Messages exceeding the Visibility Timeout will be available in the queue when there is new receive request is placed.   
	/// </summary>
	/// <parameter name ="request">Represents the Receive Message Request.</parameter>
    ///<Returns>Returns the Queue Messages for processing.</Returns>
    @Override
    public List<Message> poll(ReceiveMessageRequest request) {
		
		final ConcurrentLinkedQueue<Message> queue = getOrCreateQueue(request.getQueueUrl());
		List<Message> messages = new ArrayList<>();
		Calendar cal = Calendar.getInstance();
		int visibilityTimeout = request.getVisibilityTimeout(); 
				
		// in-flight messages collection maintains the messages picked up and currently being processed.
		for(Message msg : inflightMessages) {
			int duration = Math.abs(DateTimeUtility.minutesDiff(cal.getTime(), msg.getinvisibleFromTime()));
			
			// Checks if the in-flight message has been invisible past the visibility time out setting.
			if(duration > visibilityTimeout) {
				
				// Increment the prior attempt to indicate the message has been picked again for processing.
				int priorAttempt = msg.getPriorAttempts();
				msg.setPriorAttempts(priorAttempt++);
				
				// enqueue the message to the queue for re-processing.
				queue.offer(msg);
			}
		}
		
		
		
		// Validate if the Queue has any messages to process.
		if(queue.size() == 0) {
			System.out.println("There are no messages in the Queue waiting to be processed.");
			return null;
		}
		
		int count = 0;
		while(queue.size() > 0 && count < request.getRequestBatchSize()) {
			
			// Removes the message from the queue.
			Message message = (Message)queue.poll();
			
			if(message != null) {
				// invisible From Time marks when the message has been picked up for processing.
				message.setinvisibleFromTime(cal.getTime());
				message.setReceiptId(identity.incrementAndGet());
				messages.add(message);
				
				// add the picked up message to in-flight messages collection to be able to retrieve again 
				// in case the message processing is failed and the message has to readded to the queue for
				// re-processing.
				inflightMessages.add(message);
			}
			count++;
		}
	
		return messages;
	}

	/// <summary>
    /// Deletes the given messages from the Queue.
	/// </summary>
	/// <parameter name ="request">Represents the Delete Message Request.</parameter>
    @Override
    public void delete(DeleteMessageRequest request) {
		final ConcurrentLinkedQueue<Message> queue = getOrCreateQueue(request.getQueueUrl());
		
		for(Message msg : request.getMessages()) {
			queue.remove(msg);
		}
	}
	
    
    /// <summary>
    /// Creates a Queue if there is no queue for the given url, otherwise return the instance of
    /// the queue for the given queue url.
    /// </summary>
	/// <parameter name ="url">Represents the Queue URL</parameter>
	private ConcurrentLinkedQueue<Message> getOrCreateQueue(String url) {

		ConcurrentLinkedQueue<Message> queue = queues.get(url);
		
		if (queue == null) {
	
			// Locks the critical section. Only one thread can create new instance of the Queue.
			synchronized (queues) {
			
				if(queue == null) {
					queue = new ConcurrentLinkedQueue<>();
					queues.put(url, queue);
				}
					
			}
		}

		return queue;
	}
	
}
