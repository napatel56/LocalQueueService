/**
 * 
 */
package com.messagequeue.services;

import java.util.List;

import com.services.model.Message;
import com.services.model.DeleteMessageRequest;
import com.services.model.SendMessageRequest;
import com.services.model.ReceiveMessageRequest;

public interface QueueService {
	
	//  pushes a message onto a queue.
	void push(SendMessageRequest request);
	
	//  retrieves a single message from a queue.
	List<Message> poll(ReceiveMessageRequest request) throws InterruptedException;
	
	//  deletes a message from the queue that was received by pull().
	void delete(DeleteMessageRequest request) ;
	
}
