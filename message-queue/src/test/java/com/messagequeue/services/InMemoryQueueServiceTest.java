package com.messagequeue.services;

import com.utilities.DateTimeUtility;

import static org.junit.Assert.assertEquals;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.services.model.Message;
import com.services.model.MessageStatus;
import com.messagequeue.config.ApplicationConfiguration;
import com.messagequeue.services.InMemoryQueueService;
import com.services.model.DeleteMessageRequest;
import com.services.model.SendMessageRequest;
import com.services.model.ReceiveMessageRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@ActiveProfiles("local")
public class InMemoryQueueServiceTest  {
	
	@Autowired
	private InMemoryQueueService serviceInstance;
	
	@Autowired
	private ApplicationConfiguration config;
	
	ConcurrentLinkedQueue<Message> queue;
	String queueUri;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		queueUri = config.getQueueUrl();
		queue = createQueue();
		queue.clear();
	}

	@After
	public void tearDown() throws Exception {
	}
	

	// Push a single message on the queue and check if the size of the queue increases 1.
	// A delay of 0sec for local environment is introduced before pushing the messages to the In memory queue.
	@Test(timeout = 999)
	public void testPushSingle() {
		
		int delayMessageSec = Integer.parseInt(config.getDelayPublish());
		
		// Generate Send Message Request.
		Random randomGenerator = new Random();
		List<Message> messages= new ArrayList<Message>();
		
		String messageContent = "In Memory implementation of SQS - Message ID: " + Integer.toString(randomGenerator.nextInt(500));
			
		Message message = new Message();
		message.setMessageStatus(MessageStatus.NEW);
		message.setPriorAttempts(0);
		message.setMessage(messageContent);
		messages.add(message);
	
		SendMessageRequest request = new SendMessageRequest(queueUri, messages, delayMessageSec);
		
		// Place Request to push messages to the SQS Queue. 
		serviceInstance.push(request);
		
		assertEquals(1, queue.size());
	}
	
	// Push multiple messages on the queue and check if the size of the queue 
	// increases by the number of messages being pushed.
	// A delay of 0sec for local environment is introduced before pushing the messages to the In memory queue.
	@Test(timeout = 999)
	public void testPushMultiple() {
		
		int delayMessageSec = Integer.parseInt(config.getDelayPublish());
		int msgBatchSize = Integer.parseInt(config.getMaxMessages());
		
		// Generate Send Message Request.
		Random randomGenerator = new Random();
		List<Message> messages= new ArrayList<Message>();
		
		for (int i =0; i < msgBatchSize; i++) {
		
			String messageContent = "In Memory implementation of SQS - Message ID: " + Integer.toString(randomGenerator.nextInt(500));
				
			Message message = new Message();
			message.setMessageStatus(MessageStatus.NEW);
			message.setPriorAttempts(0);
			message.setMessage(messageContent);
			messages.add(message);
		}
		
		SendMessageRequest request = new SendMessageRequest(queueUri, messages, delayMessageSec);
		
		// Place Request to push messages to the SQS Queue. 
		serviceInstance.push(request);
		
		assertEquals(msgBatchSize, queue.size());
	}
	
	// Pushed number of messages smaller than the Batch Size. 
	// Expected output should be 
	@Test(timeout = 999)
	public void testPoll(){
		
		int delayMessageSec = Integer.parseInt(config.getDelayPublish());
		int visibilityTimeout = Integer.parseInt(config.getVisibilityTimeout());
		int msgBatchSize = Integer.parseInt(config.getMaxMessages());
		
		// Generate Send Message Request.
		Random randomGenerator = new Random();
		List<Message> messages= new ArrayList<Message>();
		
		for (int i =0; i < msgBatchSize; i++) {
		
			String messageContent = "In Memory implementation of SQS - Message ID: " + Integer.toString(randomGenerator.nextInt(500));
				
			Message message = new Message();
			message.setMessageStatus(MessageStatus.NEW);
			message.setPriorAttempts(0);
			message.setMessage(messageContent);
			messages.add(message);
		}
		
		SendMessageRequest request = new SendMessageRequest(queueUri, messages, delayMessageSec);
		serviceInstance.push(request);
		
		ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest(queueUri,msgBatchSize, visibilityTimeout);
		List<Message> receivedMessages = (List<Message>)serviceInstance.poll(receiveRequest);
		
		int actual = receivedMessages != null ? receivedMessages.size() : 0;
		assertEquals(msgBatchSize, actual);
	}
	
	// Visibility Time out for local environment is configured as 5 minutes.
	// Set the in-flight message with invisible from time to 10 minutes
	@Test(timeout = 999)
	public void testVisibilityTimeout() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		
		int visibilityTimeout = Integer.parseInt(config.getVisibilityTimeout());
		int msgBatchSize = Integer.parseInt(config.getMaxMessages());
		
		List<Message> inflightMessages = new ArrayList<Message>();
		// Add message to in-flight message collection with invisible from time set to 15 minutes 
	    // past the current time.
	    
		// Generate Send Message Request.
		Random randomGenerator = new Random();
		Calendar cal = Calendar.getInstance();
		Date invisibleFromTime = DateTimeUtility.addMinutesToDate(-10, cal.getTime());
		
		String messageContent = "In Memory implementation of SQS - Message ID: " + Integer.toString(randomGenerator.nextInt(500));
		
		Message message = new Message();
		message.setMessageStatus(MessageStatus.INFLIGHT);
		// Set Prior Attempt to 1 indicating this message was attempted once earlier.
		message.setPriorAttempts(1);
		message.setMessage(messageContent);
		// Set InvisibleFromTime to 15 past the current time.
		message.setinvisibleFromTime(invisibleFromTime);
		inflightMessages.add(message);
		
		// Make the inflightMessage collection visible through Reflection.
		Field field = InMemoryQueueService.class.getDeclaredField("inflightMessages");
	    field.setAccessible(true);
	    field.set(serviceInstance, inflightMessages);
		
	    ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest(queueUri,msgBatchSize, visibilityTimeout);
		List<Message> receivedMessages = (List<Message>)serviceInstance.poll(receiveRequest);
		
		int actual = receivedMessages != null ? receivedMessages.size() : 0;
		assertEquals(1, actual);
		
	}
	
	@Test(timeout=999)
	public void testDelete() {
		int delayMessageSec = Integer.parseInt(config.getDelayPublish());
		
		// Generate Send Message Request.
		Random randomGenerator = new Random();
		List<Message> messages= new ArrayList<Message>();
		
		
		String messageContent = "In Memory implementation of SQS - Message ID: " + Integer.toString(randomGenerator.nextInt(500));
			
		Message message = new Message();
		message.setMessageStatus(MessageStatus.NEW);
		message.setPriorAttempts(0);
		message.setMessage(messageContent);
		messages.add(message);
		SendMessageRequest request = new SendMessageRequest(queueUri, messages, delayMessageSec);
		
		// Place Request to push messages to the SQS Queue. 
		serviceInstance.push(request);
		
		List<Message> deleteMessages = new ArrayList<Message>();
		deleteMessages.add(message);
		
		DeleteMessageRequest deleteRequest = new DeleteMessageRequest(queueUri, deleteMessages);
		serviceInstance.delete(deleteRequest);
		
		assertEquals(0, queue.size());
	}
	
	/// <summary>
    /// Helper method that invokes the private CreateQueueFile through Reflection to get 
	/// a file reference to queue message file. 
	/// </summary>
    ///<Returns>Returns the Queue Messages File.</Returns>
	@SuppressWarnings("unchecked")
	private ConcurrentLinkedQueue<Message>  createQueue() throws Exception {
		// Call the Get Queue to get reference to the messages file(File Queue).
		Method getMessageQueue = InMemoryQueueService.class.getDeclaredMethod("getOrCreateQueue",String.class);
		getMessageQueue.setAccessible(true);
		ConcurrentLinkedQueue<Message> queue = (ConcurrentLinkedQueue<Message>)getMessageQueue.invoke(serviceInstance,queueUri);
		
		return queue;
	}
}

