package com.messagequeue.services;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.services.model.Message;
import com.services.model.MessageStatus;
import com.messagequeue.config.ApplicationConfiguration;
import com.messagequeue.services.FileQueueService;
import com.services.model.DeleteMessageRequest;
import com.services.model.SendMessageRequest;
import com.services.model.ReceiveMessageRequest;
import com.utilities.DateTimeUtility;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@ActiveProfiles("test")
public class FileQueueServiceTest {
	static String queueUri;
	static final String queueName = "messages";
	private static final String lockName = ".lock";
	
	@Autowired
	private FileQueueService serviceInstance;
	
	@Autowired
	private ApplicationConfiguration config;
	
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	
	}

	@Before
	public void setUp() throws Exception {
		queueUri = config.getQueueUrl();
	}

	@After
	public void tearDown() throws Exception {
	
	}
	
	@Test
	public void testPush() throws Exception {
		
		int delayMessageSec = Integer.parseInt(config.getDelayPublish());
		int msgBatchSize = Integer.parseInt(config.getMaxMessages());
		
		File file = CreateQueueFile();
		
		AcquireLock();
		
		// Clear the contents of the queue.
		try(PrintWriter pw = new PrintWriter(new FileWriter(file))) {   // append
			pw.print("");
		}
				
		ReleaseLock();
		
		// Generate Send Message Request.
		Random randomGenerator = new Random();
		List<Message> messages= new ArrayList<Message>();
		
		for (int i =0; i < msgBatchSize; i++) {
		
			String messageContent = "File implementation of SQS - Message ID: " + Integer.toString(randomGenerator.nextInt(500));
				
			Message message = new Message();
			message.setMessageStatus(MessageStatus.NEW);
			message.setPriorAttempts(0);
			message.setMessage(messageContent);
			messages.add(message);
		}
		
		SendMessageRequest request = new SendMessageRequest(queueUri, messages, delayMessageSec);
		
		// Place Request to push messages to the SQS Queue. 
		serviceInstance.push(request);
		
		file = CreateQueueFile();
		
		AcquireLock();
		
		//Call helper function to get the number of messages in the messages file.
		int actualMessageCount = totalRecordCount(file);
		
		ReleaseLock();
		
		assertEquals(msgBatchSize, actualMessageCount);
	}
	
	@Test
	public void testReplaceSelected() throws Exception {
		DateFormat formatter =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		String currTime = formatter.format(cal.getTime());
		
		String message = "0|0|" + currTime + "||File implementation of SQS - Message ID:123";
		String newMessage = "1|0|" + currTime + "|123|File implementation of SQS - Message ID:123";
		
		File file = CreateQueueFile();
		
		AcquireLock();
		
		// Clear the contents of the queue.
		try(PrintWriter pw = new PrintWriter(new FileWriter(file))) {   // append
			pw.print("");
		}
				
		try(PrintWriter pw = new PrintWriter(new FileWriter(file, true))) {   // append
			pw.println(message);
		}
    	
		ReleaseLock();
		
		Method replaceSel = FileQueueService.class.getDeclaredMethod("replaceSelected",String.class, String.class, String.class);
		replaceSel.setAccessible(true);
		replaceSel.invoke(serviceInstance,message, newMessage, queueUri);
		
		AcquireLock();
		
		String actualMessage = convertFileToString(file);
		
		ReleaseLock();
		
		assertEquals(newMessage, actualMessage);
	}
	
	@Test
	public void testDeleteMessages() throws Exception {
		int msgBatchSize = 10;
		int deletedMessageCount = 4;
		
		// Generate Send Message Request.
		Random randomGenerator = new Random();
		List<Message> messages= new ArrayList<>();
		
		for (int i =0; i < msgBatchSize; i++) {
		
			String messageContent = "File implementation of SQS - Message ID: " + Integer.toString(randomGenerator.nextInt(500));
				
			Message message = new Message();
			
			message.setMessageStatus(MessageStatus.NEW);
			message.setPriorAttempts(0);
			message.setMessage(messageContent);
			messages.add(message);
		}
		
		try{
			
			// Call the Helper Method to create the messages file(File Queue).
			File file = CreateQueueFile();
			
			AcquireLock();
			
			// Clear the contents of the queue.
			try(PrintWriter pw = new PrintWriter(new FileWriter(file))) {   // append
				pw.print("");
			}
					
			try(PrintWriter pw = new PrintWriter(new FileWriter(file, true))) {   // append
				for(Message msg : messages) {
					pw.println(msg);
				}
			}
		}
		finally {
			ReleaseLock();
		}
		
		List<Message> deleteMessages = new ArrayList<>();
		
		int count = 0;
		for (Message msg : messages) {
			
			if(count < deletedMessageCount) {
				deleteMessages.add(msg);
			}
			count++;
		}
		
		DeleteMessageRequest request = new DeleteMessageRequest(queueUri, deleteMessages);
		
		serviceInstance.delete(request);
		
		int expectedRemainingMessage = 6;
		int actualMessageCount;
		
		try {
					
			File file = CreateQueueFile();
			
			AcquireLock();
			
			//Call helper function to get the number of messages in the messages file.
			actualMessageCount = totalRecordCount(file);
		}
		finally{
			ReleaseLock();
		}
		assertEquals(expectedRemainingMessage, actualMessageCount);
		
		
	}
	
	/// <summary>
    /// Verifies that no messages are returned when size of the queue is Empty and 
	/// an appropriate message should be printed on the console.
    /// </summary>
    @Test
	public void testPollWithEmptyQueue() throws Exception {
	   	int visibilityTimeout = Integer.parseInt(config.getVisibilityTimeout());
	    	File messagesFile = CreateQueueFile();
			
	    	AcquireLock();
	    	
		// Clear the contents of the queue.
		try(PrintWriter pw = new PrintWriter(new FileWriter(messagesFile))) {   // append
			pw.print("");
		}
		
		ReleaseLock();
			
	    	ReceiveMessageRequest request = new ReceiveMessageRequest(queueUri, 5,visibilityTimeout);
		List<Message> messages = serviceInstance.poll(request);
		
		assertNull(messages);
	}
	
    /// <summary>
    /// Verifies that no messages are returned when Queue is Not Empty. But there are no new messages 
	/// present in queue for processing. An appropriate message should be printed on the console.
    /// </summary>
    ///<TestResults>An appropriate message should be printed on the console stating
    /// There are no messages in queue waiting to be processed.</TestResults>
    @Test
    public void testPollWithNoNewMessagesInQueue() throws Exception {
    	
    		DateFormat formatter =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		String currTime = formatter.format(cal.getTime());
		
		int visibilityTimeout = Integer.parseInt(config.getVisibilityTimeout());
		
    		String message = "INFLIGHT|0|"+ currTime + "|7777|File implementation of SQS - Message ID:123";
		
		
		File messagesFile = CreateQueueFile();
		
		AcquireLock();
		
		// Clear the contents of the queue.
		try(PrintWriter pw = new PrintWriter(new FileWriter(messagesFile))) {   // append
			pw.print("");
		}
		
		try(PrintWriter pw = new PrintWriter(new FileWriter(messagesFile, true))) {   // append
			pw.println(message);
		}
		
		ReleaseLock();
		
		ReceiveMessageRequest request = new ReceiveMessageRequest(queueUri, 5, visibilityTimeout);
		List<Message> messages = serviceInstance.poll(request);
		
		assertNull(messages);
    }
    
    @Test
    public void testPoll() throws Exception{
    	
    	int msgBatchSize = 10;
    	int visibilityTimeout = Integer.parseInt(config.getVisibilityTimeout());
    	
    	Random randomGenerator = new Random();
		List<Message> messages= new ArrayList<Message>();
		
		for (int i =0; i < msgBatchSize; i++) {
		
			String messageContent = "File implementation of SQS - Message ID: " + Integer.toString(randomGenerator.nextInt(500));
				
			Message message = new Message();
			
			if(i%2 == 0 ) {
				message.setMessageStatus(MessageStatus.NEW);
			}
			else {
				message.setMessageStatus(MessageStatus.INFLIGHT);
			}
			
			message.setPriorAttempts(0);
			message.setMessage(messageContent);
			messages.add(message);
		}
		
		int expectedMessagesPolled = 5;
		
		AcquireLock();
		File messagesFile = CreateQueueFile();
		
		// Clear the contents of the queue.
		try(PrintWriter pw = new PrintWriter(new FileWriter(messagesFile))) {   // append
			pw.print("");
		}
		
		try(PrintWriter pw = new PrintWriter(new FileWriter(messagesFile, true))) {   // append
			for(Message msg : messages) {
				pw.println(msg);
			}
		}
		
		ReleaseLock();
		
		ReceiveMessageRequest request = new ReceiveMessageRequest(queueUri, 10, visibilityTimeout);
		List<Message> polledMessages = serviceInstance.poll(request);
		
		assertEquals(expectedMessagesPolled, polledMessages.size());
		
    }
    
    
    @Test(timeout = 999)
	public void testVisibilityTimeout() throws Exception {
		int msgBatchSize = 3;
		int visibilityTimeout = Integer.parseInt(config.getVisibilityTimeout());
		
		List<Message> inflightMessages = new ArrayList<Message>();
		// Add message to in-flight message collection with invisible from time set to 15 minutes 
	    // past the current time.
	    
		// Generate Send Message Request.
		Random randomGenerator = new Random();
		Calendar cal = Calendar.getInstance();
		Date invisibleFromTime = DateTimeUtility.addMinutesToDate(-10, cal.getTime());
		
		String messageContent = "File implementation of SQS - Message ID: " + Integer.toString(randomGenerator.nextInt(500));
		
		Message message = new Message();
		message.setMessageStatus(MessageStatus.INFLIGHT);
		// Set Prior Attempt to 1 indicating this message was attempted once earlier.
		message.setPriorAttempts(1);
		message.setMessage(messageContent);
		// Set InvisibleFromTime to 15 past the current time.
		message.setinvisibleFromTime(invisibleFromTime);
		inflightMessages.add(message);
		
		AcquireLock();
		File messagesFile = CreateQueueFile();
		
		// Clear the contents of the queue.
		try(PrintWriter pw = new PrintWriter(new FileWriter(messagesFile))) {   // append
			pw.print("");
		}
		
		try(PrintWriter pw = new PrintWriter(new FileWriter(messagesFile, true))) {   // append
			for(Message msg : inflightMessages) {
				pw.println(msg);
			}
		}
		
		ReleaseLock();
		
		ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest(queueUri,msgBatchSize, visibilityTimeout);
		List<Message> receivedMessages = (List<Message>)serviceInstance.poll(receiveRequest);
		
		int actual = receivedMessages != null ? receivedMessages.size() : 0;
		assertEquals(1, actual);
		
	}
    
	
	// ########## 	Helper methods for Testing FileQueueService ##########################
    
    /// <summary>
    /// Helper method to convert a given File into string representation. 
	/// </summary>
    ///<Returns>String representation of File</Returns>
    private String convertFileToString(File file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader (file));
	    String         line = null;
	    StringBuilder  stringBuilder = new StringBuilder();
	    
	    try {
	        while((line = reader.readLine()) != null) {
	            stringBuilder.append(line);
	            stringBuilder.append(System.getProperty("line.separator"));
	        }
	        
	        return stringBuilder.toString().trim();
	    } finally {
	        reader.close();
	    }
		
	}
	
    /// <summary>
    /// Helper method to find the total number of records (messages) in the given file. 
	/// </summary>
    ///<Returns>Number of records in File</Returns>
	private int totalRecordCount(File file) throws IOException {
		int count = 0;
		BufferedReader reader = null;
		try {
			
			reader = new BufferedReader(new FileReader (file));
	    
			while((reader.readLine()) != null) {
	            count++;
	        }
	    } finally {
	    		reader.close();
	    }
		
	    return count;
	}
	
	
	
	/// <summary>
    /// Helper method that invokes the private CreateQueueFile through Reflection to get 
	/// a file reference to queue message file. 
	/// </summary>
    ///<Returns>Returns the Queue Messages File.</Returns>
	private File CreateQueueFile() throws Exception {
		// Call the Get Queue to get reference to the messages file(File Queue).
		Method getMessageFile = FileQueueService.class.getDeclaredMethod("getQueue",String.class);
		getMessageFile.setAccessible(true);
		File messagesFile = (File)getMessageFile.invoke(serviceInstance,queueUri);
		
		return messagesFile;
	}
	
	/// <summary>
    /// Helper method that invokes the private acquireWriteLock through Reflection to get 
	/// lock to operate on the Messages file (Queue file). 
	/// </summary>
	private void AcquireLock() throws Exception {
		File lockFile = new File(queueUri + lockName);
		Method acquireWriteLock = FileQueueService.class.getDeclaredMethod("acquireWriteLock",File.class);
		acquireWriteLock.setAccessible(true);
		acquireWriteLock.invoke(serviceInstance,lockFile);
	}
	
	/// <summary>
    /// Helper method that invokes the private releaseWriteLock through Reflection to release 
	/// lock held on the Messages file (Queue file. 
	/// </summary>
	private void ReleaseLock() throws Exception {
		File lockFile = new File(queueUri + lockName);
		
		Method acquireWriteLock = FileQueueService.class.getDeclaredMethod("releaseWriteLock",File.class);
		acquireWriteLock.setAccessible(true);
		acquireWriteLock.invoke(serviceInstance,lockFile);
	}
}