package com.messagequeue.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import com.services.model.Message;
import com.services.model.MessageStatus;
import com.services.model.DeleteMessageRequest;
import com.services.model.SendMessageRequest;
import com.services.model.ReceiveMessageRequest;
import com.utilities.DateTimeUtility;

@Service
@Profile("test")
public class FileQueueService implements QueueService {
	
	private static final String queueName = "messages";
	private static final String lockName = ".lock";
	private static AtomicInteger identity = new AtomicInteger();
		
	
	/// <summary>
    /// Publishes the Messages to the File Queue with Message Status as NEW indicating
    /// these messages are available for processing.
    /// </summary>
    /// <parameter name="SendMessageRequest">Represents Send Message Request.</parameter>
	@Override
	public void push(SendMessageRequest request) {
	    
    		File wlock = getWriteLockFile(request.getQueueUrl());
    		File queue = getQueue(request.getQueueUrl());
    		
	    acquireWriteLock(wlock);
	    
	    try (PrintWriter pw = new PrintWriter(new FileWriter(queue, true))){
	    		    	
	    		for (Message message : request.getAllMessages()) {
		        pw.println(message.toString());
		    }
	    } 
	    catch (IOException e) {
	    		e.printStackTrace();
	    } 
	    finally {
	    		releaseWriteLock(wlock);
	    }
	}
    
    /// <summary>
    /// Fetches newly queued messages limited by the Request Batch Size parameter of the Request
    /// and marks them as in-flight messages so that these messages are not available for other
    /// consumers.
    /// </summary>
    /// <parameter name="ReceiveMessageRequest">Represents Receive Request.</parameter>
    /// <Returns>Returns list of messages for processing to Consumer.</Returns>
	@Override
	public List<Message> poll(ReceiveMessageRequest request) {
		
    		String queueUri = request.getQueueUrl();
		File wLock = getWriteLockFile(queueUri);
		
		File queue = new File(queueUri + queueName);
		
		if (!queue.exists()) {
			System.out.println("There are no messages in the Queue waiting to be processed.");
    			return null;
		}
		
		Path path = queue.toPath();
		//DateFormat formatter =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		//String currTime = formatter.format(cal.getTime());
		Date currTime = cal.getTime();
		int visibilityTimeout = request.getVisibilityTimeout();
		List<Message> messagesToProcess = null;
		
		// Establishes lock to be able to retrieve the newly queued messages and mark them as in process.
    		acquireWriteLock(wLock);
    	
		try {
	    	
	    		List<Message> visTimeoutMessages = getVisibilityTimeoutMessages(path, visibilityTimeout, request.getRequestBatchSize());
	    	
			// Call the Newly queued messages are pulled.
	    		List<Message> filteredMessages = Files.lines(path)
												.filter(s -> s.contains(MessageStatus.NEW.toString()))
												.limit(request.getRequestBatchSize()-visTimeoutMessages.size())
												.map(p-> deserializeMessage(p)).collect(Collectors.toList());
			
	    	
	    		messagesToProcess = Stream.concat(visTimeoutMessages.stream(), filteredMessages.stream())
	    											.collect(Collectors.toList());
	    	
	    		/// Change the Message status of the message to indicate it is being picked up
		    /// for processing and will not be visible for other consumers.
		    	for(Message msg : messagesToProcess) {
		    		
		    		String originalMessage = msg.toString();
		    		int receiptId = identity.incrementAndGet();
		    		msg.setMessageStatus(MessageStatus.INFLIGHT);
		    		msg.setinvisibleFromTime(currTime);
		    		msg.setReceiptId(receiptId);
		    		
		    		replaceSelected(originalMessage, msg.toString(), queueUri);
		    		//changeMessageStatus(originalMessage,MessageStatus.INFLIGHT,currTime,identity.incrementAndGet(),queueUri))
		    	}
												
		    	if(filteredMessages.size() == 0 && visTimeoutMessages.size() == 0) {
		    		System.out.println("There are no messages in the Queue waiting to be processed.");
		    		return null;
		    	}
	    } 
	    catch (IOException e) {
			e.printStackTrace();
		} 
	    
	    finally{
	    		releaseWriteLock(wLock);
	    }
		
		return messagesToProcess;
	}

    /// <summary>
    /// Transforms the String Representation of the Message in to the Message object. 
    /// </summary>
    /// <parameter name="messageText">Represents string representation of the message.</parameter>
	@Override
	public void delete(DeleteMessageRequest request) {
		File wLock = getWriteLockFile(request.getQueueUrl());
		
		acquireWriteLock(wLock);
		
		try {
			
			if(request.getMessages().size() > 0) {
				String queue = getUpdatedQueue(request.getMessages(), request.getQueueUrl());
				writeToFile(queue, request.getQueueUrl());
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			releaseWriteLock(wLock);
		}
	}

	/// <summary>Converts the Message String into Message object.</summary>
    /// <parameter name = "messageText">String representation of the message.</parameter>
    /// <Returns>Message Object</Returns>
    private Message deserializeMessage(String messageText) {
    	
    	DateFormat formatter =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	String[] tokens = messageText.split("\\|");
    	
    	Message message = new Message();
    	
    	try {
    		message.setMessageStatus(MessageStatus.valueOf(tokens[0]));
    		message.setPriorAttempts(Integer.parseInt(tokens[1]));
    		
    		if(!tokens[2].isEmpty()) { 
    			message.setinvisibleFromTime(((Date)formatter.parse(tokens[2])));
    		}
    		
    		message.setReceiptId(Integer.parseInt(tokens[3]));
        	message.setMessage(tokens[4]);
    } 
    	catch (ParseException e) {
			e.printStackTrace();
		}
    	
    	return message;
    	
    }
    
    /// <summary>Retrieves messages with visibility timeout to be sent for reprocessing.</summary>
    /// <parameter name = "path">String file queue path.</parameter>
    /// <parameter name = "messageText">Represents visibility time out period.</parameter>
    /// <parameter name = "messageText">Represents the Batch Size of a single poll operation.</parameter>
    /// <Returns>List of In-flight messages with visibility timeout.</Returns>
    private List<Message> getVisibilityTimeoutMessages(Path path, int visibilityTimeout, int batchSize) throws IOException {
    	
    		List<Message> visibilityTimeoutMessages = new ArrayList<Message>();
    		Calendar cal = Calendar.getInstance();
		
		try {
	    	// in-flight messages collection maintains the messages picked up and currently being processed.
			List<Message> inflightMessages = Files.lines(path)
												.filter(s -> s.contains(MessageStatus.INFLIGHT.toString()))
												.limit(batchSize)
												.map(p-> deserializeMessage(p)).collect(Collectors.toList());
			
			for(Message msg : inflightMessages) {
				int duration = Math.abs(DateTimeUtility.minutesDiff(cal.getTime(), msg.getinvisibleFromTime()));
				
				if(duration > visibilityTimeout) {
					
					// Increment the prior attempt to indicate the message has been picked again for processing.
					int priorAttempt = msg.getPriorAttempts();
					msg.setPriorAttempts(priorAttempt++);
					visibilityTimeoutMessages.add(msg);
				}
			}
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		
		return visibilityTimeoutMessages;
    	
    }
    
	/*/// <summary>
    /// To Change the Message status of the message to indicate it is being picked up
    /// for processing and will not be visible for other consumers.
    /// </summary>
    /// <parameter name="originalMessage">Represents original message that is queued.</parameter>
    /// <parameter name="status">Represents the Status of the Message (NEW , INFLIGHT, PROCESSED).</parameter>
    /// <parameter name="currTime">Represents the time the message is picked for processing.</parameter>
    /// <parameter name="receiptId">Represents the Receipt Id of the message.</parameter>
    /// <parameter name="queueUri">Represents the Directory Location where the message file (Queue) is located.</parameter>
    private void changeMessageStatus(String originalMessage, MessageStatus status, String currTime, int receiptId, String queueUri){
		String[] tokens = originalMessage.split("\\|");
		tokens[0] = status.toString();
		tokens[2] = currTime;
		tokens[3] = Integer.toString(receiptId);
		
		String newMessage = String.join("|", tokens);
		
		try {
			replaceSelected(originalMessage, newMessage, queueUri);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	*/
    
	/// <summary>Acquires a lock on the File Queue so that other threads wanting to acquire lock
    /// on the File are kept waiting until the lock is released.</summary>
    /// <parameter>Represent the Lock File (Object) of the File Queue.</parameter>
	private void acquireWriteLock(File wLock) {
		
		while (!wLock.mkdir()) {
			try {
				System.out.println("Waiting to acquire");
				Thread.sleep(50);
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	
	/// <summary>Release the File Locked held to block Writing/ Reading into/from the Messages Queue.</summary>
    /// <parameter>Represent the Lock File (Object) of the File Queue.</parameter>
	private void releaseWriteLock(File wLock) {
		wLock.delete();
	}
	
	/// <summary>Creates message file Queue if it does not exists.</summary>
    /// <parameter>Represent message file queue URL.</parameter>
	/// <Returns>Reference to the message file queue.</Returns>
	private File getQueue(String queueUri) {
				
		File file = new File(queueUri + queueName);
		
		if(!file.exists()) {
			try {
				System.out.println("create directory");
				file.getParentFile().mkdir();
				System.out.println("cAbout to create file");
				file.createNewFile();
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return file;
	}
	
	/// <summary>Creates a .lock file to indicate Lock is acquired on the message file queue.</summary>
    /// <parameter>Represent message file queue URL.</parameter>
	/// <Returns>Reference to the .lock file.</Returns>
	private File getWriteLockFile(String queueUri) {
		
		File file = new File(queueUri + lockName);
		return file;
	}
	
	/// <summary>
    /// Finds the Text (message) to be replaced in the messages file and replace with the new updated message.
    /// </summary>
    /// <parameter name="originalText">Represents original message that is going to be modified.</parameter>
	/// <parameter name="replaceWith">Represents updated message.</parameter>
	/// <parameter name="queueUri">Represents the Directory Location where the message file (Queue) is located.</parameter>
	private void replaceSelected(String originalText, String replaceWith, String queueUri)  {
		Path path = Paths.get(queueUri, queueName);
	
		try {
			
			// Retrieves all the messages from the Queue.
			List<String> fileContent = new ArrayList<>(Files.readAllLines(path)); //.readAllLines(path, StandardCharsets.UTF_8));
			
			for (int i = 0; i < fileContent.size(); i++) {
				
				// Replace the original Test with the updated message.
			    if (fileContent.get(i).equals(originalText)) {
			        fileContent.set(i, replaceWith);
			        break;
			    }
			}
			
			
			// Write back the modified string array back to the queue file.
			Files.write(path, fileContent, StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
			
	    }
		catch (IOException e) {
			e.printStackTrace();
		}
		
				    
	}
	
	/// <summary>
    /// Gets the Updated the Queue File content after deletion of processed messages.
    /// </summary>
	/// <parameter name="deletedMessages">Represents list of messages to be deleted from the file Queue.</parameter>
	/// <parameter name="replaceWith">Represents File Queue URL.</parameter>
    private String getUpdatedQueue(List<Message> deletedMessages, String queueUri)   {
		
		String[] serializedMessages = new String[deletedMessages.size()];
	    
		int i = 0;
	    for(Message message : deletedMessages) {
	    		serializedMessages[i] = message.toString();
	    		i++;
	    }
	    
		String filePath = queueUri.concat(queueName);
		File file = new File(filePath);
		
		String currentLine;
	    //BufferedReader reader = null;
	    StringBuilder  stringBuilder = new StringBuilder();
	    
	    try (BufferedReader reader =new BufferedReader(new FileReader(file))){ 
	    				   
		    while((currentLine = reader.readLine()) != null) {
		    	
		    		boolean matched = false;
				for(String msg : serializedMessages)
			    {
					if(currentLine.equals(msg)) {
			    			matched = true;
			    			break;
					}
			    }
				
				if(matched) 
			    		continue;
				
			    stringBuilder.append(currentLine);
				stringBuilder.append(System.getProperty("line.separator"));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	    
	    return stringBuilder.toString().trim();
	}
    
    /// <summary>
    /// Writes the String (file content) on to the file queue.</summary>
    /// <parameter name="messages">Represents messages with each message seperated by a new line character.</parameter>
    /// <parameter name="replaceWith">Represents file Queue URL.</parameter>
	private void writeToFile(String messages, String queueUri) throws Exception  {
		String filePath = queueUri.concat(queueName);
		File messagesFile = new File(filePath);
		
		// Clear the contents of the queue.
		try(PrintWriter pw = new PrintWriter(new FileWriter(messagesFile))) {   
			pw.println(messages);
		}
	}
}