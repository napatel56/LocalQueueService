package com.services.model;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Message {
	
	private MessageStatus status;
	private int priorAttempts;
	private Date invisibleFromTime;
	private int receiptId;
	private String content;
	
	public Message() {}
	
	public Message(Date invisibleFromTime, String content ){
		super();
		this.invisibleFromTime = invisibleFromTime;
		this.content = content;
		this.status= MessageStatus.NEW;
	}
	
	public String getMessage(){
		return content;
	}
	
	public void setMessage(String content){
		this.content = content;
	}
	
	public int getPriorAttempts(){
		return priorAttempts;
	}
	
	public void setPriorAttempts(int priorAttempts){
		this.priorAttempts = priorAttempts;
	}
	
	public void setinvisibleFromTime(Date invisibleFromTime){
		this.invisibleFromTime = invisibleFromTime;
	}
	
	public Date getinvisibleFromTime(){
		return invisibleFromTime;
	}
	
	public int getReceiptId(){
		return receiptId;
	}
	
	public void setReceiptId(int receiptId){
		this.receiptId = receiptId;
	}
	
	public MessageStatus getMessageStatus(){
		return status;
	}
	
	public void setMessageStatus(MessageStatus status){
		this.status = status;
	}
	
	public String toString(){
		Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String vFrom = (invisibleFromTime != null) ? formatter.format(invisibleFromTime):"";
		
		return status + "|" +
				Integer.toString(priorAttempts) + "|" +  
						vFrom + "|" + 
					Integer.toString(receiptId) + "|" +
					content;
	}
}