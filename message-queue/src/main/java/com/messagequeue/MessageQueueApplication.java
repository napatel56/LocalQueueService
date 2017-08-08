package com.messagequeue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.messagequeue.config.ApplicationConfiguration;
import com.messagequeue.services.QueueService;


@SpringBootApplication
@ComponentScan("com.messagequeue")
public class MessageQueueApplication implements CommandLineRunner{

	@Autowired
	QueueService queueService;
	
	@Autowired
	private ApplicationConfiguration config;
	
	public static void main(String[] args) {
		
		SpringApplication.run(MessageQueueApplication.class, args);
	
	}

	@Override
    public void run(String... args) throws Exception {
		
		String queueUrl = config.getQueueUrl();
		
		int threads = Integer.parseInt(config.getWorkerThreads());
		int delayPublish = Integer.parseInt(config.getDelayPublish());
		int visibilityTimeout = Integer.parseInt(config.getVisibilityTimeout());
		int maxMessages = Integer.parseInt(config.getMaxMessages());
		
		ExecutorService prodExecutorService = Executors.newFixedThreadPool(threads);
		ExecutorService conExecutorService = Executors.newFixedThreadPool(threads);
		
		prodExecutorService.execute(new Producer(queueService, queueUrl,delayPublish,maxMessages));
		prodExecutorService.execute(new Producer(queueService, queueUrl,delayPublish,maxMessages));
		prodExecutorService.execute(new Producer(queueService, queueUrl,delayPublish,maxMessages));
		
		conExecutorService.execute(new Consumer(queueService, queueUrl, visibilityTimeout, maxMessages));
		
		prodExecutorService.execute(new Producer(queueService, queueUrl,delayPublish,maxMessages));
		
		conExecutorService.execute(new Consumer(queueService, queueUrl, visibilityTimeout, maxMessages));
		conExecutorService.execute(new Consumer(queueService, queueUrl, visibilityTimeout, maxMessages));
		conExecutorService.execute(new Consumer(queueService, queueUrl, visibilityTimeout, maxMessages));
	   
		prodExecutorService.shutdown();
		
		conExecutorService.shutdown();
		
		
	}
}
