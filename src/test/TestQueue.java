package test;

import java.io.File;

import queue.Queue;
import queue.TextQueue;

public class TestQueue {
	
	public static void testFileQueue() {
		File queueDir = new File("C:\\Users\\Lukas\\Desktop");
		Queue<String> queue = new TextQueue(queueDir);
		queue.add("test");
		queue.add("abc");
		queue.add("abc");
		queue.add("def");
		System.out.println(queue.pop("worker"));
		System.out.println(queue.pop("worker"));
		queue.done("abc", "worker1");
		queue.back("def", false);
	}
	
	public static void testMonitorQueue() {
		
	}
	
	public static void main(String[] args) {
		testFileQueue();
	}

}
