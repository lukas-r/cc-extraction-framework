package queue.shared;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import javax.net.ssl.SSLServerSocketFactory;

import queue.Queue;

public class SharedQueueProvider<T> {
	ServerSocket socket;
	Queue<T> queue;
	
	public SharedQueueProvider(Queue<T> queue, int port) {
		try {
			this.socket = SSLServerSocketFactory.getDefault().createServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.queue = queue;
	}
	
	public void listen() {
		while(true) {
			try {
				Socket client = this.socket.accept();
				
				SharedQueueThread<T> thread = new SharedQueueThread<T>(client, this.queue);
				thread.run();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
