package queue.shared;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import queue.Queue;
import queue.QueueElement;
import queue.shared.Message.Type;

public class SharedQueueThread<T> extends Thread {
	
	Socket socket;
	Queue<T> queue;
	
	public SharedQueueThread(Socket socket, Queue<T> queue) {
		this.socket = socket;
		this.queue = queue;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		ObjectInputStream input = null;
		ObjectOutputStream output = null;
		try {
			input = new ObjectInputStream(this.socket.getInputStream());
			output = new ObjectOutputStream(this.socket.getOutputStream());
			
			Message message = (Message) input.readObject();
			ObjectMessage<T> elementMessage;		
			switch(message.getType()) {
				case ADD:
					elementMessage = (ObjectMessage<T>) message; 
					queue.add(elementMessage.getObject());
					break;
				case ADD_FIRST:
					elementMessage = (ObjectMessage<T>) message; 
					queue.addFirst(elementMessage.getObject());
					break;
				case BACK:
					TupleMessage<T, Boolean> elementBooleanMessage;
					elementBooleanMessage = (TupleMessage<T, Boolean>) message;
					queue.back(elementBooleanMessage.getObject1(), elementBooleanMessage.getObject2());
					break;
				case DONE:
					TupleMessage<T, String> elementStringMessage;
					elementStringMessage = (TupleMessage<T, String>) message;
					queue.done(elementStringMessage.getObject1(), elementStringMessage.getObject2());
					break;
				case GET_DONE:
					output.writeObject(new ObjectMessage<QueueElement<T>[]>(Type.GET_DONE, queue.getDone()));
					break;
				case GET_PENDING:
					output.writeObject(new ObjectMessage<QueueElement<T>[]>(Type.GET_PENDING, queue.getPending()));
					break;
				case GET_TODO:
					output.writeObject(new ObjectMessage<T[]>(Type.GET_TODO, queue.getTodo()));
					break;
				case IS_FINISHED:
					output.writeObject(new ObjectMessage<Boolean>(Type.IS_FINISHED, new Boolean(queue.isFinished())));
					break;
				case POP:
					ObjectMessage<String> stringMessage;
					stringMessage = (ObjectMessage<String>) message;
					T element = queue.pop(stringMessage.getObject());
					output.writeObject(new ObjectMessage<T>(Type.POP, element));
					break;
				case REMOVE:
					elementMessage = (ObjectMessage<T>) message; 
					queue.remove(elementMessage.getObject());
					break;
				case RESET:
					queue.reset();
					break;
				default:
					break;
			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				this.socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
}
