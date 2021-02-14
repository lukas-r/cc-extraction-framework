package queue.shared;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.net.ssl.SSLSocketFactory;

import queue.Queue;
import queue.QueueElement;
import queue.shared.Message.Type;

public class SharedQueue<T> implements Queue<T> {
	
	String hostname;
	int port;
	
	public SharedQueue(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}
	
	private Socket getSocket() throws UnknownHostException, IOException {
		return SSLSocketFactory.getDefault().createSocket(this.hostname, this.port);
	}
	
	private <U> U sendCommand(Command<U> command) {
		Socket socket = null;
		ObjectOutputStream output = null;
		ObjectInputStream input = null;
		try {
			socket = this.getSocket();
			output = new ObjectOutputStream(socket.getOutputStream());
			input = new ObjectInputStream(socket.getInputStream());
			return command.send(input, output);
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
			return null;
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
			if (socket != null) {				
				try {
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void add(T element) {
		this.sendCommand((input, output) -> {
			output.writeObject(new ObjectMessage<T>(Type.ADD, element));
			return null;
		});
	}

	@Override
	public void addFirst(T element) {
		this.sendCommand((input, output) -> {
			output.writeObject(new ObjectMessage<T>(Type.ADD_FIRST, element));
			return null;
		});
	}

	@SuppressWarnings("unchecked")
	@Override
	public T pop(String name) {
		return this.sendCommand((input, output) -> {
			output.writeObject(new ObjectMessage<String>(Type.POP, name));
			return ((ObjectMessage<T>) input.readObject()).getObject();
		});
	}

	@Override
	public void done(T element, String name) {
		this.sendCommand((input, output) -> {
			output.writeObject(new TupleMessage<T, String>(Type.DONE, element, name));
			return null;
		});
	}

	@Override
	public void back(T element, boolean first) {
		this.sendCommand((input, output) -> {
			output.writeObject(new TupleMessage<T, Boolean>(Type.BACK, element, first));
			return null;
		});
	}

	@Override
	public void remove(T element) {
		this.sendCommand((input, output) -> {
			output.writeObject(new ObjectMessage<T>(Type.REMOVE, element));
			return null;
		});
	}

	@Override
	public void reset() {
		this.sendCommand((input, output) -> {
			output.writeObject(new EmptyMessage(Type.RESET));
			return null;
		});
	}

	@SuppressWarnings("unchecked")
	@Override
	public T[] getTodo() {
		return this.sendCommand((input, output) -> {
			output.writeObject(new EmptyMessage(Type.GET_TODO));
			return ((ObjectMessage<T[]>) input.readObject()).getObject();
		});
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueueElement<T>[] getPending() {
		return this.sendCommand((input, output) -> {
			output.writeObject(new EmptyMessage(Type.GET_PENDING));
			return ((ObjectMessage<QueueElement<T>[]>) input.readObject()).getObject();
		});
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueueElement<T>[] getDone() {
		return this.sendCommand((input, output) -> {
			output.writeObject(new EmptyMessage(Type.GET_DONE));
			return ((ObjectMessage<QueueElement<T>[]>) input.readObject()).getObject();
		});
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean isFinished() {
		return this.sendCommand((input, output) -> {
			output.writeObject(new EmptyMessage(Type.IS_FINISHED));
			return ((ObjectMessage<Boolean>) input.readObject()).getObject();
		});
	}
	
	private static interface Command<T> {
		
		public T send(ObjectInputStream input, ObjectOutputStream output) throws IOException, ClassNotFoundException;
		
	}

}
