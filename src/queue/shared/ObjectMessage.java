package queue.shared;

public class ObjectMessage<T> extends Message {
	
	private static final long serialVersionUID = 1L;
	private T object;

	public ObjectMessage(Type type, T object) {
		super(type);
		this.object = object;
	}
	
	public T getObject() {
		return this.object;
	}

}
