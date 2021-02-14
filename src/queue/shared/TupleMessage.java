package queue.shared;

public class TupleMessage<T, U> extends Message {
	
	private static final long serialVersionUID = 1L;
	private T object1;
	private U object2;

	public TupleMessage(Type type, T object1, U object2) {
		super(type);
		this.object1 = object1;
		this.object2 = object2;
	}
	
	public T getObject1() {
		return this.object1;
	}
	
	public U getObject2() {
		return this.object2;
	}

}
