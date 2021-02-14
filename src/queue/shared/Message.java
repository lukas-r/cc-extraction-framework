package queue.shared;

import java.io.Serializable;

public abstract class Message implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private Type type;
	
	public static enum Type {
		ADD, ADD_FIRST, POP, DONE, BACK, REMOVE, RESET, GET_TODO, GET_DONE, GET_PENDING, IS_FINISHED
	}
	
	public Message(Type type) {
		this.type = type;
	}
	
	public Type getType() {
		return this.type;
	}
}
