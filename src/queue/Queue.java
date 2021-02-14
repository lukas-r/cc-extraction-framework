package queue;

public interface Queue<T> {
	
	public void add(T element);
	
	public void addFirst(T element);
	
	public T pop(String name);	
	
	public void done(T element, String name);
	
	public void back(T element, boolean first);
	
	public void remove(T element);
	
	public void reset();
	
	public T[] getTodo();
	
	public QueueElement<T>[] getPending();
	
	public QueueElement<T>[] getDone();
	
	public boolean isFinished();
	
}
