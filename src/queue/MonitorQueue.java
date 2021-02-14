package queue;

import java.util.Date;
import java.util.concurrent.Semaphore;

public class MonitorQueue<T> implements Queue<T> {
	public final Queue<T> queue;
	final protected int timeout;
	final private int recheckInterval;
	
	private MethodRunner timeoutChecker;
	private Semaphore busy;
	
	public MonitorQueue(Queue<T> queue, int timeout, int recheckInterval) {
		this.queue = queue;
		this.timeout = timeout;
		this.recheckInterval = recheckInterval;
		this.busy = new Semaphore(1, true);
		this.startMonitor();
	}
	
	public void startMonitor() {
		if (this.timeoutChecker == null) {
			this.timeoutChecker = new MethodRunner(this.recheckInterval, this::checkTimeout);
			this.timeoutChecker.start();
		}
	}
	
	public void stopMonitor() {
		if (this.timeoutChecker != null) {
			this.timeoutChecker.stop();
			this.timeoutChecker = null;
		}
	}
	
	private void checkTimeout() {
		try {
			this.busy.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		Date now = new Date();
		QueueElement<T>[] pending = this.queue.getPending();
		for (QueueElement<T> element: pending) {
			if (now.getTime() - element.date.getTime() > timeout * 1000) {
				this.queue.back(element.element, false);
			}
		}
		this.busy.release();
	}

	@Override
	public void add(T element) {
		try {
			this.busy.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.queue.add(element);
		this.busy.release();
	}
	
	@Override
	public void addFirst(T element) {
		try {
			this.busy.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.queue.addFirst(element);
		this.busy.release();
	}

	@Override
	public T pop(String name) {
		try {
			this.busy.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		T element = this.queue.pop(name);
		this.busy.release();
		return element;
	}

	@Override
	public void done(T element, String name) {
		try {
			this.busy.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.queue.done(element, name);
		this.busy.release();
	}
	
	@Override
	public void back(T element, boolean first) {
		try {
			this.busy.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.queue.back(element, first);
		this.busy.release();
	}

	@Override
	public void remove(T element) {
		try {
			this.busy.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.queue.remove(element);
		this.busy.release();
	}
	
	@Override
	public void reset() {
		try {
			this.busy.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.queue.reset();
		this.busy.release();
	}

	@Override
	public T[] getTodo() {
		try {
			this.busy.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		T[] todo = this.queue.getTodo();
		this.busy.release();
		return todo;
	}

	@Override
	public QueueElement<T>[] getPending() {
		try {
			this.busy.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		QueueElement<T>[] pending = this.queue.getPending();
		this.busy.release();
		return pending;
	}

	@Override
	public QueueElement<T>[] getDone() {
		try {
			this.busy.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		QueueElement<T>[] done = this.queue.getDone();
		this.busy.release();
		return done;
	}

	@Override
	public boolean isFinished() {
		try {
			this.busy.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		boolean finished = this.queue.isFinished();
		this.busy.release();
		return finished;
	}

}
