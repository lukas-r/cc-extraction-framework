package queue;

public class MethodRunner {
	private int interval;
	private Runnable method;
	
	Runner runner;
	Thread daemon;
	
	public MethodRunner(int interval, Runnable method) {
		this.interval = interval;
		this.method = method;
	}
	
	public void start() {
		if (this.runner == null) {;
			this.runner = new Runner();
			this.daemon = new Thread(this.runner);
		}
	}
	
	public void stop() {
		this.runner.run = false;
		this.daemon.interrupt();
		this.runner = null;
		this.daemon = null;
	}
	
	private class Runner implements Runnable {
		private boolean run;
		
		@Override
		public void run() {
			while(this.run) {
				try {
					this.wait(interval);
					method.run();
				} catch (InterruptedException e) {
					
				}
			}
		}
		
	}

}
