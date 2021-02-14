package runner;

public abstract class IdentifiableRunnableCreator {
	
	public abstract void run(String name);
	
	public Runnable get(String name) {
		return new IdentifiableRunnable(name);
	}
	
	public class IdentifiableRunnable implements Runnable {
		String name;
		
		public IdentifiableRunnable(String name) {
			this.name = name;
		}

		@Override
		public void run() {
			IdentifiableRunnableCreator.this.run(this.name);
		}
		
	}
}
