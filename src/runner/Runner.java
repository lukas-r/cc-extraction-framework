package runner;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Runner {
	private final int threadNo;
	private final IdentifiableRunnableCreator prepare;
	private final IdentifiableRunnableCreator task;
	private final IdentifiableRunnableCreator cleanup;
	private final Supplier<Boolean> isDone;
	
	public Runner(int threadNo, IdentifiableRunnableCreator prepare, IdentifiableRunnableCreator task, IdentifiableRunnableCreator cleanup, Supplier<Boolean> isDone) {
		this.threadNo = threadNo;
		this.prepare = prepare;
		this.task = task;
		this.cleanup = cleanup;
		this.isDone = isDone;
	}
	
	public Runner(int threadNo, Consumer<String> prepare, Consumer<String> task, Consumer<String> cleanup, Supplier<Boolean> isDone) {
		this(threadNo, new IdentifiableRunnableCreator() {
			@Override
			public void run(String name) {
				prepare.accept(name);
			}
		}, new IdentifiableRunnableCreator() {
			@Override
			public void run(String name) {
				task.accept(name);
			}
		}, new IdentifiableRunnableCreator() {
			@Override
			public void run(String name) {
				cleanup.accept(name);
			}
		}, isDone);
	}
	
	public void run() {
		this.prepare.run("PREPARE");
		System.out.println("PREPARATION DONE");
		
		ExecutorService executor = Executors.newFixedThreadPool(threadNo);
		CompletionService<Integer> completionService = new ExecutorCompletionService<Integer>(executor);
		
		int running = 0;
		for (int i = 0; i < this.threadNo; i++) {
			System.out.println("SUBMIT TASK" + i);
			completionService.submit(this.task.get("TASK" + i), i);
			running++;
		}
		
		int nextThread = this.threadNo;
		while(!executor.isTerminated()) {
			int lastThread = -1;
			try {
				lastThread = completionService.take().get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			running--;
			if (!this.isDone.get()) {
				String threadName = "TASK" + (lastThread >= 0 ? lastThread: nextThread++);
				System.out.println("RESTART " + threadName);
				completionService.submit(this.task.get(threadName), lastThread);
			} else if (running == 0) {
				executor.shutdown();
				break;
			}
		}
		System.out.println("TASKS DONE");
		this.cleanup.run("CLEANUP");
		System.out.println("CLEANUP DONE");
	}

}
