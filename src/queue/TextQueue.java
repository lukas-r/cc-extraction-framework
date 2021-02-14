package queue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import utils.FileLocker;

public class TextQueue implements Queue<String> {
	public final static int LOCK_WAITING_INTERVALL = 500;
	
	public final static String TODO_FILE_NAME = "todo.txt";
	public final static String PENDING_FILE_NAME = "pending.txt";
	public final static String DONE_FILE_NAME = "done.txt";
	
	private File path;
	private File todoFile;
	private File pendingFile;
	private File doneFile;
	
	public TextQueue(File path) {
		this.path = path;
		this.todoFile = new File(this.path, TODO_FILE_NAME);
		this.pendingFile = new File(this.path, PENDING_FILE_NAME);
		this.doneFile = new File(this.path, DONE_FILE_NAME);
		try {
			todoFile.createNewFile();
			pendingFile.createNewFile();
			doneFile.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static List<String> splitLines(String text) {
		List<String> lines = new ArrayList<String>(Arrays.asList(text.split("\\r?\\n")));
		lines.removeAll(Collections.singleton(""));
		return lines;
	}
	
	public static String joinLines(List<String> lines) {
		return String.join(System.lineSeparator(), lines);
	}
	
	private static String joinElements(List<QueueElement<String>> elements) {
		return elements.stream().map(e -> e.toString()).collect(Collectors.joining(System.lineSeparator()));
	}
	
	private static List<QueueElement<String>> parseElements(String text){
		return TextQueue.splitLines(text).stream().map(QueueElement::<String>fromString).collect(Collectors.toList());
	}
	
	@Override
	public void add(String element) {
		FileLocker locker = new FileLocker(this.todoFile);
		List<String> elements = TextQueue.splitLines(locker.read());
		if (!elements.contains(element)) {
			locker.appendLine(element);
		}
		locker.close();
	}
	
	@Override
	public void addFirst(String element) {
		FileLocker locker = new FileLocker(this.todoFile);
		String content = locker.read();
		List<String> elements = TextQueue.splitLines(content);
		if (!elements.contains(element)) {
			locker.write(element);
			locker.appendLine(content);
		}
		locker.close();
	}

	@Override
	public String pop(String name) {
		FileLocker todoLocker = new FileLocker(this.todoFile);
		List<String> elements = TextQueue.splitLines(todoLocker.read());
		if (elements.size() == 0) {
			todoLocker.close();
			return null;
		}
		String next = elements.get(0);
		todoLocker.write(TextQueue.joinLines(elements.subList(1, elements.size())));
		
		QueueElement<String> element = new QueueElement<String>(name, next, new Date());
		FileLocker pendingLocker = new FileLocker(this.pendingFile);
		pendingLocker.appendLine(element.toString());
		
		pendingLocker.close();
		todoLocker.close();
		
		return next;
	}

	@Override
	public void done(String element, String name) {
		FileLocker todoLocker = new FileLocker(this.todoFile);
		FileLocker pendingLocker = new FileLocker(this.pendingFile);
		FileLocker doneLocker = new FileLocker(this.doneFile);
		
		List<String> todoElements = TextQueue.splitLines(todoLocker.read());
		List<QueueElement<String>> pendingElements = TextQueue.parseElements(pendingLocker.read());
		
		QueueElement<String> done = new QueueElement<String>(name, element, new Date());
		
		if (todoElements.remove(element)) {
			todoLocker.write(TextQueue.joinLines(todoElements));
		}
		if (pendingElements.remove(done)) {
			pendingLocker.write(TextQueue.joinElements(pendingElements));
		}
		doneLocker.appendLine(done.toString());
		
		doneLocker.close();
		pendingLocker.close();
		todoLocker.close();
	}

	@Override
	public void back(String element, boolean first) {
		FileLocker todoLocker = new FileLocker(this.todoFile);
		FileLocker pendingLocker = new FileLocker(this.pendingFile);
		
		String todoContent = todoLocker.read();
		List<String> todoElements = TextQueue.splitLines(todoContent);
		List<QueueElement<String>> pendingElements = TextQueue.parseElements(pendingLocker.read());
		
		QueueElement<String> ele = new QueueElement<String>(null, element, null);

		if (pendingElements.remove(ele)) {
			pendingLocker.write(TextQueue.joinElements(pendingElements));
				if (!todoElements.contains(element)) {
					if (first) {
						todoLocker.write(element);
						todoLocker.appendLine(todoContent);
					} else {				
						todoLocker.appendLine(element);
				}
			}
		}
		
		pendingLocker.close();
		todoLocker.close();
	}

	@Override
	public void remove(String element) {
		FileLocker todoLocker = new FileLocker(this.todoFile);
		FileLocker pendingLocker = new FileLocker(this.pendingFile);
		
		List<String> todoElements = TextQueue.splitLines(todoLocker.read());
		List<QueueElement<String>> pendingElements = TextQueue.parseElements(pendingLocker.read());
		
		QueueElement<String> ele = new QueueElement<String>(null, element, null);
		
		if (todoElements.remove(element)) {
			todoLocker.write(TextQueue.joinLines(todoElements));
		}
		if (pendingElements.remove(ele)) {
			pendingLocker.write(TextQueue.joinElements(pendingElements));
		}
		pendingLocker.close();
		todoLocker.close();
	}
	
	@Override
	public void reset() {
		FileLocker todoLocker = new FileLocker(this.todoFile);
		FileLocker pendingLocker = new FileLocker(this.pendingFile);
		FileLocker doneLocker = new FileLocker(this.doneFile);
		
		todoLocker.write("");
		pendingLocker.write("");
		doneLocker.write("");
		
		doneLocker.close();
		pendingLocker.close();
		todoLocker.close();
	}

	@Override
	public String[] getTodo() {
		FileLocker todoLocker = new FileLocker(this.todoFile);
		List<String> todoElements = TextQueue.splitLines(todoLocker.read());
		todoLocker.close();
		return Arrays.copyOf(todoElements.toArray(), todoElements.size(), String[].class);
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueueElement<String>[] getPending() {
		FileLocker pendingLocker = new FileLocker(this.pendingFile);
		List<QueueElement<String>> pendingElements = TextQueue.parseElements(pendingLocker.read());
		pendingLocker.close();
		QueueElement<String>[] array = new QueueElement[pendingElements.size()];
		array = pendingElements.toArray(array);
		return array;
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueueElement<String>[] getDone() {
		FileLocker doneLocker = new FileLocker(this.doneFile);
		List<QueueElement<String>> doneElements = TextQueue.parseElements(doneLocker.read());
		doneLocker.close();
		return Arrays.copyOf(doneElements.toArray(), doneElements.size(), QueueElement[].class);
	}

	@Override
	public boolean isFinished() {
		FileLocker todoLocker = new FileLocker(this.todoFile);
		FileLocker pendingLocker = new FileLocker(this.pendingFile);
		
		List<String> todoElements = TextQueue.splitLines(todoLocker.read());
		List<QueueElement<String>> pendingElements = TextQueue.parseElements(pendingLocker.read());
		
		todoLocker.close();
		pendingLocker.close();
		
		return todoElements.isEmpty() && pendingElements.isEmpty();
	}

}
