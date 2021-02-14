package utils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

import com.google.common.base.Splitter;

public class FileLocker {
	final static Charset CHARSET = StandardCharsets.UTF_8;
	final static int BUFFER_SIZE = 8;
	
	private static Map<String, Semaphore> blockers = new HashMap<String, Semaphore>();
	
	File file;
	FileChannel channel;
	FileLock lock;
	
	public FileLocker(File file) {
		this.file = file;
		try {
			String path = file.getCanonicalPath();
			Semaphore semaphore;
			synchronized (FileLocker.blockers) {
				semaphore = blockers.get(path);
				if (semaphore == null) {
					semaphore = new Semaphore(1);
					FileLocker.blockers.put(path, semaphore);
				}
			}
			try {
				semaphore.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			this.channel = FileChannel.open(this.file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
			this.lock = this.channel.lock();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String read() {
		try {
			this.channel.position(0);
			StringBuilder content = new StringBuilder();
			while (true) {
				ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
				int len = this.channel.read(buffer);
				if (len < 0) {
					break;
				}
				byte[] bytes = len < BUFFER_SIZE ? Arrays.copyOf(buffer.array(), len) : buffer.array();
				content.append(new String(bytes, CHARSET));
			}
			return content.toString();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private void write(String text, boolean append) {
		try {
			if (append) {
				this.channel.position(this.channel.size());
			} else {
				this.channel.truncate(0);
			}
			Iterable<String> chunks = Splitter.fixedLength(BUFFER_SIZE).split(text);
			for (String chunk: chunks) {
				this.channel.write(ByteBuffer.wrap(chunk.getBytes(CHARSET)));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void write(String text) {
		this.write(text, false);
	}
	
	public void appendLine(String text) {
		try {
			if (this.channel.size() > 0) {	
				text = System.lineSeparator() + text;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.write(text, true);
	}
	
	public void close() {
		if (this.lock != null) {
			try {
				this.lock.release();
			} catch (IOException e) {
				e.printStackTrace();
			}
			this.lock = null;
		}
		if (this.channel != null) {
			try {
				this.channel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			this.channel = null;
		}
		synchronized (FileLocker.blockers) {
			try {
				synchronized (FileLocker.blockers) {					
					FileLocker.blockers.get(this.file.getCanonicalPath()).release();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
