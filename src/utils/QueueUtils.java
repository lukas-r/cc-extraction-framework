package utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.io.Files;

public class QueueUtils {
	
	public static void getQueueStrings(File inputFile, File outputFile) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split("\t");
				if (parts.length == 3) {
					try {
						writer.write(parts[1]);
						writer.newLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			writer.flush();
			reader.close();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Set<String> getFileDifference(File file1, File file2) {
		try {
			BufferedReader reader1 = new BufferedReader(new FileReader(file1));
			BufferedReader reader2 = new BufferedReader(new FileReader(file2));
			
			Set<String> set1 = new HashSet<String>();
			Set<String> set2 = new HashSet<String>();
			
			String line;
			while ((line = reader1.readLine()) != null) {
				set1.add(line);
			}
			while ((line = reader2.readLine()) != null) {
				set2.add(line);
			}
			
			SetView<String> diff = Sets.difference(set1, set2);
			
			reader1.close();
			reader2.close();
			
			return diff;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void removeUndone(File done, File folder) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(done));
			Set<String> files = new HashSet<String>();
			
			String line;			
			while ((line = reader.readLine()) != null) {
				String fileName = line.substring(line.lastIndexOf("/") + 1);
				fileName = fileName.substring(0, fileName.indexOf("."));
				files.add(fileName);
			}
			
			for (File subFile: folder.listFiles()) {
				if (subFile.isFile() && !files.contains(subFile.getName())) {
					subFile.delete();
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void deleteLastLine(File inputDir, File outputDir) {
		File[] files = inputDir.listFiles();
		int lastP = -1;
		for(int i = 0; i < files.length; i++) {
			if (!files[i].isFile()) {
				continue;
			}
			if (i * 100 / files.length > lastP) {
				lastP = i * 100 / files.length;
				System.out.println(lastP + "%");
			}
			try {
				RandomAccessFile f = new RandomAccessFile(files[i], "rw");
				long pos = f.length() - 1;
				while (pos >= 0) {
					f.seek(pos);
					if (f.readByte() == 10) {
						if (pos > 0) {
							f.seek(pos - 1);
							if (f.readByte() == 13) {
								pos -= 1;
							}
						}
						f.setLength(pos);
						break;
					}
					pos--;
				}
				f.close();
				Files.move(files[i], outputDir.toPath().resolve(files[i].getName()).toFile());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("100%");
	}
	
	public static void getRemaingTodos(List<String> done, List<String> todos, File output) {
		Map<String, String> map = new HashMap<String, String>();
		for (String todo: todos) {
			map.put(CommonCrawlSource.getFileNameFromUrl(todo), todo);
		}
		for (String file: done) {
			map.remove(file);
		}
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(output));
			for (String file: map.values()) {
				writer.write(file);
				writer.newLine();
			}
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
