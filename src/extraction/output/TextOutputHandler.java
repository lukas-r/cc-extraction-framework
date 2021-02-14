package extraction.output;

import java.io.IOException;
import java.io.Writer;

import extraction.logging.Logger;

public class TextOutputHandler extends OutputHandler {
	private Writer writer;
	private boolean headerWritten;

	public TextOutputHandler(Logger logger, Writer writer, boolean writeHeader) {
		super(logger);
		this.writer = writer;
		if (!writeHeader) {
			this.headerWritten = true;
		}
	}
	
	private void writeHeader(String[] attributes) {
		try {
			this.writer.append(String.join("\t", attributes));
			this.writer.append(System.lineSeparator());
			this.writer.flush();
		} catch (IOException e) {
			logger.error("Write header error", e);
		}
	}

	@Override
	public void output(OutputObject object) {
		if (!this.headerWritten) {
			this.writeHeader(object.getAttributes());
			this.headerWritten = true;
		}
		try {
			writer.write(String.join("\t", object.getData().values()));
			writer.write(System.lineSeparator());
			this.writer.flush();
		} catch (IOException e) {
			logger.error("Write error", e);
		}
	}
	
	@Override
	public void close() {
		try {
			this.writer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			this.writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
