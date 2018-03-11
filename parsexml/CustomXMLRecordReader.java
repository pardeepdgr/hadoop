package parsexml;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CustomXMLRecordReader extends RecordReader<LongWritable, Text> {
	private byte[] startTag;
	private byte[] endTag;
	private long start;
	private long end;
	private FSDataInputStream fsin;
	private DataOutputBuffer buffer = new DataOutputBuffer();

	private LongWritable key = new LongWritable();
	private Text value = new Text();

	@Override
	public void close() throws IOException {
		fsin.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (fsin.getPos() - start) / (float) (end - start);
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		startTag = conf.get(CustomXMLInputFormat.startTagKey).getBytes("utf-8");
		endTag = conf.get(CustomXMLInputFormat.endTagKey).getBytes("utf-8");
		FileSplit fileSplit = (FileSplit) split;

		start = fileSplit.getStart();
		end = start + fileSplit.getLength();
		Path filePath = fileSplit.getPath();
		FileSystem fs = filePath.getFileSystem(conf);
		fsin = fs.open(filePath);
		fsin.seek(start);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (fsin.getPos() < end) {
			if (readUntillMatch(startTag, false)) {
				try {
					buffer.write(startTag);
					if (readUntillMatch(endTag, true)) {
						key.set(fsin.getPos());
						value.set(buffer.getData(), 0, buffer.getLength());
						return true;
					}
				} finally {
					buffer.reset();
				}
			}
		}
		return false;
	}

	private boolean readUntillMatch(byte[] match, boolean withinBlock)
			throws IOException {
		int i = 0;
		while (true) {
			int b = fsin.read();
			if (b == -1) {
				// end of the file
				return false;
			}
			if (withinBlock) {
				// save to buffer
				buffer.write(b);
			}
			if (b == match[i]) {
				i++;
				if (i > match.length)
					return true;
			} else {
				i = 0;
			}
			if (!withinBlock && i == 0 && fsin.getPos() >= end) {
				return false;
			}
		}
	}

}
