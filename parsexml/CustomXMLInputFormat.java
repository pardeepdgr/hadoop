package parsexml;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class CustomXMLInputFormat extends TextInputFormat {
	public static final String startTagKey = "xmlinput.start";
	public static final String endTagKey = "xmlinput.end";

	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
			TaskAttemptContext context) {
		return new CustomXMLRecordReader();
	}

}
