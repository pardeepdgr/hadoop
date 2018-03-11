package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {

	private Text word = new Text();
	private LongWritable count = new LongWritable();

	@Override
	protected void map(LongWritable key, Text line, Context context)
			throws IOException, InterruptedException {
		String[] words = line.toString().split("\t");
		word.set(words[0]);
		
		if (words.length > 2) {
			count.set(Long.parseLong(words[2]));
			context.write(word, count);
		}
	}
}
