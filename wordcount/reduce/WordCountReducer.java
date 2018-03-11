package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	private LongWritable output = new LongWritable();
	@Override
	public void reduce(Text word, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException{
		long sum = 0;
		
		for(LongWritable value : values){
			sum = sum + value.get();
		}
		output.set(sum);
		context.write(word, output);
	}
}
