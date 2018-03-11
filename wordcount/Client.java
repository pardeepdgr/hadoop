package wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Client {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Path inputDir = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Configuration config = new Configuration();
		Job job = new Job(config, "WordCount");
		job.setJarByClass(Client.class);

		job.setMapperClass(WordCountMapper.class);
//		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);

		FileSystem hdfs = FileSystem.get(config);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
		
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
	}

}
