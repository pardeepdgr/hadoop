package parsexml;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	private Text outputKey = new Text();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		context.write(new Text("<configuration>"), null);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			outputKey.set(constructPropertyXML(key, value));
			context.write(outputKey, null);
		}
	}

	private String constructPropertyXML(Text name, Text value) {
		StringBuilder sb = new StringBuilder();
		sb.append("<property><name>").append(name).append("</name><value>")
				.append(value).append("</value></property>");
		return sb.toString();
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		context.write(new Text("</configuration>"), null);
	}
}
