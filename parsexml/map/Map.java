package parsexml;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String document = value.toString();
		try {
			XMLStreamReader reader = XMLInputFactory.newInstance()
					.createXMLStreamReader(
							new ByteArrayInputStream(document.getBytes()));
			String propertyName = "";
			String propertyValue = "";
			String currentElement = "";

			while (reader.hasNext()) {
				int code = reader.next();
				switch (code) {
				case XMLStreamConstants.START_ELEMENT:
					currentElement = reader.getLocalName();
					break;
				case XMLStreamConstants.CHARACTERS:
					if (currentElement.equalsIgnoreCase("name")) {
						propertyName += reader.getText();
					} else if (currentElement.equalsIgnoreCase("value")) {
						propertyValue += reader.getText();
					}
					break;
				}
			}
			reader.close();
			context.write(new Text(propertyName.trim()),
					new Text(propertyValue.trim()));
		} catch (XMLStreamException e) {
			e.printStackTrace();
		} catch (FactoryConfigurationError e) {
			e.printStackTrace();
		}
	}
}
