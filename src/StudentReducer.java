import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class StudentReducer extends MapReduceBase implements
		org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {
	// FORM <InKeyType , InValueType, OutKeyType, OutValueType
	@Override
	public void reduce(Text _key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		Text key = _key;
		int frequencyForDateOfBirth = 0;
		while (values.hasNext()) {
			// replace ValueType with the real type of your value
			IntWritable value = values.next();
			frequencyForDateOfBirth += value.get();
			// process value
		}
		output.collect(key, new IntWritable(frequencyForDateOfBirth));
	}
}
