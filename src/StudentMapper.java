import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class StudentMapper extends MapReduceBase implements
		org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
	// FORM <InKeyType , InValueType, OutKeyType, OutValueType
	private final static IntWritable ret = new IntWritable(1);

	@Override
	public void map(LongWritable _key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		String[] Row = value.toString().split(",");
		System.out.println(Row[0] + " " + Row[1]);
		output.collect(new Text(Row[6]), ret);
	}
}
