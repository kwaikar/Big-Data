package yelp.statistics;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * For all the businesses that are located in “Palo Alto”, output their full
 * address and also how many businesses are in each address. You can use the
 * full_address column as the filter column.
 * 
 * @author Kanchan Waikar
 * Date Created : Feb 10, 2016 - 9:20:48 AM
 */
public class BusinessesFromPaloAlto_Q1 {

	public static class BusinessMap extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// from business
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(), delims);

			if (businessData.length == 3) {
				if (businessData[1].contains("Palo Alto"))
					context.write(new Text(businessData[1]), new IntWritable(1));
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for (IntWritable t : values) {
				count++;
			}

			context.write(key, new IntWritable(count));

		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get
																						// all
																						// args
		if (otherArgs.length != 2) {
			System.err.println("Usage: CountYelpBusiness <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "CountYelp");
		job.setJarByClass(BusinessesFromPaloAlto_Q1.class);

		job.setMapperClass(BusinessMap.class);
		job.setReducerClass(Reduce.class);
		// uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);

		// set output key type

		job.setOutputKeyClass(Text.class);

		// set output value type
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
