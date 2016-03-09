package mapreduce.patterns;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Top10Raters_Q4 {

	public static class GenericMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String delims = "//^";
			String[] data = StringUtils.split(value.toString(), delims);
			try {
				double rating = Double.parseDouble(data[3]);
				context.write(new Text(data[1]), new DoubleWritable(rating));
			} catch (NumberFormatException e) {
				context.write(new Text(data[1]), new DoubleWritable(0.0));
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

		}
	}

	public static class ReviewReduce extends Reducer<Text, DoubleWritable, Text, Text> {

		Map<Text, Integer> countMap = new HashMap<Text, Integer> ();
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for (DoubleWritable val : values) {
					count++;
			}
			countMap.put(key,count);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			for (Map.Entry<Text, Integer> entry : HadoopDataHelper.getTopNValues(countMap, 10).entrySet()) {
				context.write(new Text(entry.getKey().toString()), new Text(entry.getValue().toString()));
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 4) {
			System.err.println("Usage: Top10Raters_Q4 <in_1> <out>  ");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Pattenrns");
		job.setJarByClass(Top10Raters_Q4.class);

		job.setMapperClass(GenericMap.class);
		job.setReducerClass(ReviewReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(1);

		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		// set the HDFS path of the input data
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, GenericMap.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, GenericMap.class);
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		// Wait till job completion
		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}