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

import mapreduce.patterns.common.UserIdAndStars;

public class TexasRatingCounts_Q5 {

	public static class GenericMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String delims = "//^";
			String[] data = StringUtils.split(value.toString(), delims);
			if (data.length == 4) {
				try {
					double rating = Double.parseDouble(data[3]);
					context.write(new Text(data[2]), new DoubleWritable(rating));
				} catch (NumberFormatException e) {
					context.write(new Text(data[2]), new DoubleWritable(0.0));
				}
			} else if (data.length == 3) {
				if (data[1].contains("TX")) {
					context.write(new Text(data[0]), new DoubleWritable(-10.0));
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

		}
	}

	public static class ReviewReduce extends Reducer<Text, DoubleWritable, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			Text keyForMap = null;
			/**
			 * Output only those ratings for which UserId was found blank.
			 */
			for (DoubleWritable val : values) {
				if (val.get() > new DoubleWritable(-1.0).get()) {
					count++;
				} else {
					keyForMap = key;
				}
			}
			if (keyForMap != null) {
				context.write(keyForMap, new Text(count + ""));
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: TexasRatingCounts_Q5 <in_1> <in_1> <out>  ");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Texas Ratings");
		job.setJarByClass(TexasRatingCounts_Q5.class);

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