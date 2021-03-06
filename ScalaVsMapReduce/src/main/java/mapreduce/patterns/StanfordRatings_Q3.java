package mapreduce.patterns;

import java.io.IOException;

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

public class StanfordRatings_Q3 {

	public static class GenericMap extends Mapper<LongWritable, Text, Text, UserIdAndStars> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String delims = "//^";
			String[] data = StringUtils.split(value.toString(), delims);
			if (data.length>3) {
				/**
				 * This is review data, output the rating
				 */
				try {
					double rating = Double.parseDouble(data[3]);
					context.write(new Text(data[2]), new UserIdAndStars(new Text(data[1]), new DoubleWritable(rating)));
				} catch (NumberFormatException e) {
					context.write(new Text(data[2]), new UserIdAndStars(new Text(data[1]), new DoubleWritable(0.0)));
				}
			} else {
				if (data[1].contains("Stanford")) {
					context.write(new Text(data[0]), new UserIdAndStars(new Text(""), new DoubleWritable(-10.0)));
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

		}
	}

	public static class ReviewReduce extends Reducer<Text, UserIdAndStars, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<UserIdAndStars> values, Context context)
				throws IOException, InterruptedException {

			Text keyForMap = null;
			/**
			 * Output only those ratings for which UserId was found blank.
			 */
			for (UserIdAndStars val : values) {
				if (val.getRating().get() <= -1.0) {
					keyForMap = key;
					break;
				}
			}
			if (keyForMap != null) {
				for (UserIdAndStars val : values) {
					if(val.getUserId().toString().length()>0)
					context.write(val.getUserId(), val.getRating());
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: StanfordRatings_Q3 <business_file> <review_file> <out> ");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Stanford ratings");
		job.setJarByClass(StanfordRatings_Q3.class);

		job.setMapperClass(GenericMap.class);
		job.setReducerClass(ReviewReduce.class);
		job.setOutputKeyClass(Text.class);

		job.setMapOutputValueClass(UserIdAndStars.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

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