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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import mapreduce.patterns.common.BusinessAndRating;

/**
 * Find the top ten rated businesses using the average ratings. Recall that star
 * column in review.csv file represents the rating.
 */
public class Top10BusinessesAverageRating_Q1 {

	public static class ReviewMap extends Mapper<LongWritable, Text, Text, BusinessAndRating> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String delims = "//^";
			String[] data = StringUtils.split(value.toString(), delims);
			if (data.length == 4) {
				/**
				 * This is review data, output the rating
				 */
				try {
					double rating = Double.parseDouble(data[3]);
					context.write(new Text(data[2]),
							new BusinessAndRating(new Text(data[2]), new DoubleWritable(rating)));
				} catch (NumberFormatException e) {
					context.write(new Text(data[2]), new BusinessAndRating(new Text(data[2]), new DoubleWritable(0.0)));
				}
			} else if (data.length == 3) {
				/**
				 * This is business Data, output the address and categories
				 * information.
				 */
				context.write(new Text(data[0]),
						new BusinessAndRating(new Text(data[0]), new Text(data[1]), new Text(data[2])));
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

		}
	}

	public static class ReviewReduce extends Reducer<Text, BusinessAndRating, Text, Text> {

		Map<BusinessAndRating, Double> countMap = new HashMap<BusinessAndRating, Double>();

		@Override
		public void reduce(Text key, Iterable<BusinessAndRating> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			double sum = 0.0;
			BusinessAndRating keyForMap = null;
			for (BusinessAndRating val : values) {
				if (val.getRating().get() != -1) {
					sum += val.getRating().get();
					count++;
				} else {
					keyForMap = val;
				}
			}
			Double avg = ((double) sum / (double) count);
			if (keyForMap != null) {
				countMap.put(keyForMap, avg);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			for (Map.Entry<BusinessAndRating, Double> entry : HadoopDataHelper.getTopNValues(countMap, 10).entrySet()) {
				context.write(new Text(entry.getKey().toString()), new Text(entry.getValue().toString()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: Top10BusinessesAverageRating_Q1 <in_1> <in_2> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Top 10 Ratings");
		job.setJarByClass(Top10BusinessesAverageRating_Q1.class);

		job.setMapperClass(ReviewMap.class);
		job.setReducerClass(ReviewReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(1);

		job.setMapOutputValueClass(BusinessAndRating.class);
		job.setOutputValueClass(Text.class);

		// set the HDFS path of the input data
		MultipleInputs.addInputPath(job,new Path(otherArgs[0]), TextInputFormat.class, Top10BusinessesAverageRating_Q1.ReviewMap.class);
		MultipleInputs.addInputPath(job,new Path(otherArgs[1]), TextInputFormat.class, Top10BusinessesAverageRating_Q1.ReviewMap.class);
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		// Wait till job completion
		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}