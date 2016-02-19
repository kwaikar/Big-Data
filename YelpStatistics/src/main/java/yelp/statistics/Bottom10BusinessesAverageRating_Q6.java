package yelp.statistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Find the lowest ten rated businesses using the average ratings. Recall that star
 * column in review.csv file represents the rating.
 */
public class Bottom10BusinessesAverageRating_Q6 {

	public static class ReviewMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String delims = "//^";
			String[] reviewData = StringUtils.split(value.toString(), delims);
			if (reviewData.length == 4) {
				try {
					double rating = Double.parseDouble(reviewData[3]);
					context.write(new Text(reviewData[2]), new DoubleWritable(rating));
				} catch (NumberFormatException e) {
					context.write(new Text(reviewData[2]), new DoubleWritable(0.0));
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

		}
	}

	public static class ReviewReduce extends Reducer<Text, DoubleWritable, Text, Text> {

		private Map<String, Double> countMap = new HashMap<String, Double>();

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			double sum = 0.0;

			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}
			Double avg = ((double) sum / (double) count);
			countMap.put(key.toString(), avg);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			for (Map.Entry<String, Double> entry : HadoopDataHelper.getBottomNValues(countMap, 10).entrySet()) {
				context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
			}
		}


	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
		if (otherArgs.length != 2) {
			System.err.println("Usage: Bottom10BusinessesAverageRating_Q6 <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Bottom 10 Ratings");
		job.setJarByClass(Bottom10BusinessesAverageRating_Q6.class);

		job.setMapperClass(ReviewMap.class);
		job.setReducerClass(ReviewReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(1);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}