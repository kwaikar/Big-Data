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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import mapreduce.patterns.StanfordRatings_Q3.GenericMap;

public class Top10Raters_Q4 {

	public static class GenericMap extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String delims = "//^";
			String[] data = StringUtils.split(value.toString(), delims);
			if (data.length == 4) {
				try {
				double rating = Double.parseDouble(data[3]);
				context.write(new Text(data[1]), new Text(""+rating));
			} catch (NumberFormatException e) {
				context.write(new Text(data[1]), new Text(""+0.0));
			}
		}else   {
			/**
			 * This is user Data, output the userId information.
			 */
				context.write(new Text(data[0]), new Text(data[1]+"~"+-10.0));
		}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

		}
	}

	public static class ReviewReduce extends Reducer<Text, Text, Text, Text> {

		Map<String, Integer> countMap = new HashMap<String, Integer> ();
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String name="";
			int count = 0;
			for (Text val : values) {
				if(val.toString().contains("~"))
				{
				name=val.toString().split("~")[0];	
				}
				else
				{
					count++;
				}
			}if(name.length()!=0){
			countMap.put(key.toString()+"~"+name,count);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			for (Map.Entry<String, Integer> entry :  HadoopDataHelper.getTopNValues(countMap, 10).entrySet()) {
				context.write(new Text(entry.getKey().split("~")[0] ), new Text(entry.getKey().split("~")[1]));
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: Top10Raters_Q4 <in_1> <in_2> <out>  ");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Pattenrns");
		job.setJarByClass(Top10Raters_Q4.class);

		job.setMapperClass(GenericMap.class);
		job.setReducerClass(ReviewReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(1);

		job.setMapOutputValueClass(Text.class);
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