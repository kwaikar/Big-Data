package bigdata.combiner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * This class implements In mapper combiner. It implements associative array in which it accumulates frequency of each word.
 * @author Kanchan Waikar
 * Date Created : Feb 17, 2016 - 6:24:09 PM
 *
 */
public class WordCountWithInMapperCombiner {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();
		private IntWritable count = new IntWritable();
		private Map<String, Integer> countMap = new HashMap<String, Integer>();

		/**
		 * Input key - offset of the word in the file, input value - input
		 * value. Text - output of the mapper IntWritable - counter
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				Integer count = countMap.get(token);
				if (count == null) {
					count = 0;
				}
				countMap.put(token, (count + 1));

			}
		}

		@Override
		protected void cleanup(Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			/**
			 * Gets called at end of execution of the mapper. If there are three
			 * mappers, will get called thrice. Advantage over combiner is -
			 * combiner may or may not get called, but this definitely would.
			 */
			for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
				word.set(entry.getKey());
				count.set(entry.getValue());
				context.write(word, count);
			}

			super.cleanup(context);
		}
	}

	/**
	 * Two input and two outputs. Context represents framework object.
	 * 
	 * @author kanchan
	 *
	 */
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

if(args.length!=2)
	{
	System.out.println("Args expected : [FILE_INPUT_DIRECTORY] [OUTPUT_DIRECTORY]");
	}
Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
		conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
		conf.set("mapreduce.framework.name", "yarn");
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountWithInMapperCombiner.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}