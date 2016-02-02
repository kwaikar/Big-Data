package twitter.topicextractor;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrendExtractor {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		Pattern pattern = Pattern.compile("#[^ ]*[ |\t|\n|\r|\f|$]");

		/**
		 * Input key - offset of the word in the file, input value - input
		 * value. Text - output of the mapper IntWritable - counter
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Matcher matcher = pattern.matcher(value.toString());
			while (matcher.find()) {
				word.set(matcher.group().trim());
				context.write(word, one);
			}
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
 
}