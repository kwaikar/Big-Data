package yelp.statistics;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This class Executes Map-Reduce algorithm that extracts hashtags from the
 * tweet.
 * 
 * @author Kanchan Waikar Date Created : 7:38:09 PM
 *
 */
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

		 Map<String, IntWritable> countMap = new HashMap<String, IntWritable>();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			countMap.put(key.toString(),new IntWritable(sum));
		}
		/*
		 * With one reducer, cleanup method can be used in order to Select top "N" values from the output.
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Map<String, IntWritable> result = HadoopDataHelper.getTopNValues(countMap, 10);
			for (Map.Entry<String, IntWritable> entry : result.entrySet()) {
				context.write(new Text(entry.getKey()),entry.getValue());
			}
		}

	}

}