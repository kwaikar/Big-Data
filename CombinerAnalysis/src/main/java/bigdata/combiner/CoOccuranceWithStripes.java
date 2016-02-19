package bigdata.combiner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class CoOccuranceWithStripes extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(CoOccuranceWithStripes.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new CoOccuranceWithStripes(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		if(args.length!=5){
			System.out.println("Please provide 5 inputs : [INPUT_DIR] [OUTPUT_DIR] [STOP_WORDS_FILE] [MIN_WORD_LENGTH] [NUM_NEIGHBOURS]");
		}
		Job job = Job.getInstance(getConf(), "wordcount");
		job.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless job.setInputFormatClass is
		// used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
		job.addCacheFile(new Path(args[2]).toUri());
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputValueClass(Text.class);
		job.getConfiguration().setInt("MIN_WORD_LENGTH", Integer.parseInt(args[3]));
		job.getConfiguration().setInt("NUM_NEIGHBORS", Integer.parseInt(args[4]));

		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
		private final static IntWritable one = new IntWritable(1);
		private boolean caseSensitive = false;
		private String input;
		private Set<String> patternsToSkip = new HashSet<String>();
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		protected void setup(Mapper.Context context) throws IOException, InterruptedException {
			if (context.getInputSplit() instanceof FileSplit) {
				this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
			} else {
				this.input = context.getInputSplit().toString();
			}
			Configuration config = context.getConfiguration();
			this.caseSensitive = config.getBoolean("wordcount.case.sensitive", true);
			if (config.getBoolean("wordcount.skip.patterns", false)) {
				URI[] localPaths = context.getCacheFiles();
				parseSkipFile(localPaths[0]);
			}
		}

		private void parseSkipFile(URI patternsURI) {
			LOG.info("Added file to the distributed cache: " + patternsURI);
			BufferedReader fis = null;

			try {
				fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
				String pattern;
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '" + patternsURI + "' : "
						+ StringUtils.stringifyException(ioe));
			} finally {
				try {
					fis.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		Text word = new Text();

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			int minWordLength = context.getConfiguration().getInt("MIN_WORD_LENGTH", 5);
			int neighbors = context.getConfiguration().getInt("NUM_NEIGHBORS", 5);
			MapWritable occurrenceMap = new MapWritable();
			String line = lineText.toString();
			if (!caseSensitive) {
				line = line.toLowerCase();
			}
			List<String> validWords = new ArrayList<String>();
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.trim().length() == minWordLength) {
					if (word.isEmpty() || patternsToSkip.contains(word)) {
						continue;
					}
					validWords.add(word.trim());
				}
			}

			if (validWords.size() > 1) {
				for (int i = 0; i < validWords.size(); i++) {

					int start = (i - neighbors < 0) ? 0 : i - neighbors;
					int end = (i + neighbors >= validWords.size()) ? validWords.size() - 1 : i + neighbors;
					for (int j = start; j <= end; j++) {
						if (j != i) {
							Text neighbor = new Text(validWords.get(j));
							if (occurrenceMap.containsKey(neighbor)) {
								IntWritable count = (IntWritable) occurrenceMap.get(neighbor);
								count.set(count.get() + 1);
							} else {
								occurrenceMap.put(neighbor, new IntWritable(1));
							}
						}
					}
					word.set(validWords.get(i));
					context.write(word, occurrenceMap);
				}

			}
		}
	}

	public static class Reduce extends Reducer<Text, MapWritable, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<MapWritable> maps, Context context)
				throws IOException, InterruptedException {
			MapWritable finalMap = new MapWritable();
			for (MapWritable map : maps) {
				for (Entry<Writable, Writable> entry : map.entrySet()) {
					IntWritable value = (IntWritable) finalMap.get(entry.getKey());
					if(value==null)
					{
						finalMap.put(entry.getKey(),entry.getValue());
					}
					else
					{
						finalMap.put(entry.getKey(), new IntWritable(value.get()+((IntWritable)entry.getValue()).get()));
					}
				}
			}
			StringBuilder sb = new StringBuilder("[");
			for (Entry<Writable, Writable> entry : finalMap.entrySet()) {
				if(sb.toString().length()!=1)
				{
					sb.append(",");
				}
				sb.append(entry.getKey()+":"+entry.getValue());
			}
			sb.append("]");
			context.write(word, new Text(sb.toString()));
		}
	}
}
