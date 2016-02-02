package twitter.topicextractor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.LocalDate;

import twitter.topicextractor.TrendExtractor.IntSumReducer;
import twitter.topicextractor.TrendExtractor.TokenizerMapper;
import twitter4j.Query;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

/**
 * This class is responsible for downloading tweets from Twitter account. This
 * class exposes method that accepts the
 * 
 * @author Kanchan Waikar Date Created : 7:11:21 AM
 *
 */
public class TweetDownloader {
	public static final DateFormat TWITTER_DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

	public static void main(String[] args) {
		try {
			if (args.length != 3) {
				System.out.println("args" + args.length);
				System.out.println(
						"Command Format : java -jarname [HDFS_Input_DIR] [HDFS_OUTPUT_DIR] [TWITTER_SEARCH_QUERY]");
			} else {
				if (HadoopDataHelper.doesFileOrFolderExist(args[0])) {
					System.out.println("File/Folder already exists: " + args[0]);
					System.exit(1);
				}
				if (HadoopDataHelper.doesFileOrFolderExist(args[1])) {
					System.out.println("File/Folder already exists: " + args[1]);
					System.exit(1);
				}
				downloadTweetsToHadoopCluster(args[0], args[2]);

				Job job = Job.getInstance(HadoopDataHelper.getMapReduceConfiguration(), "TweetCount");
				job.setJarByClass(TrendExtractor.class);
				job.setMapperClass(TokenizerMapper.class);
				job.setCombinerClass(IntSumReducer.class);
				job.setReducerClass(IntSumReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			}
		} catch (TwitterException te) {
			te.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method downloads tweets for given string to the directory on hadoop
	 * cluster specified.
	 * 
	 * @param hdfsDirectory
	 *            - Input directory in which tweeter input files need to be
	 *            stored
	 * @param queryString
	 *            - Query to be searched on twitter
	 * @throws TwitterException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public static void downloadTweetsToHadoopCluster(String hdfsDirectory, String queryString)
			throws TwitterException, IOException, MalformedURLException {
		LocalDate input = new LocalDate();

		for (int i = 0; i < 6; i++) {
			Twitter twitter = TwitterFactory.getSingleton();
			Query query = new Query(queryString);
			StringBuilder sb = new StringBuilder();
			input = new LocalDate(input.minusDays(1));
			query.setSince(TWITTER_DATE_FORMATTER.format(input.toDate()));
			query.setCount(500);
			query.setUntil(TWITTER_DATE_FORMATTER.format(input.plusDays(1).toDate()));
			List<Status> statuses = twitter.search(query).getTweets();
			for (Status status : statuses) {
				sb.append(status.getText().replaceAll("[\n|\r]", "") + "\n");
			}
			System.out.println("Number of tweets found for date range " + query.getSince() + " - " + query.getUntil()
					+ " : " + statuses.size());
			HadoopDataHelper.downloadFileToHDFS(
					new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8)),
					hdfsDirectory + "input_" + i + ".txt");
			twitter=null;
		}
	}
}
