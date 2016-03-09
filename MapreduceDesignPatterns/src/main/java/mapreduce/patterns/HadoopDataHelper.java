package mapreduce.patterns;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Progressable;

/**
 * This class contains helper methods for downloading Data in Hadoop Cluster
 * 
 * @author Kanchan Waikar Date Created : 9:29:12 PM
 *
 */
public class HadoopDataHelper {

	private static final String CORE_SITE_CONF_PATH = "/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml";
	private static final String HDFS_SITE_CONF_PATH = "/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml";
	private static final String MAP_REDUCE_TRACKER = "mapred.job.tracker";
	private static final String HADOOP_PORT = "hdfs://cshadoop1:61120";
	private static final String YARN_ADDRESS = "yarn.resourcemanager.address";
	private static final String YARN_URL = "cshadoop1.utdallas.edu:8032";
	private static final String MAP_REDUCE_FRAMEWORK_NAME = "mapreduce.framework.name";
	private static final String YARN = "yarn";

	/**
	 * This function returns Hadoop Configuration
	 * 
	 * @return - HDFS Cluster configuration
	 */
	public static Configuration getHdfsConfiguration() {
		Configuration conf = new Configuration();
		conf.addResource(new Path(CORE_SITE_CONF_PATH));
		conf.addResource(new Path(HDFS_SITE_CONF_PATH));
		return conf;
	}

	/**
	 * This method checks whether file or folder specified by URL exists or not.
	 * 
	 * @param hdfsPath
	 *            - Input HDFS path.
	 * @return flag - boolean value
	 * @throws IOException
	 */
	public static boolean doesFileOrFolderExist(String hdfsPath) throws IOException {
		return FileSystem.get(URI.create(hdfsPath.trim()), HadoopDataHelper.getHdfsConfiguration())
				.exists(new Path(hdfsPath));
	}

	/**
	 * This method creates a Mapreduce configuration and returns the same.
	 * 
	 * @return - Mapreduce Configuration
	 */
	public static Configuration getMapReduceConfiguration() {
		Configuration conf = new Configuration();
		conf.set(MAP_REDUCE_TRACKER, HADOOP_PORT);
		conf.set(YARN_ADDRESS, YARN_URL);
		conf.set(MAP_REDUCE_FRAMEWORK_NAME, YARN);
		return conf;
	}

	/**
	 * This function downloads file from input stream to the destination file.
	 * 
	 * @param input
	 *            - InputStream of The file that needs to be saved.
	 * @param destFileName
	 *            - Destination hadoop cluster URL to which file needs to be
	 *            downloaded
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public static void downloadFileToHDFS(InputStream input, String destFileName)
			throws IOException, MalformedURLException {
		Configuration conf = getHdfsConfiguration();
		FileSystem fs = FileSystem.get(URI.create(destFileName), conf);
		if (!fs.exists(new Path(destFileName))) {
			OutputStream out = fs.create(new Path(destFileName), new Progressable() {
				public void progress() {
					System.out.print("...");
				}
			});
			/**
			 * Close true so that streams are closed.
			 */
			IOUtils.copyBytes(input, out, 4096, true);
			System.out.println("Download complete :" + destFileName);
		} else {
			System.out.println("Aborting download: File already exists at destination path");
		}
	}

	/**
	 * This method returns top N values based on the valueof the key.
	 * 
	 * @param countMap
	 * @return
	 */
	public  static <T,K extends Comparable<K>> Map<T, K> getTopNValues(Map<T, K> countMap, Integer count) {
		List<Map.Entry<T, K>> list = new LinkedList<Map.Entry<T, K>>(countMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<T, K>>() {
			public int compare(Map.Entry<T, K> o1, Map.Entry<T, K> o2) {
				return ((o2).getValue()).compareTo((o1).getValue());
			}
		});

		int counter = 0;
		Map<T, K> result = new LinkedHashMap<T, K>();
		Iterator<Map.Entry<T, K>> it = list.iterator();
		
		while (it.hasNext() && counter < count) {
			Map.Entry<T, K> entry = (Map.Entry<T, K>) it.next();
			result.put(entry.getKey(), entry.getValue());
			counter++;
		}
		return result;
	}
	
	/**
	 * This method returns top N values based on the valueof the key.
	 * 
	 * @param countMap
	 * @return
	 */
	public  static <T,K extends Comparable<K>> Map<T, K> getBottomNValues(Map<T, K> countMap, Integer count) {
		List<Map.Entry<T, K>> list = new LinkedList<Map.Entry<T, K>>(countMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<T, K>>() {
			public int compare(Map.Entry<T, K> o1, Map.Entry<T, K> o2) {
				return ((o1).getValue()).compareTo((o2).getValue());
			}
		});

		int counter = 0;
		Map<T, K> result = new LinkedHashMap<T, K>();
		Iterator<Map.Entry<T, K>> it = list.iterator();
		
		while (it.hasNext() && counter < count) {
			Map.Entry<T, K> entry = (Map.Entry<T, K>) it.next();
			result.put(entry.getKey(), entry.getValue());
			counter++;
		}
		return result;
	}
	public static void main(String[] args) {
		Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();
		countMap.put(new Text("ABC"),new IntWritable(1));
		countMap.put(new Text("AB2C"),new IntWritable(2));
		countMap.put(new Text("ABC"),new IntWritable(3));
		countMap.put(new Text("AB1"),new IntWritable(4));
		countMap.put(new Text("ABC3"),new IntWritable(5));
		countMap.put(new Text("ABC4"),new IntWritable(7));
		countMap.put(new Text("ABC5"),new IntWritable(6));
		countMap.put(new Text("ABC6"),new IntWritable(8));
		countMap.put(new Text("ABC68"),new IntWritable(9));
		countMap.put(new Text("ABC7"),new IntWritable(10));
		System.out.println(HadoopDataHelper.getTopNValues(countMap, 5));
	}
}
