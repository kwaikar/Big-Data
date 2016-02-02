package twitter.topicextractor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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
	 * @param hdfsPath - Input HDFS path.
	 * @return flag - boolean value 
	 * @throws IOException
	 */ 
	public static boolean doesFileOrFolderExist( String hdfsPath) throws IOException
	{
		 return FileSystem.get(URI.create(hdfsPath.trim()), HadoopDataHelper.getHdfsConfiguration()).exists(new Path(hdfsPath));
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
}
