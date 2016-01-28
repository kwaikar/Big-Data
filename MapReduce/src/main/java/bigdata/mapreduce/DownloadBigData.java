package bigdata.mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

/**
 * This is an incremental assignment. For the first part, you will need to write
 * Java code to download the following books (the .txt version only) and then
 * upload them to your HDFS directory. § The Outline of Science, Vol. 1 (of 4)
 * by J. Arthur Thomson § The Notebooks of Leonardo Da Vinci § The Art of War by
 * 6th cent. B.C. Sunzi § The Adventures of Sherlock Holmes by Sir Arthur Conan
 * Doyle § The Devil's Dictionary by Ambrose Bierce § Encyclopaedia Britannica,
 * 11th Edition, Volume 4, Part 3 You should then decompress the files on the
 * server using any of examples shown in class.
 * 
 * @author kanchan
 */
public class DownloadBigData {

	public static final String CORE_SITE_CONF_PATH = "/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml";
	public static final String HDFS_SITE_CONF_PATH = "/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml";

	public static void main(String[] args) throws Exception {

		String[] ebookArchiveUrls = { "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2",
				"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2",
				"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2",
				"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2",
				"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2",
				"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2" };
		try {
			DownloadBigData.downloadAndUnzipFiles(ebookArchiveUrls);
		} catch (IOException ioe) {
			System.out.println("Exception occured while downloading and unzipping archives.");
			ioe.printStackTrace();
		}

	}

	/**
	 * This method accepts fileURLs for Archives and unzips the same.
	 * 
	 * @param fileURLs - URLS of archives
	 * @throws IOException
	 */
	public static void downloadAndUnzipFiles(String[] fileURLs) throws IOException {

		Configuration conf = new Configuration();
		conf.addResource(new Path(CORE_SITE_CONF_PATH));
		conf.addResource(new Path(HDFS_SITE_CONF_PATH));
		Scanner scanner = new Scanner(System.in);
		System.out.println("Please provide Destination File Path:");
		String destDir = scanner.next().trim();
		scanner.close();
		
		FileSystem fs = FileSystem.get(URI.create(destDir.trim()), conf);
		if (!fs.exists(new Path(destDir))) {
			System.out.println("Unable to access path specified. Please provide alternate destination path");
		} else {
			for (String fileURL : fileURLs) {
				String destFileName = destDir + fileURL.substring(fileURL.lastIndexOf('/'), fileURL.length());
				downloadFileToHDFS(fileURL, destFileName, conf);
				extractCompressedFile(conf, destFileName.trim());
			}
		}
	}

	/**
	 * This method extracts the compressed file
	 * 
	 * @param conf
	 *            - Hadoop cluster configuration
	 * @param compressedFilePath
	 * @throws IOException
	 */
	public static void extractCompressedFile(Configuration conf, String compressedFilePath) throws IOException {
		System.out.println("Initiating decompression");
		FileSystem fs = FileSystem.get(URI.create(compressedFilePath), conf);
		Path inputPath = new Path(compressedFilePath);
		CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(inputPath);
		if (codec == null) {
			System.err.println("No codec found for '" + inputPath + "'");
			System.exit(1);
		}
		String outputUri = CompressionCodecFactory.removeSuffix(compressedFilePath, codec.getDefaultExtension());
		if (!fs.exists(new Path(outputUri))) {
			InputStream in = codec.createInputStream(fs.open(inputPath));
			OutputStream out = fs.create(new Path(outputUri));
			IOUtils.copyBytes(in, out, conf);
			System.out.println("Extraction complete: " + outputUri);
		} else {
			System.out.println("Aborted : File already exists" );
		}
	}

	/**
	 * @param inputFileURL
	 *            - Input file URL
	 * @param destFileName
	 *            - destination fileName
	 * @param conf
	 *            - Loaded Hadoop Configuration object
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public static void downloadFileToHDFS(String inputFileURL, String destFileName, Configuration conf)
			throws IOException, MalformedURLException {
		System.out.println("Initiating download for " + inputFileURL);

		InputStream input = new URL(inputFileURL).openStream();
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
		}
		else
		{
			System.out.println("Aborting download: File already exists at destination path");
		}
	}
}
