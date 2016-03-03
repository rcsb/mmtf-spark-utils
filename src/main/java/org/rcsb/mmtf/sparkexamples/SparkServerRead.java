package org.rcsb.mmtf.sparkexamples;



import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.biojava.nbio.structure.Structure;
import org.rcsb.mmtf.mappers.ByteArrayToBioJavaStructMapper;
import org.rcsb.mmtf.mappers.ByteWriteToByteArr;

/**
 * Demo Map-Reduce program that shows how to read a Hadoop Sequence file and
 * generate BioJava objects
 * @author  Peter Rose
 */
public class SparkServerRead implements Serializable {    
	/**
	 * 
	 */
	private static final long serialVersionUID = 3037567648753603114L;
	private static int NUM_THREADS = 24;
	private static int NUM_TASKS_PER_THREAD = 3; // Spark recommends 2-3 tasks per thread
	//	private static final Logger logger = LoggerFactory.getLogger(SparkRead.class);
	public static void main(String[] args )
	{
		Properties sysProps = System.getProperties();

		sysProps.setProperty("PDB_CACHE_DIR", "/home/anthony/PDB_CACHE");
		sysProps.setProperty("PDB_DIR", "/home/anthony/PDB_CACHE");
		String path = "/home/anthony/src/codec-devel/Total.hadoop.bzip2";
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[" + NUM_THREADS + "]")
				.setAppName(SparkServerRead.class.getSimpleName());
		// Set the config
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Time the proccess
		long start = System.nanoTime();
		JavaPairRDD<String, Structure> jprdd = sc
				// Read the file
				.sequenceFile(path, Text.class, BytesWritable.class, NUM_THREADS * NUM_TASKS_PER_THREAD)
				// Now get the structure
				.mapToPair(new ByteWriteToByteArr())
				.mapToPair(new ByteArrayToBioJavaStructMapper());

		JavaRDD<String> vals = jprdd.keys();
		List<String> theseVals = vals.collect();
		System.out.println(theseVals.size());
		sc.stop();
		sc.close();
		System.out.println("Time: " + (System.nanoTime() - start)/1E9 + " sec.");
	}
}


