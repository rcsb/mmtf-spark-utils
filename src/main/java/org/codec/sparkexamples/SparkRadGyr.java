package org.codec.sparkexamples;


import java.io.Serializable;
import java.util.List;



import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codec.mappers.ByteArrayToBioJavaStructMapper;
import org.codec.mappers.ByteWriteToByteArr;
import org.codec.mappers.GetBioAssemblies;
import org.codec.proccessors.RadiusOfGyrationMapper;

/**
 * Demo Map-Reduce program that shows how to read a Hadoop Sequence file and
 * generate BioJava objects
 * @author  Peter Rose
 */
public class SparkRadGyr implements Serializable {    
	/**
	 * 
	 */
	private static final long serialVersionUID = 3037567648753603114L;
	private static int NUM_THREADS = 4;
	private static int NUM_TASKS_PER_THREAD = 3; // Spark recommends 2-3 tasks per thread
	//	private static final Logger logger = LoggerFactory.getLogger(SparkRead.class);
	public static void main(String[] args ) 
	{
		String path = "totFileTestSmall.hadoop";
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[" + NUM_THREADS + "]")
				.setAppName(SparkRadGyr.class.getSimpleName());
		// Set the config
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Time the proccess
		long start = System.nanoTime();
		JavaPairRDD<String, Float> jprdd = sc
				.sequenceFile(path, Text.class, BytesWritable.class, NUM_THREADS * NUM_TASKS_PER_THREAD)
				// Now get the structure
				.mapToPair(new ByteWriteToByteArr())
				.mapToPair(new ByteArrayToBioJavaStructMapper())
				.flatMapToPair(new GetBioAssemblies())
				.mapToPair(new RadiusOfGyrationMapper());
		// Now collect the results
		JavaRDD<Float> values = jprdd.values();
		List<Float> outValues = values.collect();
		// Now print them out
		System.out.println(outValues);
		sc.stop();
		sc.close();
		System.out.println("Time: " + (System.nanoTime() - start)/1E9 + " sec.");
	}
}

