package org.codec.sparkexamples;



import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codec.mappers.ByteWriteToByteArr;
import org.rcsb.mmtf.biojavaencoder.EncoderUtils;

import scala.Tuple2;

/**
 * Demo Map-Reduce program that shows how to read a Hadoop Sequence file and
 * generate BioJava objects
 * @author  Peter Rose
 */
public class SparkReadChains implements Serializable {    
	/**
	 * 
	 */
	private static final long serialVersionUID = -5794789833402280300L;

	/**
	 * 
	 */

	public static void main(String[] args )
	{
		EncoderUtils eu = new EncoderUtils();
		String path = "/home/anthony/src/codec-devel/data/Total.calpha.peter.bzip2";
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[*]")
				.setAppName(SparkReadChains.class.getSimpleName());
		// Set the config
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Time the proccess
		long start = System.nanoTime();
		JavaPairRDD<String, byte[]> jprdd = sc
				// Read the file
				.sequenceFile(path, Text.class, BytesWritable.class, 24)
				// Now get the structure
				.mapToPair(new ByteWriteToByteArr())
				.mapToPair(t -> new Tuple2<String, byte[]>(t._1,eu.getMessagePack(t._2)));
		JavaRDD<String> vals = jprdd.keys();
		List<String> theseVals = vals.collect();
		System.out.println(theseVals.size());
		sc.stop();
		sc.close();
		System.out.println("Time: " + (System.nanoTime() - start)/1E9 + " sec.");
	}
}


