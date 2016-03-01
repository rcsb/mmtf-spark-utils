package org.codec.sparkexamples;

import java.io.Serializable;
import java.util.Properties;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codec.mappers.ByteArrToObject;
import org.codec.mappers.ByteWriteToByteArr;
import org.codec.mappers.ChainStripper;
import org.codec.mappers.ObjectToByteArr;
import org.codec.mappers.StringByteToTextByteWriter;

public class SparkSDSCCalphaChains implements Serializable {    
	/**
	 * 
	 */
	private static final long serialVersionUID = 7724732869869546699L;

	/**
	 * 
	 */
	public static void main(String[] args )
	{
		String path = "/home/anthony/src/codec-devel/data/Total.hadoop.latest.bzip2";
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[*]")
				.setAppName(SparkServerRead.class.getSimpleName());
		// Set the config
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Time the proccess
		long start = System.nanoTime();
		JavaPairRDD<Text, BytesWritable> jprdd = sc
				// Read the file
				.sequenceFile(path, Text.class, BytesWritable.class, 24)
				// Now get the structure
				.mapToPair(new ByteWriteToByteArr())
				.filter(t -> t._1.endsWith("_calpha"))
				.mapToPair(new ByteArrToObject())
				.flatMapToPair(new ChainStripper())
				.mapToPair(new ObjectToByteArr())
				.mapToPair(new StringByteToTextByteWriter());
		
		jprdd.saveAsHadoopFile("/home/anthony/src/codec-devel/data/Total.calpha.peter.bzip2", Text.class, BytesWritable.class, SequenceFileOutputFormat.class, org.apache.hadoop.io.compress.BZip2Codec.class);
		// Now write this stuff out
		sc.stop();
		sc.close();
		System.out.println("Time: " + (System.nanoTime() - start)/1E9 + " sec.");
	}
}