package org.codec.spark_tools;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import javax.crypto.Mac;
import javax.vecmath.Point3d;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class SparkTest implements Serializable {    

	private static final long serialVersionUID = 6264935125052241326L;
	private static int NUM_THREADS = 4;
	private static int NUM_TASKS_PER_THREAD = 3; // Spark recommends 2-3 tasks per thread
//	private static final Logger logger = LoggerFactory.getLogger(SparkRead.class);
	public static void main(String[] args ) throws IOException
	{
		String path = "totFile.hadoop";
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[" + NUM_THREADS + "]")
				.setAppName(SparkTest.class.getSimpleName());
		
		
		// Set the config
		JavaSparkContext sc = new JavaSparkContext(conf);


		// Time the proccess
		long start = System.nanoTime();
		JavaPairRDD<String, Double> jprdd = sc
				.sequenceFile(path, Text.class, BytesWritable.class, NUM_THREADS * NUM_TASKS_PER_THREAD)
				// Now get the structure
				.mapToPair(new ByteWriteToByteArr())
				.mapToPair(new ByteArrayToBioJavaStructMapper())
				.mapToPair(new CountAtoms());
//				// Now get the bio assemblies
//				.flatMapToPair(new GetBioAssemblies())
//				.flatMapToPair(new GetPDBGroups())
//				// Now calculate the quaternary structure
//				.filter(new IsLigand());
////				.mapToPair(new PredictQuatStructures());

		// Now collect some results - so we can check
		JavaRDD<String> myKeys = jprdd.keys();
		JavaRDD<Double> values = jprdd.values();
		List<String> outKeys = myKeys.collect();
		List<Double> outValues = values.collect();
		System.out.println(outKeys);
		System.out.println(outValues);
		// Now close everything down
//		logger.warn("JOSE BETTER BE RIGHT ABOUT THIS.....");
		sc.stop();
		sc.close();
		System.out.println("Time: " + (System.nanoTime() - start)/1E9 + " sec.");
	}
}



