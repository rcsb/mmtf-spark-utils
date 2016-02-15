package org.codec.spark_tools;
//package org.compression.hadooputils;
//
//import java.io.FileNotFoundException;
//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.SortedSet;
//
//import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.biojava.nbio.structure.Group;
//import org.biojava.nbio.structure.rcsb.GetRepresentatives;
//
//public class ParallelWriteFile implements Serializable{
//	
//	private static int NUM_THREADS = 4;
//	private static int NUM_TASKS_PER_THREAD = 3; // Spark recommends 2-3 tasks per thread
//	public static void main(String[] args ) throws FileNotFoundException
//	{
//		//Now fill with a 1000 PDB codes
//		int counter = 0;
//		int maxStructs = 1000;
//		SortedSet<String> thisSet = GetRepresentatives.getAll();
//		List<String> thisList = new ArrayList<String>(thisSet);
//		Collections.shuffle(thisList);
//		List<String> pdbCodeList = thisList.subList(0, maxStructs);	
//		// This is the default 2 line structure for Spark applications
//		SparkConf conf = new SparkConf().setMaster("local[" + NUM_THREADS + "]")
//				.setAppName(SparkRead.class.getSimpleName());
//		// Set the config
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		
//		JavaPairRDD<String, Group> jprdd = sc
//				.sequenceFile(path, Text.class, BytesWritable.class, NUM_THREADS * NUM_TASKS_PER_THREAD)
//				// Now get the structure
//				.mapToPair(new ByteArrayToBioJavaStructMapper());
//		JavaRDD<String> myKeys = sc.parallelize(pdbCodeList);
//		
//		
//	}
//}
