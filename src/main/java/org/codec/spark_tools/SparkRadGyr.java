package org.codec.spark_tools;


import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
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
import org.biojava.nbio.structure.io.mmcif.ChemCompGroupFactory;
import org.biojava.nbio.structure.io.mmcif.DownloadChemCompProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
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
	public static void main(String[] args ) throws UnsupportedEncodingException, FileNotFoundException, IOException
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
//		try (Writer writer = new BufferedWriter(new OutputStreamWriter(
//				new FileOutputStream("Ligands.sdf"), "utf-8"))) {
//			for(String text: outValues){
//				writer.write(text);
//				writer.write("\n");
//			}
//		}
		sc.stop();
		sc.close();
		System.out.println("Time: " + (System.nanoTime() - start)/1E9 + " sec.");
	}
}

