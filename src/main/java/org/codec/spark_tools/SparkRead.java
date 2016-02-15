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
import org.apache.spark.api.java.function.PairFunction;
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
public class SparkRead implements Serializable {    
	/**
	 * 
	 */
	private static final long serialVersionUID = 3037567648753603114L;
	private static int NUM_THREADS = 4;
	private static int NUM_TASKS_PER_THREAD = 3; // Spark recommends 2-3 tasks per thread
	//	private static final Logger logger = LoggerFactory.getLogger(SparkRead.class);
	public static void main(String[] args ) throws UnsupportedEncodingException, FileNotFoundException, IOException
	{
		String path = "Total.hadoop.nocodec";
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[" + NUM_THREADS + "]")
				.setAppName(SparkRead.class.getSimpleName());
		// Set the config
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Set the downloader - to download the CIF files
		DownloadChemCompProvider thisProvider = new DownloadChemCompProvider();
		thisProvider.setDownloadAll(true);
		ChemCompGroupFactory.setChemCompProvider(thisProvider);
		thisProvider.checkDoFirstInstall();
		// Time the proccess
		long start = System.nanoTime();
		JavaPairRDD<String, Structure> jprdd = sc
				.sequenceFile(path, Text.class, BytesWritable.class, NUM_THREADS * NUM_TASKS_PER_THREAD)
				.mapToPair(new ByteWriteToByteArr())
				// Now get the structure
				.mapToPair(new ByteArrayToBioJavaStructMapper());
//				// Now get all the groups
//				.flatMapToPair(new GetPDBGroups())
//				// Now check if the group is a ligands
//				.filter(new IsLigand())
//				// Now map this to a pari
//				.mapToPair(new Writeligand());
		// Now collect the results
		JavaRDD<String> values = jprdd.keys();
		List<String> outValues = values.collect();
		System.out.println(outValues.size());
//		// Now write them to a file
//		try (Writer writer = new BufferedWriter(new OutputStreamWriter(
//				new FileOutputStream("Ligands.sdf"), "utf-8"))) {
//			for(String text: outValues){
//				writer.write(text);
//				writer.write("\n");
//			}
//		}
//		sc.stop();
//		sc.close();
//		System.out.println("Time: " + (System.nanoTime() - start)/1E9 + " sec.");
	}
}


