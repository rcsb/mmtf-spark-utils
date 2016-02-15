package org.codec.spark_tools;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import javax.crypto.Mac;
import javax.vecmath.Point3d;

import org.apache.commons.io.IOUtils;
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
import org.biojava.nbio.structure.io.mmcif.ChemCompProvider;
import org.biojava.nbio.structure.io.mmcif.DownloadChemCompProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
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
	public static void main(String[] args ) throws UnsupportedEncodingException, FileNotFoundException, IOException
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
//				.flatMapToPair(new GetBioAssemblies())
//				.mapToPair(new PredictQuatStructures());
//				// Now get all the groups
//				.flatMapToPair(new GetPDBGroups())
//				// Now check if the group is a ligands
//				.filter(new IsLigand())
//				// Now map this to a pair
//				.mapToPair(new Writeligand());
//		// Now collect the results
////		JavaRDD<String> values = jprdd.values();
//		List<String> outValues = values.collect();
		JavaRDD<String> vals = jprdd.keys();
		List<String> outKeys = vals.collect();		

		sc.stop();
		sc.close();
		System.out.println("Time: " + (System.nanoTime() - start)/1E9 + " sec.");
	}
}


