package org.codec.spark_tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;



/*
 * Class to write files that can 
 */
public class SparkSDSCHashMapWriter {

	public static void main(String[] args) throws Exception {
		SparkSDSCHashMapWriter sdhw = new SparkSDSCHashMapWriter();
		// The path of the hadoop file
		int numThreads = 24;
		String uri = "/home/anthony/src/codec-devel/Total.hadoop.TRIAL.bzip2";
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[" + numThreads + "]")
				.setAppName(SparkRead.class.getSimpleName());
		conf.set("spark.driver.maxResultSize", "14g");
		// Set the config
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Read in with spark
		JavaPairRDD<String, byte[]> totalDataset = sc
				.sequenceFile(uri, Text.class, BytesWritable.class, 4 * 3)
				.mapToPair(new ByteWriteToByteArr());

		// 
		JavaPairRDD<String, byte[]> mainMap = totalDataset.filter(new Function<Tuple2<String,byte[]>, Boolean>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -7172364344277495432L;

			@Override
			public Boolean call(Tuple2<String, byte[]> v1) throws Exception {
				if(v1._1.endsWith("_total")==true){
					return true;
				}
				return false;
			}
		});
		JavaPairRDD<String, byte[]> headerMap = totalDataset.filter(new Function<Tuple2<String,byte[]>, Boolean>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 7574230201179927345L;

			@Override
			public Boolean call(Tuple2<String, byte[]> v1) throws Exception {
				if(v1._1.endsWith("_header")==true){
					return true;
				}
				return false;
			}
		});
		JavaPairRDD<String, byte[]> calphaMap = totalDataset.filter(new Function<Tuple2<String,byte[]>, Boolean>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -8312184119385524L;

			@Override
			public Boolean call(Tuple2<String, byte[]> v1) throws Exception {
				if(v1._1.endsWith("_calpha")==true){
					return true;
				}
				return false;
			}
		});
		// Now collect these as maps
		sdhw.writeHashMapToFile(headerMap.collectAsMap(), "headerMap.map");
		sdhw.writeHashMapToFile(calphaMap.collectAsMap(), "calphaMap.map");
		sdhw.writeHashMapToFile(mainMap.collectAsMap(), "mainMap.map");
		// Close the spark context
		sc.close();
	}

	/**
	 * 
	 * @param mapToWrite
	 * @param fileName
	 * @throws IOException
	 */
	public void writeHashMapToFile(Map<String, byte[]> mapToWrite, String fileName) throws IOException{

		File file = new File(fileName);
		FileOutputStream f = new FileOutputStream(file);
		ObjectOutputStream s = new ObjectOutputStream(f);
		s.writeObject(mapToWrite);
		s.close();

	}
	/**
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private Map<String, byte[]> readMapFromFile(String fileName) throws IOException, ClassNotFoundException {

		File file = new File(fileName);
		FileInputStream f = new FileInputStream(file);
		ObjectInputStream s = new ObjectInputStream(f);
		Map<String, byte[]> inputMap = (Map<String, byte[]>) s.readObject();
		s.close();
		return inputMap;
	}

}
