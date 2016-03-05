package org.rcsb.mmtf.sparkexamples;



import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.vecmath.Point3d;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.rcsb.mmtf.dataholders.CalphaAlignBean;
import org.rcsb.mmtf.mappers.AlignmentMapper;
import org.rcsb.mmtf.mappers.ByteWriteToByteArr;

import com.fasterxml.jackson.databind.ObjectMapper;

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
		String path = "/home/anthony/src/codec-devel/data/Total.calpha.peter.bzip2";
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[*]")
				.setAppName(SparkReadChains.class.getSimpleName());
		
		conf.set("spark.driver.maxResultSize", "14g");
		// Set the config
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Time the proccess
		long start = System.nanoTime();
		List<Tuple2<String, Point3d[]>> jprdd = sc
				// Read the file
				.sequenceFile(path, Text.class, BytesWritable.class, 24)
				// Now get the structure
				.mapToPair(new ByteWriteToByteArr())
				.map(t -> new ObjectMapper(new MessagePackFactory()).readValue(t._2, CalphaAlignBean.class))
				.mapToPair(t -> new Tuple2<String, Point3d[]>(t.getPdbId()+"_"+t.getChainId(), t.getCoordList()))
				.collect();
				
		// Get the total number of chains
		int nChains = jprdd.size();
		
		final Broadcast<List<Tuple2<String,Point3d[]>>> chainsBc = sc.broadcast(jprdd);
		// Now do the analysis across all these pairs
		List<Tuple2<Integer, Integer>> totList = new ArrayList<Tuple2<Integer,Integer>>();
		for(int i =0; i<nChains;i++){
			for(int j=0; j<nChains;j++){
				if(i!=j){
				totList.add(new Tuple2<Integer, Integer>(i, j));
				}
			}
		}
		
		 JavaPairRDD<String, Float> list = sc
				.parallelizePairs(totList, 24) // distribute data
				.mapToPair(new AlignmentMapper(chainsBc)); // maps pairs of chain id indices to chain id, TM score pairs
	
		list.saveAsTextFile("out.results");
		sc.close();

		System.out.println("Time: " + (System.nanoTime() - start)/1E9 + " sec.");
	}
}


