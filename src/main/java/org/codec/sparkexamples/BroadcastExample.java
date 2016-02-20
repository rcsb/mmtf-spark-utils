package org.codec.sparkexamples;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.biojava.nbio.structure.Chain;
import org.codec.mappers.ByteArrayToBioJavaStructMapper;
import org.codec.mappers.ByteWriteToByteArr;
import org.codec.mappers.ChainPairToTmMapper;

import scala.Tuple2;

public class BroadcastExample {

	private static int NUM_THREADS = 24;
	private static int NUM_TASKS_PER_THREAD = 4;
	public static void main(String[] args )
	{

		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[" + NUM_THREADS + "]")
				.setAppName(SparkRead.class.getSimpleName());
		conf.set("spark.driver.maxResultSize", "14g");
		// Set the config
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// This time we read in from this input file 
		String path = "/home/anthony/src/codec-devel/data/Total.hadoop.maindata.bzip2";
		
		// Now get the chains
		List<Tuple2<String, Chain>> chains = sc
				.sequenceFile(path, Text.class, BytesWritable.class, NUM_THREADS * NUM_TASKS_PER_THREAD)
				.sample(false, 0.01)
				.mapToPair(new ByteWriteToByteArr())
				.mapToPair(new ByteArrayToBioJavaStructMapper())
				.flatMapToPair(new StructToChains())
				.collect();

		// Broadcast this list
		final Broadcast<List<Tuple2<String, Chain>>> chainsBc = sc.broadcast(chains);
		int nChains = chains.size();
		
		// Now do the analysis across all these pairs
		List<Tuple2<Integer, Integer>> totList = new ArrayList<Tuple2<Integer,Integer>>();
		for(int i =0; i<nChains;i++){
			for(int j=0; j<nChains;j++){
				if(i!=j){
				totList.add(new Tuple2<Integer, Integer>(i, j));
				}
			}
		}
		
		 JavaPairRDD<String, Double> list = sc
				.parallelizePairs(totList, NUM_THREADS*NUM_TASKS_PER_THREAD) // distribute data
				.mapToPair(new ChainPairToTmMapper(chainsBc)); // maps pairs of chain id indices to chain id, TM score pairs
	
		list.saveAsTextFile("out.results");
		sc.close();
	}
}
