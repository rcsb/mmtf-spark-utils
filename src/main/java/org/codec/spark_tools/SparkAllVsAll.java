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
import java.util.Map.Entry;
import java.util.Set;

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
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.align.util.AlignmentTools;
import org.biojava.nbio.structure.io.mmcif.ChemCompGroupFactory;
import org.biojava.nbio.structure.io.mmcif.DownloadChemCompProvider;
import org.biojava.nbio.structure.io.sifts.SiftsChainEntry;
import org.biojava.nbio.structure.io.sifts.SiftsChainToUniprotMapping;
import org.biojava.nbio.structure.io.sifts.SiftsEntity;
import org.biojava.nbio.structure.io.sifts.SiftsMappingProvider;
import org.biojava.nbio.structure.io.sifts.SiftsSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.biojava.nbio.structure.rcsb.RCSBDescriptionFactory;
import org.biojava.nbio.structure.rcsb.RCSBMacromolecule;

import scala.Tuple2;
/**
 * Demo Map-Reduce program that shows how to read a Hadoop Sequence file and
 * generate BioJava objects
 * @author  Peter Rose
 */
public class SparkAllVsAll implements Serializable {    
	/**
	 * 
	 */
	private static final long serialVersionUID = 3037567648753603114L;
	private static int NUM_THREADS = 4;
	private static int NUM_TASKS_PER_THREAD = 3; // Spark recommends 2-3 tasks per thread
	//	private static final Logger logger = LoggerFactory.getLogger(SparkRead.class);
	public static void main(String[] args ) throws UnsupportedEncodingException, FileNotFoundException, IOException
	{
		String PDBCode = "1aq1";
		String chainId = "A";

		// Fist load this n
		SiftsChainToUniprotMapping sctum = SiftsChainToUniprotMapping.load();
		SiftsChainEntry chain = sctum.getByChainId(PDBCode,chainId);
		String uniProtId = chain.getUniProtId();
		SiftsChainEntry outAns = sctum.getByUniProtId(uniProtId);
		
		String outPDB = outAns.getPdbId();
		System.out.println(outPDB);
		//		
		//		String path = "totFile.hadoop";
		//		// This is the default 2 line structure for Spark applications
		//		SparkConf conf = new SparkConf().setMaster("local[" + NUM_THREADS + "]")
		//				.setAppName(SparkAllVsAll.class.getSimpleName());
		//		// Set the config
		//		JavaSparkContext sc = new JavaSparkContext(conf);
		//		// Time the proccess
		//		long start = System.nanoTime();
		//		JavaPairRDD<String, Structure> jprdd = sc
		//				.sequenceFile(path, Text.class, BytesWritable.class, NUM_THREADS * NUM_TASKS_PER_THREAD)
		//				// Now get the structure
		//				.mapToPair(new ByteArrayToBioJavaStructMapper());
		////				// Now get all the groups
		////				.flatMapToPair(new GetPDBGroups())
		////				// Now check if the group is a ligands
		////				.filter(new IsLigand())
		////				// Now map this to a pari
		////				.mapToPair(new Writeligand());
		//
		//		
		//		
		//		JavaPairRDD<String, Structure> smallerSample = jprdd.sample(true, 0.01);
		//		// Now collect the structurees and broadcast them
		//		Broadcast<List<Structure>> broadcastStructs = sc.broadcast(smallerSample.values().collect());
		//
		//		List<Tuple2<Integer,Integer>> totList = new ArrayList<Tuple2<Integer,Integer>>();
		//		JavaRDD<Tuple2<Integer, Integer>> jnrdd = sc.parallelize(totList);
		//		// Now do the function
		//		JavaPairRDD<String,Float> outAns = jnrdd.mapToPair(new PairFunction<Tuple2<Integer,Integer>, String, Float>() {
		//
		//			/**
		//			 * 
		//			 */
		//			private static final long serialVersionUID = -1508448765397487770L;
		//
		//			@Override
		//			public Tuple2<String, Float> call(Tuple2<Integer, Integer> t) throws Exception {
		//				// TODO Auto-generated method stub
		//				Structure s1 =broadcastStructs.getValue().get(t._1);
		//				Structure s2 =broadcastStructs.getValue().get(t._2);
		//				// Now generate the answer
		//				float thisAns = (float)1.0;
		//				Tuple2<String, Float> outAns = new Tuple2<String, Float>(s1.getPDBCode()+s2.getPDBCode(),thisAns);
		//				return outAns;
		//			}
		//			
		//			
		//		});
		//		
		//		
		////		
		////		// Now write them to a file
		////		try (Writer writer = new BufferedWriter(new OutputStreamWriter(
		////				new FileOutputStream("Ligands.sdf"), "utf-8"))) {
		////			for(String text: outValues){
		////				writer.write(text);
		////				writer.write("\n");
		////			}
		////		}
		//		sc.stop();
		//		sc.close();
		//		System.out.println("Time: " + (System.nanoTime() - start)/1E9 + " sec.");
	}
}


