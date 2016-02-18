package org.codec.sparkexamples;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.biojava.nbio.structure.StructureIO;
import org.biojava.nbio.structure.align.util.AtomCache;
import org.biojava.nbio.structure.io.FileParsingParameters;
import org.biojava.nbio.structure.io.LocalPDBDirectory.FetchBehavior;
import org.biojava.nbio.structure.io.mmcif.ChemCompGroupFactory;
import org.biojava.nbio.structure.io.mmcif.DownloadChemCompProvider;
import org.codec.mappers.CBSToBytes;
import org.codec.mappers.PDBCodeToCBS;
import org.codec.mappers.StringByteToTextByteWriter;

import scala.Tuple2;

public class SparkReplace {

	private static final int NUM_THREADS = 24;
	private static final int NUM_TASKS_PER_THREAD = 3;


	public static void main(String[] args )
	{

		String path = "Total.hadoop.latest.bzip2";
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[" + NUM_THREADS + "]")
				.setAppName(SparkRead.class.getSimpleName());
		// Set the config
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<Text, BytesWritable> origData = sc
				.sequenceFile(path, Text.class, BytesWritable.class, NUM_THREADS * NUM_TASKS_PER_THREAD);

		
		SparkReplace sr = new SparkReplace();
		List<String> newList = new ArrayList<String>();
		newList.add("2KPR");
		newList.add("2PN3");
		newList.add("2KZD");
		newList.add("8BNA");
		newList.add("3JA4");
		newList.add("3NAO");
		newList.add("4XNO");
		newList.add("4YAZ");
		List<Tuple2<String, byte[]>> newData = sr.getNew(newList, sc);
		 JavaPairRDD<String, byte[]> me = sc.parallelizePairs(newData);
		origData.join(me.mapToPair(new StringByteToTextByteWriter()));
		origData.saveAsHadoopFile("NEWDATA", Text.class, BytesWritable.class, SequenceFileOutputFormat.class, org.apache.hadoop.io.compress.BZip2Codec.class);
		
		sc.stop();
		sc.close();
	}

	private List<Tuple2<String, byte[]>> getNew(List<String> newList,JavaSparkContext sc){

		// A hack to make sure we're not downloading the whole pdb
		Properties sysProps = System.getProperties();

		sysProps.setProperty("PDB_CACHE_DIR", "/home/anthony/PDB_CACHE");
		sysProps.setProperty("PDB_DIR", "/home/anthony/PDB_CACHE");
		AtomCache cache = new AtomCache();
		cache.setUseMmCif(true);
		cache.setFetchBehavior(FetchBehavior.FETCH_FILES);
		FileParsingParameters params = cache.getFileParsingParams();
		params.setCreateAtomBonds(true);
		params.setAlignSeqRes(true);
		params.setParseBioAssembly(true);
		DownloadChemCompProvider dcc = new DownloadChemCompProvider();
		ChemCompGroupFactory.setChemCompProvider(dcc);
		dcc.checkDoFirstInstall();
		params.setLoadChemCompInfo(true);
		cache.setFileParsingParams(params);
		StructureIO.setAtomCache(cache);
		// Get all the PDB IDs
		// Now read this list in
		List<Tuple2<String, byte[]>> distData = sc.parallelize(newList)
		.mapToPair(new PDBCodeToCBS())
		.flatMapToPair(new CBSToBytes()).collect();		
		return distData;
	}
	
}