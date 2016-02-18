package org.codec.sparkexamples;

import java.util.ArrayList;

import java.util.List;
import java.util.Properties;
import java.util.SortedSet;

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
import org.biojava.nbio.structure.rcsb.GetRepresentatives;
import org.codec.mappers.CBSToBytes;
import org.codec.mappers.PDBCodeToCBS;
import org.codec.mappers.StringByteToTextByteWriter;

public class SparkSDSCHadoopWriter {

	private static int NUM_THREADS = 24;
	public static void main(String[] args )
	{

		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[" + NUM_THREADS + "]")
				.setAppName(SparkRead.class.getSimpleName());
		// Set the config
		JavaSparkContext sc = new JavaSparkContext(conf);

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
		SortedSet<String> thisSet = GetRepresentatives.getAll();
		List<String> pdbCodeList = new ArrayList<String>();
		pdbCodeList.add("4AOB");
		pdbCodeList.add("312D");
		pdbCodeList.add("328D");
		pdbCodeList.add("340D");
		// Now read this list in
		JavaPairRDD<Text, BytesWritable> distData =
				sc.parallelize(pdbCodeList)
				.mapToPair(new PDBCodeToCBS())
				.flatMapToPair(new CBSToBytes())
				.mapToPair(new StringByteToTextByteWriter());
		// Now save this as a Hadoop sequence file
		String uri = "Total.hadoop.latest.bzip2";		
		distData.saveAsHadoopFile(uri, Text.class, BytesWritable.class, SequenceFileOutputFormat.class, org.apache.hadoop.io.compress.BZip2Codec.class);
		sc.close();
	}
}

