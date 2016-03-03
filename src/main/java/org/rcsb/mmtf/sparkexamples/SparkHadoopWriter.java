package org.rcsb.mmtf.sparkexamples;


import java.util.ArrayList;
import java.util.Collections;
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
import org.rcsb.mmtf.mappers.CBSToBytes;
import org.rcsb.mmtf.mappers.PDBCodeToCBS;
import org.rcsb.mmtf.mappers.StringByteToTextByteWriter;



public class SparkHadoopWriter {

	private static int NUM_THREADS = 4;
	public static void main(String[] args )
	{

		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[" + NUM_THREADS + "]")
				.setAppName(SparkRead.class.getSimpleName());
		// Set the config
		JavaSparkContext sc = new JavaSparkContext(conf);

		// A hack to make sure we're not downloading the whole pdb
		Properties sysProps = System.getProperties();
		sysProps.setProperty("PDB_DIR", "/Users/anthony/PDB_CACHE");
		AtomCache cache = new AtomCache();
		cache.setUseMmCif(true);
		cache.setFetchBehavior(FetchBehavior.FETCH_FILES);
		FileParsingParameters params = cache.getFileParsingParams();
		params.setLoadChemCompInfo(true);
		params.setCreateAtomBonds(true);
		params.setAlignSeqRes(true);
		params.setParseBioAssembly(true);
		ChemCompGroupFactory.setChemCompProvider(new DownloadChemCompProvider());
		cache.setFileParsingParams(params);
		StructureIO.setAtomCache(cache);
		// The compressor class
		int maxStructs = 10;
		SortedSet<String> thisSet = GetRepresentatives.getAll();
		List<String> thisList = new ArrayList<String>(thisSet);
		Collections.shuffle(thisList);
		List<String> pdbCodeList = thisList.subList(0, maxStructs);	
		JavaPairRDD<Text, BytesWritable> distData =
				sc.parallelize(pdbCodeList)
				.mapToPair(new PDBCodeToCBS())
				.flatMapToPair(new CBSToBytes())
				.mapToPair(new StringByteToTextByteWriter());

		// Now save this as a Hadoop sequence file
		String uri = "Total.TEST.hadoop";		
		distData.saveAsHadoopFile(uri, Text.class, BytesWritable.class, SequenceFileOutputFormat.class);
		sc.close();
	}
}

