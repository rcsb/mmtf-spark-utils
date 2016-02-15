package org.codec.spark_tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.StructureIO;
import org.biojava.nbio.structure.align.util.AtomCache;
import org.biojava.nbio.structure.io.FileParsingParameters;
import org.biojava.nbio.structure.io.LocalPDBDirectory.FetchBehavior;
import org.biojava.nbio.structure.io.mmcif.ChemCompGroupFactory;
import org.biojava.nbio.structure.io.mmcif.DownloadChemCompProvider;
import org.biojava.nbio.structure.rcsb.GetRepresentatives;



public class SparkHadoopWriter {

	private static int NUM_THREADS = 4;
	private static int NUM_TASKS_PER_THREAD = 3; // Spark recommends 2-3 tasks per thread
	public static void main(String[] args ) throws UnsupportedEncodingException, FileNotFoundException, IOException
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
				.mapToPair(new PDBCodeToCBSMapper())
				.flatMapToPair(new FlatMapToBytes())
				.mapToPair(new StringByteToTextByteWriter());
		//				.mapToPair(new PairFunction<String, Text, BytesWritable>() {
		//					/**
		//					 * Serial id for the function
		//					 */
		//					private static final long serialVersionUID = 1L;
		//
		//					@Override
		//					public Tuple2<Text, BytesWritable> call(String t) throws Exception {
		//						CompressmmCIF cm = new CompressmmCIF();
		//						// TODO Auto-generated method stub
		//						// Now set the nature of the key value pair
		//						CreateBasicStructure cbs = new CreateBasicStructure();
		//						Map<Integer, PDBGroup> totMap = new HashMap<Integer, PDBGroup>();
		//						try{
		//							cbs.createFromJavaStruct(t, totMap);
		//						}
		//						catch(Exception e){
		//							// Return an empty array
		//							Text outT = new Text();
		//							outT.set(t);
		//							BytesWritable outBytes = new BytesWritable();
		//							byte[] theseBytes = new byte[0];
		//							outBytes.set(theseBytes, 0, theseBytes.length);
		//							return new Tuple2<Text, BytesWritable>(outT,outBytes);
		//						}
		//						// Now get the header too
		//						HeaderBean headerData = cbs.getHeaderStruct();
		//						BioDataStruct thisBS = cbs.getBioStruct();
		//						CalphaBean calphaStruct = cbs.getCalphaStruct();
		//						// NOW JUST WRITE THE KEY VALUE PAIRS HERE
		//						// Now write the byte array - and extract ResMap to consolidate it 
		//						Text outT = new Text();
		//						outT.set(t);
		//						BytesWritable outBytes = new BytesWritable();
		//						byte[] theseBytes = cm.compHadoopDS(thisBS, headerData);
		//						outBytes.set(theseBytes, 0, theseBytes.length);
		//						return new Tuple2<Text, BytesWritable>(outT,outBytes);
		//					}
		//				});



		// Now save this as a Hadoop sequence file
		String uri = "Total.TEST.hadoop";		
		distData.saveAsHadoopFile(uri, Text.class, BytesWritable.class, SequenceFileOutputFormat.class);
		//		distData.saveAsHadoopDataset(conf);
		//		distData.saveAsHadoopFile(, Text.class, BytesWritable.class,  SequenceFileAsBinaryOutputFormat.class);

	}
}

