package org.codec.sparkexamples;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.biojava.nbio.structure.StructureIO;
import org.biojava.nbio.structure.align.util.AtomCache;
import org.biojava.nbio.structure.io.FileParsingParameters;
import org.biojava.nbio.structure.io.LocalPDBDirectory.FetchBehavior;
import org.biojava.nbio.structure.io.mmcif.ChemCompGroupFactory;
import org.biojava.nbio.structure.io.mmcif.DownloadChemCompProvider;
import org.biojava.nbio.structure.rcsb.GetRepresentatives;
import org.codec.biojavaencoder.ParseFromBiojava;
import org.rcsb.mmtf.dataholders.PDBGroup;

public class SparkSDSCTester {

	private static int NUM_THREADS = 24;
	public static void main(String[] args ) throws IOException
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
		List<String> pdbCodeList = new ArrayList<String>(thisSet);
		// Now read this list in
		JavaRDD<String> distData =
				sc.parallelize(pdbCodeList)
				.filter(new Function<String, Boolean>() {
					
					/**
					 * 
					 */
					private static final long serialVersionUID = -656847737353155798L;

					@Override
					public Boolean call(String v1) throws Exception {
						ParseFromBiojava cbs = new ParseFromBiojava();
						Map<Integer, PDBGroup> totMap = new HashMap<Integer, PDBGroup>();
						try{
							cbs.createFromJavaStruct(v1, totMap);
						}
						catch(Exception e){
							// Just return the object
							return true;
						}
						// If it doesn't fail also return the object
						return false;
					}
				});
		// Now collect these
		List<String> errorList = distData.collect();
		System.out.println("Number of failing objects: "+errorList.size());
		FileWriter writer = new FileWriter("Errors.txt"); 
		for(String str: errorList) {
		  writer.write(str);
		}
		writer.close();
		sc.close();
	}
}

