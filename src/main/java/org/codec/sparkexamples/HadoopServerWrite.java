package org.codec.sparkexamples;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
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
import org.apache.hadoop.io.Text;
import org.biojava.nbio.structure.StructureException;
import org.biojava.nbio.structure.StructureIO;
import org.biojava.nbio.structure.align.util.AtomCache;
import org.biojava.nbio.structure.io.FileParsingParameters;
import org.biojava.nbio.structure.io.mmcif.ChemCompGroupFactory;
import org.biojava.nbio.structure.io.mmcif.DownloadChemCompProvider;
import org.biojava.nbio.structure.rcsb.GetRepresentatives;
import org.codec.dataholders.PDBGroup;
import org.codec.dataholders.BioDataStruct;
import org.codec.dataholders.CreateBasicStructure;
import org.codec.dataholders.HeaderBean;
import org.codec.biojavaencoder.EncoderUtils;


public class HadoopServerWrite { 


	@SuppressWarnings("deprecation")
	public static void main( String[] args) throws IOException, StructureException, IllegalAccessException, InvocationTargetException { 
		PrintWriter failWriter = new PrintWriter("errorPDBs", "UTF-8");
		// A hack to make sure we're not downloading the whole pdb
		Properties sysProps = System.getProperties();
		sysProps.setProperty("PDB_DIR", "/Users/anthony/PDB_CACHE");
		AtomCache cache = new AtomCache();
		cache.setUseMmCif(true);
		FileParsingParameters params = cache.getFileParsingParams();
		params.setLoadChemCompInfo(true);
		params.setCreateAtomBonds(true);
		params.setParseBioAssembly(true);
		params.setAlignSeqRes(true);
		ChemCompGroupFactory.setChemCompProvider(new DownloadChemCompProvider());
		cache.setFileParsingParams(params);
		StructureIO.setAtomCache(cache);
		// The compressor class
		EncoderUtils cm = new EncoderUtils();
		//Now fill with a 1000 PDB codes
		int counter = 0;
		int maxStructs =0;
		SortedSet<String> thisSet = GetRepresentatives.getAll();
		List<String> thisList = new ArrayList<String>(thisSet);
		Collections.shuffle(thisList);
		List<String> pdbCodeList = thisList.subList(0, maxStructs);	
		// Remove this  from the processing
//		pdbCodeList.add("3J3Y");
//		pdbCodeList.add("4CUP");
//		pdbCodeList.add("3SN6");
//		pdbCodeList.add("1A4C");
//		pdbCodeList.add("4V5A");
//		pdbCodeList.add("4V99");
//		pdbCodeList.add("2L6N");
//		pdbCodeList.add("43PQ");
//		pdbCodeList.add("3DBQ");
		pdbCodeList.add("4V5X");
		pdbCodeList.add("4V42");
		pdbCodeList.add("4V47");
		pdbCodeList.add("4V48");
		pdbCodeList.add("4V7F");
		pdbCodeList.add("3PE6");

		// Set the file to write
		String uri = "totFile.hadoop";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create( uri), conf);
		Path path = new Path(uri);
		// Now set the nature of the key value pair
		Text key = new Text();
		// The value is a byte array
		BytesWritable value = new BytesWritable();
		SequenceFile.Writer writer = null;  
		try { 
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			for (String pdbCode: pdbCodeList) { 

				try{
					// Now set the nature of the key value pair
					Text outKey = new Text();
					// Now get the bytearry
					counter++;
					System.out.println("STARTING: "+pdbCode);
					System.out.println("NUMBER: "+counter);
					CreateBasicStructure cbs = new CreateBasicStructure();
					Map<Integer, PDBGroup> totMap = new HashMap<Integer, PDBGroup>();
					cbs.createFromJavaStruct(pdbCode, totMap);
					// Now get the header too
					HeaderBean headerData = cbs.getHeaderStruct();
					BioDataStruct thisBS = cbs.getBioStruct();
					System.out.println("SIZE "+thisBS.getInterGroupBondInds().size());
					// NOW JUST WRITE THE KEY VALUE PAIRS HERE
					outKey.set(pdbCode);
					// Now write the byte array - and extract ResMap to consolidate it 
					BytesWritable inputValue = new BytesWritable(cm.compressMainData(thisBS, headerData));
					// Get the length of this guy
					System.out.println("KEY: "+outKey);
					System.out.println("LENGTH: "+inputValue.getLength());
					writer.append(outKey, inputValue);
				}
				catch(Exception e){
					System.out.println(e);
				}
			} 
		} finally 
		{ IOUtils.closeStream( writer); 
		} 
		failWriter.close();
	}

}
