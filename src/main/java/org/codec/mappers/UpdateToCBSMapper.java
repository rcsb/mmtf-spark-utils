package org.codec.mappers;


import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureIO;
import org.codec.dataholders.CreateBasicStructure;
import org.codec.dataholders.PDBGroup;

import scala.Tuple2;

public class UpdateToCBSMapper implements PairFunction<String, String, CreateBasicStructure>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 786599975302506694L;	

	@Override
	public Tuple2<String, CreateBasicStructure> call(String pdbId) throws Exception {
		CreateBasicStructure cbs = new CreateBasicStructure();
		String thisId = pdbId.toLowerCase();
		String midStr = thisId.substring(1, 3);
		String url = "http://sandboxwest.rcsb.org:10601/sandbox-v4.0/"+midStr+"/"+thisId+"/"+thisId+".cif.gz";
		System.out.println("Collecting from this URL: "+url);
		Structure bioJavaStruct = StructureIO.getStructure(url);
		Map<Integer, PDBGroup> totMap = new HashMap<Integer, PDBGroup>();
		try{
			cbs.genFromJs(bioJavaStruct, totMap);
		}
		catch(Exception e){
			// Return an empty array
			Text outT = new Text();
			outT.set(pdbId);
			BytesWritable outBytes = new BytesWritable();
			byte[] theseBytes = new byte[0];
			outBytes.set(theseBytes, 0, theseBytes.length);
			return new Tuple2<String, CreateBasicStructure>(pdbId,cbs);
		}
		return new Tuple2<String, CreateBasicStructure>(pdbId,cbs);
	}


}