package org.codec.mappers;


import java.util.HashMap;
import java.util.Map;
import org.apache.spark.api.java.function.PairFunction;
import org.codec.dataholders.CreateBasicStructure;
import org.codec.dataholders.PDBGroup;

import scala.Tuple2;

public class PDBCodeToCBS implements PairFunction<String, String, CreateBasicStructure>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 786599975302506694L;	

	@Override
	public Tuple2<String, CreateBasicStructure> call(String t) throws Exception {
		CreateBasicStructure cbs = new CreateBasicStructure();
		Map<Integer, PDBGroup> totMap = new HashMap<Integer, PDBGroup>();
		try{
			cbs.createFromJavaStruct(t, totMap);
		}
		catch(Exception e){
			// Just return the object
			return new Tuple2<String, CreateBasicStructure>(t,cbs);
		}
		// If it doesn't fail also return the object
		return new Tuple2<String, CreateBasicStructure>(t,cbs);
	}


}

