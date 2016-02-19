package org.codec.mappers;

import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureIO;

import scala.Tuple2;

public class PdbIdToBioJavaStruct  implements PairFunction<String, String, Structure>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 786599975302506694L;	

	@Override
	public Tuple2<String, Structure> call(String t) throws Exception {
		// Get the structure
		Structure struct = StructureIO.getStructure(t);
		// Now return the tuple
		return new Tuple2<String, Structure>(t,struct);
	}

}


