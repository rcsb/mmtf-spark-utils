package org.codec.collectors;


import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;

import scala.Tuple2;

/**
 * 
 * @author anthony
 * Class to count the number of Atoms in the PDB
 */
public class CountAtoms implements  PairFunction<Tuple2<String, Structure>, String, Integer> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2788962017828944159L;

	private Integer getAtoms(Structure input){
		// Get the number of atoms in the strucutre
		int outAns = 0;
		try{
		for (Chain c: input.getChains()){
			for(Group g: c.getAtomGroups()){
				outAns+=g.getAtoms().size();
			}

		}
		return outAns;
		}
		catch(IndexOutOfBoundsException e){
			return 0;
		}
		
	}
	
	@Override
	public Tuple2<String, Integer> call(Tuple2<String, Structure> t) throws Exception {
		Tuple2<String, Integer> outAns = new Tuple2<String, Integer>(t._1, getAtoms(t._2));
		return outAns;
	}


}
