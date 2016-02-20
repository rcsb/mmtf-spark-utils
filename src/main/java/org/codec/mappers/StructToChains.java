package org.codec.mappers;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Structure;

import scala.Tuple2;
public class StructToChains implements PairFlatMapFunction<Tuple2<String, Structure>, String, Chain> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Iterable<Tuple2<String, Chain>> call(Tuple2<String, Structure> t) throws Exception {
		// Get the strucutre
		Structure inputStruct = t._2;
		// Get all the chains for the first model
		List<Chain> theseChains = inputStruct.getChains();
		// Generate the output
		List<Tuple2<String, Chain>> outPut = new ArrayList<Tuple2<String, Chain>>();
		// Loop through the chains
		for(Chain c: theseChains){
			 outPut.add(new Tuple2<String,Chain>(t._1+"_"+c.getChainID(),c));
		}
		// Now return it
		return outPut;
	}

}
