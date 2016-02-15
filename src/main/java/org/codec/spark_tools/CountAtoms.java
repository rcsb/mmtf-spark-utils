package org.codec.spark_tools;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;

import scala.Array;
import scala.Tuple2;

/**
 * 
 * @author anthony
 * Class to count the number of Atoms in the PDB
 */


public class CountAtoms implements  PairFunction<Tuple2<String, Structure>, String, Double> {
	
	private Double getAtoms(Structure input){
		// Get the number of atoms in the strucutre
		Double outAns = 0.0;
		try{
		for (Chain c: input.getChains()){
			for(Group g: c.getAtomGroups()){
				for(Atom a: g.getAtoms()){
					outAns+=1.0;
				}
			}

		}
		return outAns;
		}
		catch(IndexOutOfBoundsException e){
			return 0.0;
		}
		
	}
	
	@Override
	public Tuple2<String, Double> call(Tuple2<String, Structure> t) throws Exception {
		// TODO Auto-generated method stub
		Tuple2<String, Double> outAns = new Tuple2<String, Double>(t._1, getAtoms(t._2));
		return outAns;
	}


}
