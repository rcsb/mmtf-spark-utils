package org.rcsb.mmtf.collectors;

import java.util.ArrayList;
import java.util.List;


import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.GroupType;
import org.biojava.nbio.structure.Structure;

import scala.Tuple2;

/**
 * 
 * @author Anthony Bradley
 *Class to get all Fragments of a structure
 */
public class GetAllFragments implements PairFlatMapFunction<Tuple2<String,Structure>, String, Group>{

	private static final long serialVersionUID = 36604498761700117L;
	int fragSize = 9;

	@Override
	public Iterable<Tuple2<String, Group>> call(Tuple2<String, Structure> t) throws Exception {
		// The list to return
		List<Tuple2<String, Group>> outList = new ArrayList<Tuple2<String, Group>>();
		for(int modelNr=0;  modelNr<t._2.nrModels(); modelNr++){
			// Function to get all the fragments - and return in a tuple
			for(Chain c: t._2.getChains(modelNr)){
				List<Group> myGroups = c.getAtomGroups(GroupType.AMINOACID);
				// Now loop through these groups and add them to the outputlist
				for(Group g: myGroups){
					// Now generate the unique id
					String uniqId = modelNr+"_"+c.getChainID()+"_"+g.getResidueNumber();
					outList.add(new Tuple2<String, Group>(uniqId, g));
				}
			}
		}
		return outList;
	}



}