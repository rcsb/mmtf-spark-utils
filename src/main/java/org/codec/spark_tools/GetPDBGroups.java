package org.codec.spark_tools;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;

import scala.Tuple2;

public class GetPDBGroups implements PairFlatMapFunction<Tuple2<String,Structure>, String, Group>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 9211500299985679809L;

	@Override
	public Iterable<Tuple2<String, Group>> call(Tuple2<String, Structure> t) throws Exception {
		// TODO Auto-generated method stub
		List<Tuple2<String, Group>> outList = new ArrayList<Tuple2<String, Group>>();

		for(Chain c:  t._2.getChains()){
			for(Group g: c.getAtomGroups()){
				// Now make this group
				outList.add(new Tuple2<String, Group>(t._2.getPDBCode()+"."+g.getResidueNumber(), g));
			}
		}
		return outList;
	} 


}