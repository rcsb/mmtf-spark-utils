package org.codec.spark_tools;

import java.util.List;

import javax.management.modelmbean.ModelMBeanNotificationBroadcaster;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.GroupType;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureException;

import scala.Tuple2;

/**
 * 
 * @author anthony
 *Class to get all Fragments of a structure
 */
public class GetAllFragments implements PairFlatMapFunction<Tuple2<String,Structure>, String, Structure>{

	int fragSize = 9;
	
	@Override
	public Iterable<Tuple2<String, Structure>> call(Tuple2<String, Structure> t) throws Exception {
		// Function to get all the fragments - and return in a tuple
		for(Chain c: t._2.getChains()){
			List<Group> myGroups = c.getAtomGroups(GroupType.AMINOACID);
//
//			for(){
//				
//			}
		}
		
		return null;
	}



}