package org.codec.filters;


import org.apache.spark.api.java.function.Function;
import org.biojava.nbio.structure.Group;

import scala.Tuple2;

/**
 * Filter to check if BioJava group is a ligand
 * @author abradley
 *
 */
public class IsLigand implements Function<Tuple2<String, Group>, Boolean> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5486322301757092895L;

	@Override
	public Boolean call(Tuple2<String, Group> t) throws Exception {
		// Let's see if this guy is a ligand;
		Group inGroup = t._2;
		// Easiest one
		if(inGroup.isWater()==true){
			return false;
		}
		else if(isBanned(inGroup)==true){
			return false;
		}
		// HetID based filters for solvent molecules
		else if(isSolvent(inGroup)==true){
			return false;
		}
		// HetID based filters for buffer molecules
		else if(isGlycan(inGroup)==true){
			return false;
		}
		else if(isLigand(inGroup)==true){
			return true;
		}
		
		else{
			return false;
		}
		
	}

	/**
	 * Collection of functions to check if a group is allowed or not
	 * @param inGroup
	 * @return
	 */
	private boolean isBanned(Group inGroup) {
		//
		if(inGroup.getPDBName().equals("NAP")){
			return true;
		}
		if(inGroup.getPDBName().equals("NAD")){
			return true;
		}
		if(inGroup.getPDBName().equals("HEM")){
			return true;
		}
		if(inGroup.getPDBName().equals("DEQ")){
			return true;
		}
		if(inGroup.getPDBName().equals("CFN")){
			return true;
		}
		if(inGroup.getPDBName().equals("1PS")){
			return true;
		}
		if(inGroup.getPDBName().equals("08T")){
			return true;
		}
		if(inGroup.getPDBName().equals("MG7")){
			return true;
		}
		
		return false;
	}


	/**
	 * As yet un made function to check if a group is a ligand
	 * @param inGroup
	 * @return
	 */
	private boolean isLigand(Group inGroup) {
		if(inGroup.getType()==org.biojava.nbio.structure.GroupType.HETATM){
			return true;
		}
		return false;
	}

	/**
	 * As yet un made function to check if a group is a glycan
	 * @param inGroup
	 * @return
	 */
	private boolean isGlycan(Group inGroup) {
		return false;
	}

	/**
	 * As yet un made function to check if a group is a solvent
	 * @param inGroup
	 * @return
	 */
	private boolean isSolvent(Group inGroup) {
		return false;
	}

}
