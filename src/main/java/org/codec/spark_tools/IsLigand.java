package org.codec.spark_tools;

import java.util.List;

import org.apache.hadoop.hdfs.server.datanode.tail_jsp;
import org.apache.spark.api.java.function.Function;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.Element;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;

import scala.Tuple2;

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

	private boolean hasMetal(Group inGroup) {
		for(Atom a: inGroup.getAtoms()){
			Element ele = a.getElement();
			if(ele.isMetal()){
				return true;
			}
		}
		return true;
	}

	private boolean isLigand(Group inGroup) {
		// TODO Auto-generated method stub
		if(inGroup.getType()==org.biojava.nbio.structure.GroupType.HETATM){
			return true;
		}
		return false;
	}

	private boolean isGlycan(Group inGroup) {
		// TODO Auto-generated method stub
		return false;
	}

	private boolean isSolvent(Group inGroup) {
		// TODO Auto-generated method stub
		return false;
	}

}
