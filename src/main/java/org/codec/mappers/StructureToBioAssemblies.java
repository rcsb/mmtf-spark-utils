package org.codec.mappers;
/*
 *                    BioJava development code
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  If you do not have a copy,
 * see:
 *
 *      http://www.gnu.org/copyleft/lesser.html
 *
 * Copyright for this code is held jointly by the individual
 * authors.  These should be listed in @author doc comments.
 *
 * For more information on the BioJava project and its aims,
 * or to join the biojava-l mailing list, visit the home page
 * at:
 *
 *      http://www.biojava.org/
 *
 */

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.biojava.nbio.structure.*;
import org.biojava.nbio.structure.quaternary.BioAssemblyInfo;
import org.biojava.nbio.structure.quaternary.BiologicalAssemblyBuilder;
import org.biojava.nbio.structure.quaternary.BiologicalAssemblyTransformation;
import org.biojava.nbio.structure.quaternary.io.BioUnitDataProvider;
import org.biojava.nbio.structure.quaternary.io.BioUnitDataProviderFactory;


import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


//public class ScanSymmetry implements Function<Structure, List<String>>{
public class StructureToBioAssemblies implements PairFlatMapFunction<Tuple2<String,Structure>, String, Structure>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8212007432492408878L;


	private List<Tuple2<String, Structure>> runFunc(String pdbId, Structure structure) throws StructureException {
		// Get the pdbHeader
		PDBHeader pdbHeader = structure.getPDBHeader();
		Map<Integer, BioAssemblyInfo> bioAss = pdbHeader.getBioAssemblies();
		// Create the list to store this set of results in
		List<Tuple2<String, Structure>> outList = new ArrayList<Tuple2<String, Structure>>();
		// Now just the asymunit
		Tuple2<String, Structure> firstTup = new Tuple2<String, Structure>(pdbId+".0", structure);
		outList.add(firstTup);
		for (int key: bioAss.keySet()) {
			if(key==0){
				continue;
			}
			// Now get this bioassembly
			Structure thisBioAss = getBioAssembly(structure, key);
			// 
			String thisName = pdbId+"."+key;
			Tuple2<String, Structure> outTup = new Tuple2<String, Structure>(thisName, thisBioAss);
			outList.add(outTup);
		}
		return outList;
	}

	private Structure getBioAssembly(Structure asymUnit, int biolAssemblyNr) throws StructureException {
		// 0 ... asym unit
		// Here use this info to get the bioassembly info
		BioUnitDataProvider provider = BioUnitDataProviderFactory.getBioUnitDataProvider();
		List<BiologicalAssemblyTransformation> transformations = 
				asymUnit.getPDBHeader().getBioAssemblies().get(biolAssemblyNr).getTransforms();
		//		//cleanup to avoid memory leaks
		provider.setAsymUnit(null);
		provider.setAtomCache(null);
		//
		if ( transformations == null || transformations.size() == 0){
			throw new StructureException("Could not load transformations to recreate biological assembly nr " + biolAssemblyNr + " of " + asymUnit.getPDBCode());
		}
		//		// Now build the bio assembly
		Structure answer;
		BiologicalAssemblyBuilder builder = new BiologicalAssemblyBuilder();
		try{
			answer = builder.rebuildQuaternaryStructure(asymUnit, transformations);
		}
		catch(ArrayIndexOutOfBoundsException e){
			// An array related to this
			System.out.println(e);
			System.out.println(asymUnit.getPDBCode());
			answer = null;
		}
		return answer;
	}


	@Override
	public Iterable<Tuple2<String, Structure>> call(Tuple2<String, Structure> t) throws Exception {
		// TODO Auto-generated method stub
		return runFunc(t._1, t._2);
	}
}