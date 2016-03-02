package org.codec.mappers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.codec.arraydecompressors.DeltaDeCompress;
import org.codec.dataholders.CalphaAlignBean;
import org.codec.dataholders.CalphaDistBean;
import org.codec.dataholders.PDBGroup;
import org.codec.decoder.DecodeStructure;

import scala.Tuple2;

/**
 * Method to strip down the chain to just the polymer (remove ligands) - and return multiple chains
 * @author anthony
 *
 */
public class ChainStripper implements PairFlatMapFunction<Tuple2<String,CalphaDistBean>, String, CalphaAlignBean>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8516822489889006992L;

	@Override
	public Iterable<Tuple2<String, CalphaAlignBean>> call(Tuple2<String, CalphaDistBean> t) throws Exception {
		// Loop through the data structure and output a new one - on a per chain level
		// The out array to produce
		List<Tuple2<String,CalphaAlignBean>> outArr = new ArrayList<Tuple2<String, CalphaAlignBean>>();
		DeltaDeCompress delta = new DeltaDeCompress();
		DecodeStructure ds = new DecodeStructure();
		CalphaDistBean xs = t._2;
		// Get the coordinates
		int[] cartnX = delta.decompressByteArray(xs.getxCoordBig(),xs.getxCoordSmall());
		int[] cartnY = delta.decompressByteArray(xs.getyCoordBig(),xs.getyCoordSmall());
		int[] cartnZ = delta.decompressByteArray(xs.getzCoordBig(),xs.getzCoordSmall());
		int[] groupsPerChain = xs.getGroupsPerChain();
		Map<Integer, PDBGroup> groupMap = xs.getGroupMap();
		int numChains = xs.getChainsPerModel()[0];
		byte[] chainList = xs.getChainList();
		// Loop through the chains
		int groupCounter = 0;
		int atomCounter = 0;
		int[] groupList = ds.bytesToInts(xs.getGroupTypeList());
		// Now set the requirements for a calpha group
		List<String> calphaArr = new ArrayList<String>();
		calphaArr.add("C");
		calphaArr.add("CA");
		List<String> dnarnaArr = new ArrayList<String>();
		dnarnaArr.add("P");
		dnarnaArr.add("P");
		
		// GENERATE THe ARRAYS TO OUTPUT		
		for (int i=0; i<numChains;i++){
			boolean peptideFlag = false;
			boolean dnaRnaFlag = false;
			CalphaAlignBean outChain = new CalphaAlignBean();
			int groupsThisChain = groupsPerChain[i];
			char[] newOneLetterCodeList = new char[groupsThisChain];
			List<Point3d> thesePoints = new ArrayList<Point3d>();
			for(int j=0; j<groupsThisChain;j++){
				int g = groupList[groupCounter];
				// Now increment the groupCounter
				groupCounter++;
				PDBGroup thisGroup = groupMap.get(g);
				List<String> atomInfo = thisGroup.getAtomInfo();
				// Now check - this is protein / DNA or RNA
				int atomCount = atomInfo.size()/2;
				if(atomCount<2){
					if(atomInfo.equals(calphaArr)==false && atomInfo.equals(dnarnaArr)==false){
						atomCounter+=atomCount;
						continue;
					}
				}
				else{
					atomCounter+=atomCount;
					continue;
				}
				if(atomInfo.equals(calphaArr)==true){
					peptideFlag=true;
				}
				if(atomInfo.equals(dnarnaArr)==true){
					dnaRnaFlag=true;
				}
				for(int k=0;k<atomCount;k++){
					Point3d newPoint = new Point3d(); 
					newPoint.x = cartnX[atomCounter+k]/1000.0;
					newPoint.y = cartnY[atomCounter+k]/1000.0;
					newPoint.z = cartnZ[atomCounter+k]/1000.0;
					thesePoints.add(newPoint);
				}
				atomCounter+=atomCount;
				newOneLetterCodeList[j] = thisGroup.getSingleLetterCode().charAt(0);
			}
			// Set data for Chain
			outChain.setPdbId(xs.getPdbId());
			outChain.setChainId(ds.getChainId(chainList, i));
			outChain.setCoordList(thesePoints);
			outChain.setSequence(newOneLetterCodeList);
			if(peptideFlag==true){
				if(dnaRnaFlag==true){
					outChain.setPolymerType("NUCLEOTIDE_PEPTIDE");

				}
				else{
					outChain.setPolymerType("PEPTIDE");

				}
			}
			else if(dnaRnaFlag==true){
				outChain.setPolymerType("NUCLEOTIDE");
			}
			outArr.add(new Tuple2<String, CalphaAlignBean>(outChain.getPdbId(), outChain));
		}
		return  outArr;
	}


}
