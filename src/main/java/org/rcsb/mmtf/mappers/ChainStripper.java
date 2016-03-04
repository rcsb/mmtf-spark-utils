package org.rcsb.mmtf.mappers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.rcsb.mmtf.arraydecompressors.DeltaDeCompress;
import org.rcsb.mmtf.dataholders.CalphaAlignBean;
import org.rcsb.mmtf.dataholders.CalphaDistBean;
import org.rcsb.mmtf.dataholders.PDBGroup;
import org.rcsb.mmtf.decoder.DecoderUtils;

import scala.Tuple2;

/**
 * Method to strip down the chain to just the polymer (remove ligands) - and return multiple chains
 * @author anthony
 *
 */
public class ChainStripper implements PairFlatMapFunction<Tuple2<String,CalphaDistBean>, String, CalphaAlignBean>{

	private int groupCounter;
	private int atomCounter;
	private int[] groupList;
	private int[] groupsPerChain;
	private int[] cartnX;
	private int[] cartnY;
	private int[] cartnZ;
	private Map<Integer, PDBGroup> groupMap;
	byte[] chainList;
	private List<String> calphaArr;
	private List<String> dnarnaArr;
	
	private static final long serialVersionUID = -8516822489889006992L;

	@Override
	public Iterable<Tuple2<String, CalphaAlignBean>> call(Tuple2<String, CalphaDistBean> t) throws Exception {
		// Loop through the data structure and output a new one - on a per chain level
		// The out array to produce
		DecoderUtils decoderUtils = new DecoderUtils();
		List<Tuple2<String,CalphaAlignBean>> outArr = new ArrayList<Tuple2<String, CalphaAlignBean>>();
		DeltaDeCompress delta = new DeltaDeCompress();
		CalphaDistBean xs = t._2;
		// Get the coordinates
		cartnX = delta.decompressByteArray(xs.getxCoordBig(),xs.getxCoordSmall());
		cartnY = delta.decompressByteArray(xs.getyCoordBig(),xs.getyCoordSmall());
		cartnZ = delta.decompressByteArray(xs.getzCoordBig(),xs.getzCoordSmall());
		groupMap = xs.getGroupMap();
		chainList = xs.getChainList();
		// Loop through the chains
		groupCounter = 0;
		atomCounter = 0;
		groupList = decoderUtils.bytesToInts(xs.getGroupTypeList());
		groupsPerChain = xs.getGroupsPerChain();

		int numChains = xs.getChainsPerModel()[0];
		// Now set the requirements for a calpha group
		calphaArr = new ArrayList<String>();
		calphaArr.add("C");
		calphaArr.add("CA");
		dnarnaArr = new ArrayList<String>();
		dnarnaArr.add("P");
		dnarnaArr.add("P");
		
		// GENERATE THe ARRAYS TO OUTPUT		
		for (int i=0; i<numChains;i++){
			CalphaAlignBean outChain;
			try{
			outChain = getChain(i, xs.getPdbId(), decoderUtils.getChainId(chainList, i));
			outArr.add(new Tuple2<String, CalphaAlignBean>(outChain.getPdbId(), outChain));
			}
			catch(Exception e){
				System.out.println("ERROR WITH "+xs.getPdbId()+" CHAIN"+decoderUtils.getChainId(chainList, i));
				System.out.println(e.getMessage());
			}
		}
		return  outArr;
	}

	private CalphaAlignBean getChain(int i, String pdbId, String chainId) {
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
		outChain.setPdbId(pdbId);
		outChain.setChainId(chainId);
		outChain.setCoordList(thesePoints.toArray(new Point3d[thesePoints.size()]));
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
		return outChain;
	}


}
