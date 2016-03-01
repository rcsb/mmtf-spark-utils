package org.codec.mappers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.codec.arraydecompressors.DeltaDeCompress;
import org.codec.biojavaencoder.EncoderUtils;
import org.codec.dataholders.CalphaDistBean;
import org.codec.dataholders.PDBGroup;
import org.codec.decoder.DecodeStructure;

import scala.Tuple2;

/**
 * Method to strip down the chain to just the polymer (remove ligands) - and return multiple chains
 * @author anthony
 *
 */
public class ChainStripper implements PairFlatMapFunction<Tuple2<String,CalphaDistBean>, String, CalphaDistBean>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8516822489889006992L;

	@Override
	public Iterable<Tuple2<String, CalphaDistBean>> call(Tuple2<String, CalphaDistBean> t) throws Exception {
		// Loop through the data structure and output a new one - on a per chain level
		// The out array to produce
		EncoderUtils eu = new EncoderUtils();
		List<Tuple2<String, CalphaDistBean>> outArr = new ArrayList<Tuple2<String, CalphaDistBean>>();
		DeltaDeCompress delta = new DeltaDeCompress();
		DecodeStructure ds = new DecodeStructure();
		CalphaDistBean xs = t._2;
		// Get the coordinates
		int[] cartnX = delta.decompressByteArray(xs.getxCoordBig(),xs.getxCoordSmall());
		int[] cartnY = delta.decompressByteArray(xs.getyCoordBig(),xs.getyCoordSmall());
		int[] cartnZ = delta.decompressByteArray(xs.getzCoordBig(),xs.getzCoordSmall());
		int[] groupsPerChain = xs.getGroupsPerChain();
		int[] secStructList = bytesToByteInts(xs.getSecStructList());
		Map<Integer, PDBGroup> groupMap = xs.getGroupMap();
		int numChains = xs.getChainsPerModel()[0];
		byte[] chainList = xs.getChainList();
		// Loop through the chains
		int groupCounter = 0;
		int atomCounter = 0;
		int[] groupList = ds.bytesToInts(xs.getGroupTypeList());
		List<String> calphaArr = new ArrayList<String>();
		calphaArr.add("C");
		calphaArr.add("CA");
		List<String> dnarnaArr = new ArrayList<String>();
		dnarnaArr.add("P");
		dnarnaArr.add("P");
		// GENERATE THe ARRAYS TO OUTPUT		
		for (int i=0; i<numChains;i++){
			CalphaDistBean outChain = new CalphaDistBean();
			
			outChain.setBioAssembly(xs.getBioAssembly());
			outChain.setChainList(java.util.Arrays.copyOfRange(chainList, i*4, i*4+4));
			outChain.setChainsPerModel(new int[] {1});
			outChain.setGroupMap(xs.getGroupMap());
			
			int groupsThisChain = groupsPerChain[i];
			int newGroupsThisChain = 0;
			char[] newOneLetterCodeList = new char[groupsThisChain];
			List<Integer> newGroupTypeList = new ArrayList<Integer>();
			List<Integer> newXCoord = new ArrayList<Integer>();
			List<Integer> newYCoord = new ArrayList<Integer>();
			List<Integer> newZCoord = new ArrayList<Integer>();
			List<Integer> newSecStructList = new ArrayList<Integer>();
			int newNumAtoms = 0;
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
				for(int k=0;k<atomCount;k++){
					newXCoord.add(cartnX[atomCounter+k]);
					newYCoord.add(cartnY[atomCounter+k]);
					newZCoord.add(cartnZ[atomCounter+k]);
				}
				atomCounter+=atomCount;
				newGroupTypeList.add(g);
				newOneLetterCodeList[j] = thisGroup.getSingleLetterCode().charAt(0);
				newSecStructList.add(secStructList[groupCounter]);
				newGroupsThisChain++;
				newNumAtoms+=atomCount;
			}
			// Set data for Chain
			outChain.setGroupsPerChain(new int[] {newGroupsThisChain});
			outChain.setGroupTypeList(eu.integersToBytes(newGroupTypeList));
			outChain.setMmtfProducer(xs.getMmtfProducer());
			outChain.setMmtfVersion(xs.getMmtfVersion());;
			outChain.setNumAtoms(newNumAtoms);
			outChain.setNumBonds(0);
			outChain.setPdbId(xs.getPdbId()+"."+ds.getChainId(chainList, i));
			outChain.setSecStructList(eu.integersToSmallBytes(newSecStructList));
			outChain.setSpaceGroup(xs.getSpaceGroup());
			outChain.setTitle(xs.getTitle());
			outChain.setUnitCell(xs.getUnitCell());
			
			// Set the coord arrays
			List<byte[]> bigAndLittleX = eu.getBigAndLittle(newXCoord);
			outChain.setxCoordBig(bigAndLittleX.get(0));
			outChain.setxCoordSmall(bigAndLittleX.get(1));
			List<byte[]> bigAndLittleY = eu.getBigAndLittle(newYCoord);
			outChain.setyCoordBig(bigAndLittleY.get(0));
			outChain.setyCoordSmall(bigAndLittleY.get(1));
			List<byte[]> bigAndLittleZ = eu.getBigAndLittle(newZCoord);
			outChain.setzCoordBig(bigAndLittleZ.get(0));
			outChain.setzCoordSmall(bigAndLittleZ.get(1));
			// Now set the one letter amimo sequence
			outChain.setOneLetterAminSeq(newOneLetterCodeList);
			// Now add this
			outArr.add(new Tuple2<String, CalphaDistBean>(outChain.getPdbId(), outChain));
		}
		
		return  outArr;
	}

	private int[] bytesToByteInts(byte[] secStructList) {
		int[] outArr = new int[secStructList.length];
		for(int i =0; i<secStructList.length; i++){
			outArr[i] = (int) secStructList[i];
		}
		return outArr;
	}

}
