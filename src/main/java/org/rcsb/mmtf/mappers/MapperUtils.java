package org.rcsb.mmtf.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureImpl;
import org.rcsb.mmtf.biojavaencoder.EncoderUtils;
import org.rcsb.mmtf.biojavaencoder.ParseFromBiojava;
import org.rcsb.mmtf.dataholders.BioDataStruct;
import org.rcsb.mmtf.dataholders.CalphaDistBean;
import org.rcsb.mmtf.dataholders.HeaderBean;
import org.rcsb.mmtf.decoder.BioJavaStructureDecoder;
import org.rcsb.mmtf.decoder.DecodeStructure;
import org.rcsb.mmtf.decoder.ParsingParams;

import scala.Tuple2;


/**
 * A class to preserve the log if the functions in mappers. 
 * Mappers should not contain logic - as they are hard to test.
 * @author Anthony Bradley
 *
 */
public class MapperUtils {
	
	/**
	 * Converts a byte array of the messagepack (mmtf) to a Biojava structure. 
	 * @param pdbCodePlus The pdb code is the first four characters. Additional characters can be used.
	 * @param inputByteArr The message pack bytre array to be decoded.
	 * @return
	 */
	public Structure byteArrToBiojavaStruct(String pdbCodePlus, byte[] inputByteArr) { 
		BioJavaStructureDecoder bjs = new BioJavaStructureDecoder();
		Structure newStruct;
		ParsingParams pp = new ParsingParams();
		try{
			DecodeStructure ds = new DecodeStructure(inputByteArr);
		ds.getStructFromByteArray(bjs, pp);
		newStruct = bjs.getStructure();
		newStruct.setPDBCode(pdbCodePlus.substring(0,4));}
		catch(Exception e){
			System.out.println(e);
			System.out.println(pdbCodePlus);
			Structure thisStruct = new StructureImpl();
			return thisStruct;
		}
		return newStruct;
	}

	/**
	 * 
	 * @param pdbCode
	 * @param parseFromBiojava
	 * @return
	 * @throws IOException 
	 */
	public List<Tuple2<String, byte[]>> getAllDataTypes(String pdbCode, ParseFromBiojava parseFromBiojava) throws IOException {
		
		// First generate the list to return
		List<Tuple2<String, byte[]>> outList = new ArrayList<Tuple2<String, byte[]>>();
		EncoderUtils cm = new EncoderUtils();
		ParseFromBiojava cbs = parseFromBiojava;
		// Now get the header too
		HeaderBean headerData = cbs.getHeaderStruct();
		BioDataStruct thisBS = cbs.getBioStruct();
		CalphaDistBean calphaDistStruct = cm.compCAlpha(cbs.getCalphaStruct(), cbs.getHeaderStruct());
		// NOW JUST WRITE THE KEY VALUE PAIRS HERE
		byte[] totBytes = cm.getMessagePack(cm.compressMainData(thisBS, headerData));
		byte[] headerBytes = cm.getMessagePack(headerData);
		byte[] calphaBytes = cm.getMessagePack(calphaDistStruct);
		// Add the total data
		outList.add(new Tuple2<String, byte[]>(pdbCode+"_total", totBytes));
		// Add the header
		outList.add(new Tuple2<String, byte[]>(pdbCode+"_header", headerBytes));
		// Add the calpha
		outList.add(new Tuple2<String, byte[]>(pdbCode+"_calpha", calphaBytes));
		return outList;
	}
	
	/**
	 * PDB RDD gnerateor. Converts a list of pdb ids to a writeable RDD
	 * @param sparkContext
	 * @return
	 */
	public JavaPairRDD<Text, BytesWritable> generateRDD(JavaSparkContext sparkContext, List<String> inputList) {
		// Set up Biojava appropriateyl
		EncoderUtils encoderUtils = new EncoderUtils();
		encoderUtils.setUpBioJava();
		return sparkContext.parallelize(inputList)
		.mapToPair(new UpdateToCBSMapper())
		.flatMapToPair(new CBSToBytes())
		.mapToPair(new StringByteToTextByteWriter());
	}
}
