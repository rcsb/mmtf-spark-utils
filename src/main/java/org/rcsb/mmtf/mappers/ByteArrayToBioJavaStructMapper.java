package org.rcsb.mmtf.mappers;

import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureImpl;
import org.rcsb.mmtf.decoder.BioJavaStructureDecoder;
import org.rcsb.mmtf.decoder.DecodeStructure;
import org.rcsb.mmtf.decoder.ParsingParams;

import scala.Tuple2;

/**
 * Maps a String byte[] of a message pack to a String Structure of the biojava structure
 * @author abradley
 *
 */
public class ByteArrayToBioJavaStructMapper implements PairFunction<Tuple2<String, byte[]>,String, Structure> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1671280971380509379L;


	@Override
	public Tuple2<String, Structure> call(Tuple2<String, byte[]> t) throws Exception {
		DecodeStructure ds = new DecodeStructure();
		BioJavaStructureDecoder bjs = new BioJavaStructureDecoder();
		Structure newStruct;
		ParsingParams pp = new ParsingParams();
		try{
		ds.getStructFromByteArray(t._2, bjs, pp);
		newStruct = bjs.getStructure();
		newStruct.setPDBCode(t._1.substring(0,4));}
		catch(Exception e){
			System.out.println(e);
			System.out.println(t._1);
			Structure thisStruct = new StructureImpl();
			return new Tuple2<String, Structure>(t._1,thisStruct);
		}
		return new Tuple2<String, Structure>(t._1, newStruct);
	}
}
