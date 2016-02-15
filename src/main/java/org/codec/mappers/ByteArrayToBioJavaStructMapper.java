package org.codec.mappers;

import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.Structure;
import org.codec.decoder.BioJavaStructureInflator;
import org.codec.decoder.DecodeStructure;
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
		BioJavaStructureInflator bjs = new BioJavaStructureInflator();
		ds.getStructFromByteArray(t._2, bjs);
		Structure newStruct = bjs.getStructure();
		newStruct.setPDBCode(t._1);
		return new Tuple2<String, Structure>(t._1, newStruct);
	}
}
