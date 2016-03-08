package org.rcsb.mmtf.mappers;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.rcsb.mmtf.biojavaencoder.EncoderUtils;
import org.rcsb.mmtf.biojavaencoder.ParseFromBiojava;
import org.rcsb.mmtf.dataholders.BioDataStruct;
import org.rcsb.mmtf.dataholders.CalphaDistBean;
import org.rcsb.mmtf.dataholders.HeaderBean;

import scala.Tuple2;

public class CBSToBytes  implements PairFlatMapFunction<Tuple2<String,ParseFromBiojava>, String, byte[]>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2066093446043635571L;

	@Override
	public Iterable<Tuple2<String, byte[]>> call(Tuple2<String, ParseFromBiojava> t) throws IOException, IllegalAccessException, InvocationTargetException {
		// First generate the list to return
		List<Tuple2<String, byte[]>> outList = new ArrayList<Tuple2<String, byte[]>>();
		EncoderUtils cm = new EncoderUtils();
		ParseFromBiojava cbs = t._2;
		// Now get the header too
		HeaderBean headerData = cbs.getHeaderStruct();
		BioDataStruct thisBS = cbs.getBioStruct();
		CalphaDistBean calphaDistStruct = cm.compCAlpha(cbs.getCalphaStruct(), cbs.getHeaderStruct());
		// NOW JUST WRITE THE KEY VALUE PAIRS HERE
		byte[] totBytes = cm.getMessagePack(cm.compressMainData(thisBS, headerData));
		byte[] headerBytes = cm.getMessagePack(headerData);
		byte[] calphaBytes = cm.getMessagePack(calphaDistStruct);
		// Add the total data
		outList.add(new Tuple2<String, byte[]>(t._1+"_total", totBytes));
		// Add the header
		outList.add(new Tuple2<String, byte[]>(t._1+"_header", headerBytes));
		// Add the calpha
		outList.add(new Tuple2<String, byte[]>(t._1+"_calpha", calphaBytes));
		return outList;
	}
}