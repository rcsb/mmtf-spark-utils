package org.codec.spark_tools;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.codec.dataholders.BioDataStruct;
import org.codec.dataholders.CalphaDistBean;
import org.codec.dataholders.CreateBasicStructure;
import org.codec.dataholders.HeaderBean;
import org.codec.biojavaencoder.EncoderUtils;

import scala.Tuple2;

public class FlatMapToBytes  implements PairFlatMapFunction<Tuple2<String,CreateBasicStructure>, String, byte[]>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2066093446043635571L;

	@Override
	public Iterable<Tuple2<String, byte[]>> call(Tuple2<String, CreateBasicStructure> t) throws IOException, IllegalAccessException, InvocationTargetException {
		// First generate the list to return
		List<Tuple2<String, byte[]>> outList = new ArrayList<Tuple2<String, byte[]>>();
		EncoderUtils cm = new EncoderUtils();
		CreateBasicStructure cbs = t._2;
		// Now get the header too
		HeaderBean headerData = cbs.getHeaderStruct();
		BioDataStruct thisBS = cbs.getBioStruct();
		CalphaDistBean calphaDistStruct = cm.compCAlpha(cbs.getCalphaStruct(), cbs.getHeaderStruct());
		// NOW JUST WRITE THE KEY VALUE PAIRS HERE
		byte[] totBytes = cm.compressMainData(thisBS, headerData);
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