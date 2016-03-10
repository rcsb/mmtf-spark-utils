package org.rcsb.mmtf.mappers;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.rcsb.mmtf.biojavaencoder.ParseFromBiojava;

import scala.Tuple2;

public class CBSToBytes  implements PairFlatMapFunction<Tuple2<String,ParseFromBiojava>, String, byte[]>{

	private static final long serialVersionUID = 2066093446043635571L;
	
	private MapperUtils mapperUtils = new MapperUtils();

	@Override
	public Iterable<Tuple2<String, byte[]>> call(Tuple2<String, ParseFromBiojava> t) throws IOException, IllegalAccessException, InvocationTargetException {
		return mapperUtils.getAllDataTypes(t._1, t._2);
	}
}