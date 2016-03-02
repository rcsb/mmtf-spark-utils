package org.codec.mappers;

import org.apache.spark.api.java.function.PairFunction;
import org.codec.biojavaencoder.EncoderUtils;
import org.codec.dataholders.CalphaAlignBean;

import scala.Tuple2;

public class ObjectToByteArr implements PairFunction<Tuple2<String, CalphaAlignBean>, String, byte[]>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8225004637753153167L;

	@Override
	public Tuple2<String, byte[]> call(Tuple2<String, CalphaAlignBean> t) throws Exception {
		EncoderUtils eu = new EncoderUtils();
		return new Tuple2<String, byte[]>(t._1,eu.getMessagePack(t._2));
	}


}
