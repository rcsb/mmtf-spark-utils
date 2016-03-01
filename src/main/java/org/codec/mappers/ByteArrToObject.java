package org.codec.mappers;


import java.util.List;

import org.apache.spark.api.java.function.PairFunction;
import org.codec.dataholders.CalphaDistBean;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class ByteArrToObject implements PairFunction<Tuple2<String,byte[]>, String, CalphaDistBean>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8225004637753153167L;

	@Override
	public Tuple2<String, CalphaDistBean> call(Tuple2<String, byte[]> t) throws Exception {
		// 
		ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
		CalphaDistBean thisObj = objectMapper.readValue(t._2, CalphaDistBean.class);
		return new Tuple2<String, CalphaDistBean>(t._1, thisObj);
	}
}
