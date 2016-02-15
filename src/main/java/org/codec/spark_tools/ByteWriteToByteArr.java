package org.codec.spark_tools;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ByteWriteToByteArr implements PairFunction<Tuple2<Text, BytesWritable>,String, byte[]> {


	/**
	 * 
	 */
	private static final long serialVersionUID = 1466772536507675533L;

	@Override
	public Tuple2<String, byte[]> call(Tuple2<Text, BytesWritable> t) throws Exception {
		// TODO Auto-generated method stub
		return new Tuple2<String, byte[]>(t._1.toString(), t._2.copyBytes());
	}

}
