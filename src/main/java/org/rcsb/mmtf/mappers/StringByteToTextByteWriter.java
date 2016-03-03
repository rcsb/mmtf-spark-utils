package org.rcsb.mmtf.mappers;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class StringByteToTextByteWriter implements PairFunction<Tuple2<String,byte[]>, Text, BytesWritable>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8149053011560186912L;

	@Override
	public Tuple2<Text, BytesWritable> call(Tuple2<String, byte[]> t) throws Exception {
		// TODO Auto-generated method stub
		Text outT = new Text();
		outT.set(t._1);
		BytesWritable outBytes = new BytesWritable();
		byte[] theseBytes = t._2;
		outBytes.set(theseBytes, 0, theseBytes.length);
		return new Tuple2<Text, BytesWritable>(outT,outBytes);
	}

}
