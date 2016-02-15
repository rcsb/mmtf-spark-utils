package org.codec.spark_tools;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.Structure;

import scala.Tuple2;

public class StringByteToTextByteWritableMapper implements PairFunction<Tuple2<String, byte[]>,Text, BytesWritable>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7030801385040583741L;

	@Override
	public Tuple2<Text, BytesWritable> call(Tuple2<String, byte[]> t) throws Exception {
		// TODO Auto-generated method stub
		Text outT = new Text();
		outT.set(t._1);
		byte[] theseBytes = t._2;
		BytesWritable outBytes = new BytesWritable();
		outBytes.set(theseBytes, 0, theseBytes.length);
		return new Tuple2<Text, BytesWritable>(outT,outBytes);
	}

}
