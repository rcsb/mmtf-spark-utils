package org.rcsb.mmtf.filters;

import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.rcsb.mmtf.dataholders.CalphaAlignBean;

import scala.Tuple2;

public class LengthDiffFilter implements Function<Tuple2<Integer, Integer>, Boolean> {


	/**
	 * 
	 */
	private static final long serialVersionUID = 5020491570385859408L;


	private List<Tuple2<String, CalphaAlignBean>> data;
	private int maxDiff;


	public LengthDiffFilter(int inputMaxDiff, Broadcast<List<Tuple2<String,CalphaAlignBean>>> chainsBc){
		data = chainsBc.getValue();
		maxDiff = inputMaxDiff;

	}

	@Override
	public Boolean call(Tuple2<Integer, Integer> v1) throws Exception {

		if(Math.abs(data.get(v1._1)._2.getSequence().length() - data.get(v1._2)._2.getSequence().length()) > maxDiff){
			return false;
		}
		else{
			return true;
		}

	}

}
