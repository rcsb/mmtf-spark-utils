package org.rcsb.mmtf.filters;

import org.apache.spark.api.java.function.Function;

public class IdFilter implements Function<String, Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Boolean call(String v1) throws Exception {
		if(v1.startsWith("1")==true){
			return true;
		}
		else{
			return false;
		}
	}

}
