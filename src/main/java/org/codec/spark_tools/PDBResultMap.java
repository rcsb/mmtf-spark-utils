package org.codec.spark_tools;

import java.util.HashMap;
import java.util.Map;

import org.codec.dataholders.PDBGroup;

public class PDBResultMap {
	private Map<Integer, Integer> countMap;
	private Map<Integer, PDBGroup> totMap;
	public Map<Integer, Integer> getCountMap() {
		return countMap;
	}
	public void setCountMap(Map<Integer, Integer> countMap) {
		this.countMap = countMap;
	}
	public Map<Integer, PDBGroup> getTotMap() {
		return totMap;
	}
	public void setTotMap(Map<Integer, PDBGroup> totMap) {
		this.totMap = totMap;
	}
}
