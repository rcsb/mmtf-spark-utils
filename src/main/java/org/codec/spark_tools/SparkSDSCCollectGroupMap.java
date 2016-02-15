package org.codec.spark_tools;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.codec.dataholders.PDBGroup;
import org.codec.biojavaencoder.EncoderUtils;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import scala.Tuple2;

public class SparkSDSCCollectGroupMap {

	public static void main(String[] args) throws Exception {
		//		SparkSDSCHashMapWriter sdhw = new SparkSDSCHashMapWriter();
		//		// The path of the hadoop file
		//		int numThreads = 24;
		//		String uri = "/home/anthony/src/codec-devel/Total.hadoop.TRIAL.bzip2";
		//		// This is the default 2 line structure for Spark applications
		//		SparkConf conf = new SparkConf().setMaster("local[" + numThreads + "]")
		//				.setAppName(SparkRead.class.getSimpleName());
		//		conf.set("spark.driver.maxResultSize", "14g");
		//		// Set the config
		//		JavaSparkContext sc = new JavaSparkContext(conf);
		//		// Read in with spark
		//		JavaPairRDD<String, Map<Integer,PDBGroup>> totalDataset = sc
		//				.sequenceFile(uri, Text.class, BytesWritable.class, 4 * 3)
		//				.mapToPair(new ByteWriteToByteArr())
		//				.filter(new Function<Tuple2<String,byte[]>, Boolean>() {
		//
		//					/**
		//					 * 
		//					 */
		//					private static final long serialVersionUID = -879876123409048115L;
		//
		//					@Override
		//					public Boolean call(Tuple2<String, byte[]> v1) throws Exception {
		//						// TODO Auto-generated method stub
		//						if(v1._1.endsWith("_total")){
		//							return true;
		//						}
		//						return false;
		//					}
		//				})
		//				.mapToPair(new PairFunction<Tuple2<String,byte[]>, String, HadoopDataStructDistBean>() {
		//					/**
		//					 * 
		//					 */
		//					private static final long serialVersionUID = -4167100210386845048L;
		//
		//					@Override
		//					public Tuple2<String, HadoopDataStructDistBean> call(Tuple2<String, byte[]> t) throws Exception {
		//						// TODO Auto-generated method stub
		//						com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper(new MessagePackFactory());;
		//						HadoopDataStructDistBean xs = objectMapper.readValue(t._2, HadoopDataStructDistBean.class);
		//						return new  Tuple2<String, HadoopDataStructDistBean>(t._1, xs);
		//					}
		//
		//
		//				}).mapToPair(new PairFunction<Tuple2<String,HadoopDataStructDistBean>, String,  Map<Integer,PDBGroup>>() {
		//
		//					/**
		//					 * 
		//					 */
		//					private static final long serialVersionUID = -5450614496373252184L;
		//
		//					@Override
		//					public Tuple2<String, Map<Integer, PDBGroup>> call(Tuple2<String, HadoopDataStructDistBean> t)
		//							throws Exception {
		//						// TODO Auto-generated method stub
		//						String pdbCode = t._1;
		//						Map<Integer, PDBGroup> groupMap = t._2.getGroupMap();
		//						return new Tuple2<String, Map<Integer, PDBGroup>>(pdbCode, groupMap);
		//					}
		//				});
		//
		//		// Now look through this and build a total map
		//		List<Map<Integer, PDBGroup>> values = totalDataset.values().collect();
		//		SparkSDSCCollectGroupMap sdcgm = new SparkSDSCCollectGroupMap();
		//		// 
		//		Map<Integer, Integer> countMap = new HashMap<Integer,Integer>();
		//		Map<Integer, PDBGroup> totMap = new HashMap<Integer, PDBGroup>();
		//		for(Map<Integer, PDBGroup> thisMap : values){
		//			for(Integer key: thisMap.keySet()){
		//				// Get the hash of this group
		//				int thisHash = sdcgm.getHashFromStringList(thisMap.get(key).getAtomInfo());
		//				if(countMap.containsKey(thisHash)==false){
		//					totMap.put(thisHash, thisMap.get(key));
		//					countMap.put(thisHash, 1);
		//				}
		//				else{
		//					int oldCount =  countMap.get(thisHash);
		//					oldCount+=1;
		//					countMap.put(thisHash,oldCount);
		//				}
		//			}
		//
		//		}


		// Now get these hashmap as a messagepack
		com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper(new MessagePackFactory());;
		BufferedInputStream in = new BufferedInputStream(new FileInputStream("OUT.DATA"));
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		BufferedOutputStream out = new BufferedOutputStream(bs);
		byte[] ioBuf = new byte[2048];       
		int bytesRead;
		while ((bytesRead = in.read(ioBuf)) != -1){
			out.write(ioBuf, 0, bytesRead);
		}
		out.close();
		in.close();

		PDBResultMap pdbResMap = objectMapper.readValue(bs.toByteArray(), PDBResultMap.class);
		Map<Integer, Integer> countMap = pdbResMap.getCountMap();
		Map<Integer, PDBGroup> totMap = pdbResMap.getTotMap();

		Map<String, Integer> hashCount = new HashMap<String, Integer>();
		// Now iterate through the totMap to find the atomMap
		for(Integer key: totMap.keySet()){
			// Get the value
			PDBGroup thisGroup = totMap.get(key);
			for(int i=0; i<thisGroup.getAtomCharges().size(); i++){
				String charge = Integer.toString(thisGroup.getAtomCharges().get(i));
				String ele = thisGroup.getAtomInfo().get(i*2);
				String name = thisGroup.getAtomInfo().get(i*2+1);
				// Get the hash
				String totString = charge+","+ele+","+name;
				if(hashCount.containsKey(totString)){
					hashCount.put(totString, hashCount.get(totString)+1);
				}
				else{
					hashCount.put(totString, 1);
				}
			}

		}
		System.out.print(hashCount);

	}

	//		// Now set the map to write out
	//		Map<Integer, PDBGroup> outMap = new HashMap<Integer, PDBGroup>();
	//
	//		// Find the 64 most populated ones - and save them as a message pack
	//		List<Integer> countVals = new ArrayList(countMap.values());
	//		Collections.sort(countVals);
	//		Collections.reverse(countVals);
	//		int outKey = 0;
	//		int minVal = countVals.get(64);
	//		for(Integer key: totMap.keySet()){
	//			if(countMap.get(key)<minVal){
	//				continue;
	//			}
	//			outMap.put(outKey, totMap.get(key));
	//			outKey++;
	//		}
	//		// Now write this out as a messagePack		
	//		File file = new File("OUT.RES");
	//		FileOutputStream f = new FileOutputStream(file);
	//		ObjectOutputStream s = new ObjectOutputStream(f);
	//		s.writeObject(outMap);
	//		s.close();

	//		CompressmmCIF cm = new CompressmmCIF();
	//		outPdbResMap.setTotMap(outMap);
	//		outPdbResMap.setCountMap(countMap);
	//		byte[] outMp = cm.getMessagePack(outPdbResMap);
	//		// Write this data 
	//		FileOutputStream fos = new FileOutputStream(new File("OUT.DATA.FILTER"));
	//		fos.write(outMp, 0, outMp.length);
	//		fos.flush();
	//		fos.close();	


	public int getHashFromStringList(List<String> strings){
		int prime = 31;
		int result = 1;
		for( String s : strings )
		{
			result = result * prime + s.hashCode();
		}
		return result;
	}


	public static <K, V extends Comparable<? super V>> Map<K, V> 
	sortByValue( Map<K, V> map )
	{
		Map<K,V> result = new HashMap<>();
		Stream <Entry<K,V>> st = map.entrySet().stream();
		st.sorted(Comparator.comparing(e -> e.getValue()))
		.forEachOrdered(e ->result.put(e.getKey(),e.getValue()));

		return result;
	}
}
