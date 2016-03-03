package org.rcsb.mmtf.sparkexamples;



import java.io.Serializable;
import java.util.List;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
/**
 * Demo Map-Reduce program that shows how to read a Hadoop Sequence file and
 * generate BioJava objects
 * @author  Peter Rose
 */
public class SparkErrorCatch implements Serializable {    
	/**
	 * 
	 */
	private static final long serialVersionUID = 3037567648753603114L;
	private static int NUM_THREADS = 24;
	private static int NUM_TASKS_PER_THREAD = 3; // Spark recommends 2-3 tasks per thread
	//	private static final Logger logger = LoggerFactory.getLogger(SparkRead.class);
	public static void main(String[] args )
	{
		// The path of the hadoop file
		String path = "/home/anthony/src/codec-devel/Total.hadoop.nocodec";
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[" + NUM_THREADS + "]")
				.setAppName(SparkRead.class.getSimpleName());
		conf.set("spark.driver.maxResultSize", "14g");
		// Set the config
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Read in with spark
		JavaPairRDD<String, byte[]> jprdd = sc
				.sequenceFile(path, Text.class, BytesWritable.class, NUM_THREADS * NUM_TASKS_PER_THREAD)
				.mapToPair(new PairFunction<Tuple2<Text,BytesWritable>, String, byte[]>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 3030996192287887259L;

					@Override
					public Tuple2<String, byte[]> call(Tuple2<Text, BytesWritable> t) throws Exception {
						// Get the tuple
						byte[] outArr = t._2.copyBytes();
						return  new Tuple2<String,byte[]>(t._1.toString(), outArr);
					}
					
				})
				.filter(new Function<Tuple2<String,byte[]>, Boolean>() {
					
					/**
					 * 
					 */
					private static final long serialVersionUID = 8287947302277751084L;

					@Override
					public Boolean call(Tuple2<String, byte[]> v1) throws Exception {
						if(v1._2.length==0){
						return true;
						}
						else{
							return false;
						}
					}
				});
		List<String> errors = jprdd.keys().collect();
		System.out.println(errors);
		sc.close();
	}
}


