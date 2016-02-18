package org.codec.sparkexamples;



import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.biojava.nbio.structure.Structure;
import org.codec.mappers.ByteArrayToBioJavaStructMapper;
import org.codec.mappers.ByteWriteToByteArr;

import scala.Tuple2;


/**
 * Demo Map-Reduce program that shows how to read a Hadoop Sequence file and
 * generate BioJava objects
 * @author  Peter Rose
 */
public class SparkRead implements Serializable {    
	/**
	 * 
	 */
	private static final long serialVersionUID = 3037567648753603114L;
	private static int NUM_THREADS = 4;
	private static int NUM_TASKS_PER_THREAD = 3; // Spark recommends 2-3 tasks per thread
	//	private static final Logger logger = LoggerFactory.getLogger(SparkRead.class);
	public static void main(String[] args )
	{
		String path = "Total.hadoop.latest.bzip2";
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[" + NUM_THREADS + "]")
				.setAppName(SparkRead.class.getSimpleName());
		// Set the config
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<String, Structure> jprdd = sc
				.sequenceFile(path, Text.class, BytesWritable.class, NUM_THREADS * NUM_TASKS_PER_THREAD)
				.mapToPair(new ByteWriteToByteArr())
				.filter(new Function<Tuple2<String,byte[]>, Boolean>() {
					
					/**
					 * 
					 */
					private static final long serialVersionUID = -4931703036945548441L;

					@Override
					public Boolean call(Tuple2<String, byte[]> v1) throws Exception {
						// TODO Auto-generated method stub
						if(v1._1.endsWith("_total")){

						return true;
						}
						else{
							return false;
						}
					}
				})
				// Now get the structure
				.mapToPair(new ByteArrayToBioJavaStructMapper());
		JavaRDD<String> values = jprdd.keys();
		List<String> outValues = values.collect();
		System.out.println(outValues.size());
		sc.close();
	}
}


