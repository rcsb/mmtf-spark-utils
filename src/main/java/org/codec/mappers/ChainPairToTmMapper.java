package org.codec.mappers;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.List;

import javax.vecmath.Point3d;

import org.apache.spark.broadcast.Broadcast;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.StructureTools;
import org.biojava.nbio.structure.align.StructureAlignment;
import org.biojava.nbio.structure.align.StructureAlignmentFactory;
import org.biojava.nbio.structure.align.ce.CeMain;
import org.biojava.nbio.structure.align.ce.CeParameters;
import org.biojava.nbio.structure.align.model.AFPChain;


/**
 * This class maps a pair of chains, specified by two indices into the broadcasted data
 * to a vector of alignment scores
 * 
 * @author  Peter Rose
 */
public class ChainPairToTmMapper implements PairFunction<Tuple2<Integer,Integer>,String,Double> {
	private static final long serialVersionUID = 1L;
	private Broadcast<List<Tuple2<String, Chain>>> data = null;


	public ChainPairToTmMapper(Broadcast<List<Tuple2<String, Chain>>> chainsBc) {
		this.data = chainsBc;
	}

	/**
	 * Returns a chainId pair with the TM scores
	 */
	public Tuple2<String, Double> call(Tuple2<Integer, Integer> tuple) throws Exception {
		
        StructureAlignment algorithm  = StructureAlignmentFactory.getAlgorithm(CeMain.algorithmName);

		
		Tuple2<String, Chain> t1 = this.data.getValue().get(tuple._1);
		Tuple2<String, Chain> t2 = this.data.getValue().get(tuple._2);
		
		StringBuilder key = new StringBuilder();
		key.append(t1._1);
		key.append(",");
		key.append(t2._1);
		
		Chain chainOne = t1._2;
		Chain chainTwo = t2._2;
		Atom[] ca1 = StructureTools.getAtomCAArray(chainOne);
		Atom[] ca2 = StructureTools.getAtomCAArray(chainTwo);
		
        // get default parameters
        CeParameters params = new CeParameters();

        // add more print
        params.setShowAFPRanges(true);

        // set the maximum gap size to unlimited 
        params.setMaxGapSize(-1);

        // The results are stored in an AFPChain object           
        AFPChain afpChain = algorithm.align(ca1,ca2,params);            
        // Set the name
        afpChain.setName1(t1._1);
        afpChain.setName2(t2._1);
        
        
		
		return new Tuple2<String, Double>(key.toString(), afpChain.getAlignScore());
    }
}
