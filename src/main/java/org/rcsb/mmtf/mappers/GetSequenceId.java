package org.rcsb.mmtf.mappers;

import java.util.List;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.biojava.nbio.alignment.Alignments;
import org.biojava.nbio.alignment.SimpleGapPenalty;
import org.biojava.nbio.alignment.Alignments.PairwiseSequenceAlignerType;
import org.biojava.nbio.alignment.template.GapPenalty;
import org.biojava.nbio.alignment.template.PairwiseSequenceAligner;
import org.biojava.nbio.core.alignment.matrices.SubstitutionMatrixHelper;
import org.biojava.nbio.core.alignment.template.SequencePair;
import org.biojava.nbio.core.alignment.template.SubstitutionMatrix;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.core.sequence.compound.AminoAcidCompound;
import org.rcsb.mmtf.dataholders.CalphaAlignBean;

import scala.Tuple2;

public class GetSequenceId implements PairFunction<Tuple2<Integer,Integer>,String,Float> {

	private SubstitutionMatrix<AminoAcidCompound> matrix;
	private GapPenalty penalty;
	private int gop;
	private int extend;
	private List<Tuple2<String, CalphaAlignBean>> data;

	public GetSequenceId(Broadcast<List<Tuple2<String,CalphaAlignBean>>> inputData) {
		data = inputData.getValue();
		// Now set these varaiable
		matrix = SubstitutionMatrixHelper.getBlosum65();
		penalty = new SimpleGapPenalty();
		gop = 8;
		extend = 1;
		penalty.setOpenPenalty(gop);
		penalty.setExtensionPenalty(extend);
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 9110842267835194607L;

	@Override
	public Tuple2<String, Float> call(Tuple2<Integer, Integer> inputTuple) throws Exception {
		ProteinSequence ps1 = new ProteinSequence(data.get(inputTuple._1)._2.getSequence());
		ProteinSequence ps2 = new ProteinSequence(data.get(inputTuple._2)._2.getSequence());

		PairwiseSequenceAligner<ProteinSequence, AminoAcidCompound> smithWaterman = Alignments.getPairwiseAligner(ps1, ps2, PairwiseSequenceAlignerType.LOCAL, penalty, matrix);
		SequencePair<ProteinSequence, AminoAcidCompound> pair = null;
		try {
			pair = smithWaterman.getPair();
		} catch (Exception e) {
			System.out.println(data.get(inputTuple._1)._1);
			System.out.println(data.get(inputTuple._2)._1);
			System.out.println("ALIGNMENT FAILED");
			return new Tuple2<String, Float>(data.get(inputTuple._1)._1+"__"+data.get(inputTuple._2)._1, (float) -1.0);
		}
		// Get the length - as the longest portion
		int length = ps1.getLength();
		if (ps2.getLength() > length) {
			length = ps2.getLength();
		}
		System.out.println(ps1.toString()+" vs "+ps2.toString()+" -> "+pair.getNumSimilars()+" and "+pair.getNumIdenticals());
		
		return new Tuple2<String, Float>(data.get(inputTuple._1)._1+"__"+data.get(inputTuple._2)._1, (float) (pair.getNumIdenticals()/(double)length));
	}



}
