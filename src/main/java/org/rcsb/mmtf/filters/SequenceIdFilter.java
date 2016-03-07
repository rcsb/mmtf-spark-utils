package org.rcsb.mmtf.filters;

import java.util.List;

import org.apache.spark.api.java.function.Function;
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

public class SequenceIdFilter implements Function<Tuple2<Integer, Integer>, Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3055519990407050360L;
	
	
	private SubstitutionMatrix<AminoAcidCompound> matrix;
	private GapPenalty penalty;
	private int gop;
	private int extend;
	private double minSeqId;
	private List<Tuple2<String, CalphaAlignBean>> data;

	public SequenceIdFilter(double minSeqIdInput, Broadcast<List<Tuple2<String,CalphaAlignBean>>> inputData) {
		// Now set these varaiable
		matrix = SubstitutionMatrixHelper.getBlosum65();
		penalty = new SimpleGapPenalty();
		minSeqId = minSeqIdInput;
	    data = inputData.getValue();
		gop = 8;
		extend = 1;
		penalty.setOpenPenalty(gop);
		penalty.setExtensionPenalty(extend);
	}

	@Override
	public Boolean call(Tuple2<Integer, Integer> v1) throws Exception {

		ProteinSequence ps1 = new ProteinSequence(data.get(v1._1)._2.getSequence());
		ProteinSequence ps2 = new ProteinSequence(data.get(v1._2)._2.getSequence());

		PairwiseSequenceAligner<ProteinSequence, AminoAcidCompound> smithWaterman = Alignments.getPairwiseAligner(ps1, ps2, PairwiseSequenceAlignerType.LOCAL, penalty, matrix);
		SequencePair<ProteinSequence, AminoAcidCompound> pair = null;
		try {
			pair = smithWaterman.getPair();
		} catch (Exception e) {
			System.out.println(data.get(v1._1)._1);
			System.out.println(data.get(v1._2)._1);
			System.out.println("ALIGNMENT FAILED");
			return false;
		}
		// Get the length - as the longest portion
		int length = ps1.getLength();
		if (ps2.getLength() > length) {
			length = ps2.getLength();
		}
		if (pair.getNumIdenticals()/(double)length >= minSeqId) {
			return true;
		}
		else{
			return false;
		}
		
	}

}
