package org.codec.proccessors;

import java.util.List;


import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.symmetry.core.QuatSymmetryDetector;
import org.biojava.nbio.structure.symmetry.core.QuatSymmetryParameters;
import org.biojava.nbio.structure.symmetry.core.QuatSymmetryResults;
import org.biojava.nbio.structure.symmetry.core.Subunits;

import scala.Tuple2;

public class PredictQuatStructures implements PairFunction<Tuple2<String, Structure>,String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7628464926191835446L;

	public String getResults(Structure structure){
		// Get the paramrters
		QuatSymmetryParameters parameters = new QuatSymmetryParameters();
		String outString = "";
		// Now with the Bioassebly - lets get the Quaterny symmetry detection
		try {
			QuatSymmetryDetector detector = new QuatSymmetryDetector(structure, parameters);

			if (detector.hasProteinSubunits()) {	

				// save global symmetry results
				List<QuatSymmetryResults> globalResults = detector.getGlobalSymmetry();
				writeString(outString, globalResults);

				// save local symmetry results
				for (List<QuatSymmetryResults> localResults: detector.getLocalSymmetries()) {
					writeString(outString, localResults);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return outString;
	}

	private void writeString(String outString, List<QuatSymmetryResults> resultsList) {
		// Now write these out
		for (QuatSymmetryResults results: resultsList) {

			int order = 1;
			if (!results.getSymmetry().equals("H")) {
				order = results.getRotationGroup().getOrder();
			}

			outString+=results.isLocal() +
					"," + results.getSubunits().isPseudoStoichiometric() +
					"," + results.getSubunits().getStoichiometry() +
					"," + results.getSubunits().isPseudoSymmetric() +
					"," + results.getSymmetry() +
					"," + order + 
					"," + isLowSymmetry(results) +
					"," + Math.round(results.getSubunits().getMinSequenceIdentity()*100.0) +
					"," + Math.round(results.getSubunits().getMaxSequenceIdentity()*100.0) +
					"," + (float) results.getScores().getRmsdCenters() +
					"," + (float) results.getScores().getRmsd() +
					"," + (float) results.getScores().getTm() +
					"," + (float) results.getScores().getMinRmsd() +
					"," + (float) results.getScores().getMaxRmsd() +
					"," + (float) results.getScores().getMinTm() +
					"," + (float) results.getScores().getMaxTm() +
					"," + (float) results.getScores().getRmsdIntra() +
					"," + (float) results.getScores().getTmIntra() +
					"," + (float) results.getScores().getSymDeviation() +
					"," + results.getSubunits().getSubunitCount() +
					"," + results.getNucleicAcidChainCount() +
					"," + results.getSubunits().getCalphaCount() +
					"\n";

		}
	}



	private boolean isLowSymmetry(QuatSymmetryResults results) {
		return getMinFold(results.getSubunits()) > 1 && results.getRotationGroup() != null && results.getRotationGroup().getPointGroup().equals("C1");
	}

	private int getMinFold(Subunits subunits) {
		if (subunits.getFolds().size() > 1) {
			return subunits.getFolds().get(1);
		}
		return subunits.getFolds().get(0);
	}

	@Override
	public Tuple2<String, String> call(Tuple2<String, Structure> t) throws Exception {
		// Get the subunits
		Tuple2<String, String>  outAns = new Tuple2<String, String>(t._1, getResults(t._2));
		return outAns;
	}
}
