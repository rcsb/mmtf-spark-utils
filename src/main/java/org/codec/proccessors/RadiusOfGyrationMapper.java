package org.codec.proccessors;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;

import scala.Tuple2;

public class RadiusOfGyrationMapper implements PairFunction<Tuple2<String,Structure>,String,Float> {
	private static final long serialVersionUID = 1L;

	public Float runFunc(Structure inStruct) throws Exception {
		double sum = 0;
		int n = 0;
		// Work out how big the array needs to be
		int size =0;
		for(int i=0;i<inStruct.nrModels();i++){
			for(Chain c: inStruct.getChains(i)){
				for(Group g: c.getSeqResGroups()){
					size+=g.getAtoms().size();
				}
			}
		}
		// Now intialise the results array
		Point3d[] inCoords = new Point3d[size];

		// Now fill the array
		size=0;
		for(int i=0;i<inStruct.nrModels();i++){
			for(Chain c: inStruct.getChains(i)){
				for(Group g: c.getSeqResGroups()){
					for(Atom a: g.getAtoms()){
						inCoords[size] = new Point3d(a.getCoords());
						size++;
					}
				}
			}
		}		
		// Now calculate the radius of gyration
	    for (int i = 0; i < size; i++) {
	    	if (inCoords[i] == null) {
	    		continue;
	    	} else {
	    		n++;
	    	}
	    	for (int j = i; j < size; j++) {
	    		if (inCoords[j] == null) continue;
	    		sum+= inCoords[i].distanceSquared(inCoords[j]);
	    	}
	    }

	    return (float)Math.sqrt(sum/n);
	}

	@Override
	public Tuple2<String, Float> call(Tuple2<String,Structure> t) throws Exception {
		// TODO Auto-generated method stub
		return new Tuple2<String, Float>(t._1.toString(),runFunc(t._2));
	}
}