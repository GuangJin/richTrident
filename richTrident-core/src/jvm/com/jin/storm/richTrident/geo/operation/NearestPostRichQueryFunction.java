package com.jin.storm.richTrident.geo.operation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.jin.storm.richTrident.geo.GeoData;
import com.jin.storm.richTrident.operation.PostRichQueryFunction;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class NearestPostRichQueryFunction implements PostRichQueryFunction{
	Fields _valueField;
	
	public NearestPostRichQueryFunction(Fields valueField){
		_valueField = valueField;
	}

	@Override
	public List<TridentTuple> refine(Object value, HashSet<TridentTuple> coarseTupleSet) {
		//System.out.println("NearestPostRichQueryFunction@"+Integer.toHexString(System.identityHashCode(this))+" receives a value:"+value+" tuples:"+coarseTupleSet);
		TridentTuple nearestTuple = null;
		float minDistance = Float.MAX_VALUE;
		for(TridentTuple t: coarseTupleSet){
			float distance = ((GeoData) t.getValueByField(_valueField.toList().get(0))).distanceTo((GeoData) value);
			if(minDistance>distance) {
				nearestTuple = t;
				minDistance = distance;
			}
		}
		ArrayList<TridentTuple> result = new ArrayList<TridentTuple>(1);
		result.add(nearestTuple);
		return result;
	}

	@Override
	public void execute(Object value, List<Object> result, TridentCollector collector) {
		if(result!=null)
			collector.emit(new Values(result));
	}

	@Override
	public void cleanup() {
		//Nothing to cleanup
	}

}
