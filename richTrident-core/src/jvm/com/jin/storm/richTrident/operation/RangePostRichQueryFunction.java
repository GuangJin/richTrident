package com.jin.storm.richTrident.operation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class RangePostRichQueryFunction implements PostRichQueryFunction{
	Fields _valueField;
	
	public RangePostRichQueryFunction(Fields valueField){
		_valueField = valueField;
	}

	@Override
	public List<TridentTuple> refine(Object value, HashSet<TridentTuple> coarseTupleSet) {
		//System.out.println("RangePostRichQueryFunction@"+Integer.toHexString(System.identityHashCode(this))+" receives a value:"+value+" tuples:"+coarseTupleSet);
		ArrayList<TridentTuple> result = new ArrayList<TridentTuple>(coarseTupleSet.size());
		result.addAll(coarseTupleSet);
		return result;
	}

	@Override
	public void execute(Object value, List<Object> result, TridentCollector collector) {
		if(result!=null)
			collector.emit(new Values(result));
	}

	@Override
	public void cleanup() {
		// Nothing to do
	}

}
