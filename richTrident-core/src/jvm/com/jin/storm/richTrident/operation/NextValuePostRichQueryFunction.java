package com.jin.storm.richTrident.operation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class NextValuePostRichQueryFunction implements PostRichQueryFunction{
	Fields _valueField;
	
	public NextValuePostRichQueryFunction(Fields valueField){
		_valueField = valueField;
	}

	@Override
	public List<TridentTuple> refine(Object value, HashSet<TridentTuple> coarseTupleSet) {
		TreeMap<Object, TridentTuple> sortedMap = new TreeMap<Object, TridentTuple>();
		for(TridentTuple t: coarseTupleSet)
			sortedMap.put(t.getValueByField(_valueField.toList().get(0)), t);
		ArrayList<TridentTuple> result = new ArrayList<TridentTuple>(1);
		result.add(sortedMap.firstEntry().getValue());
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
