package com.jin.storm.richTrident.operation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.jin.storm.richTrident.state.map.MemoryRichMapState;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

public class RangePreRichQueryFunction extends BaseQueryFunction<MemoryRichMapState, Object>{
	Fields _lower, _upper;
	boolean _lowerInclusive, _upperInclusive;
	
	public RangePreRichQueryFunction(Fields lower, boolean lowerInclusive, Fields upper, boolean upperInclusive){
		if (lower==null||lower.size()!=1||upper==null||upper.size()==0)
			throw new IllegalArgumentException("Incorret range fields received for RangePreRichQueryFunction.");
		this._lower = lower;
		this._upper = upper;
		this._lowerInclusive = lowerInclusive;
		this._upperInclusive = upperInclusive;
	}

	@Override
	public List batchRetrieve(MemoryRichMapState memoryRichMapState, List<TridentTuple> tuples) {
		TreeMap<Object, HashSet<TridentTuple>> treeMap = (TreeMap<Object, HashSet<TridentTuple>>) memoryRichMapState.getRichMap();
		ArrayList<HashSet<TridentTuple>> result = new ArrayList<HashSet<TridentTuple>>(tuples.size());
		for( TridentTuple t: tuples) {
			Object lowerBound = t.getValueByField(_lower.toList().get(0));
			Object upperBound = t.getValueByField(_upper.toList().get(0));
			NavigableMap<Object,HashSet<TridentTuple>> submap = treeMap.subMap(lowerBound,_lowerInclusive, upperBound, _upperInclusive);
			if(submap==null||submap.isEmpty())
				result.add(null);
			else {
				HashSet<TridentTuple> tmpResult = new HashSet<TridentTuple>();
				for(HashSet<TridentTuple> tupleSet: submap.values())
					for(TridentTuple tuple: tupleSet)
						tmpResult.add(tuple); 
				result.add(tmpResult);
			}
		}
		return result;
	}

	@Override
	public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
		collector.emit(new Values(result));
	}
}
