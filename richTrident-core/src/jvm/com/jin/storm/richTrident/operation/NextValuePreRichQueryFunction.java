package com.jin.storm.richTrident.operation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import backtype.storm.tuple.Values;

import com.jin.storm.richTrident.state.map.MemoryRichMapState;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

public class NextValuePreRichQueryFunction extends BaseQueryFunction<MemoryRichMapState, Object>{

	@Override
	public List batchRetrieve(MemoryRichMapState memoryRichMapState, List<TridentTuple> tuples) {
		TreeMap<Object, HashSet<TridentTuple>> treeMap = (TreeMap<Object, HashSet<TridentTuple>>) memoryRichMapState.getRichMap();
		ArrayList<HashSet<TridentTuple>> result = new ArrayList<HashSet<TridentTuple>>(tuples.size());
		for( TridentTuple t: tuples) {
			Entry<Object, HashSet<TridentTuple>> nextEntry = treeMap.higherEntry(t.get(0));
			if(nextEntry!=null)
				result.add((HashSet<TridentTuple>) nextEntry.getValue().clone()); 
			else
				result.add(null);
		}
		return result;
	}

	@Override
	public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
		collector.emit(new Values(result));
	}
}
