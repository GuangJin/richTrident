package com.jin.storm.richTrident.geo.operation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import backtype.storm.tuple.Values;

import com.jin.storm.richTrident.geo.GeoData;
import com.jin.storm.richTrident.geo.RTreeMap;
import com.jin.storm.richTrident.state.map.MemoryRichMapState;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

public class NearestPreRichQueryFunction extends BaseQueryFunction<MemoryRichMapState, Object>{

	@Override
	public List batchRetrieve(MemoryRichMapState memoryRichMapState, List<TridentTuple> tuples) {
		RTreeMap<HashSet<TridentTuple>> rtreeMap = (RTreeMap<HashSet<TridentTuple>>) memoryRichMapState.getRichMap();
		ArrayList<HashSet<TridentTuple>> result = new ArrayList<HashSet<TridentTuple>>(tuples.size());
		for( TridentTuple t: tuples) {
			HashSet<TridentTuple> nearest = rtreeMap.nearest((GeoData)t.get(0));
			if(nearest!=null&&!nearest.isEmpty())
				result.add((HashSet<TridentTuple>) nearest.clone());
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
