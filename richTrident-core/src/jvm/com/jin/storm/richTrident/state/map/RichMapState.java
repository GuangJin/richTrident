package com.jin.storm.richTrident.state.map;

import java.util.List;

import storm.trident.tuple.TridentTuple;

public interface RichMapState extends ReadOnlyRichMapState<TridentTuple> {
    void multiUpdate(List<Object> keys, List<TridentTuple> vals);//this is not like the state used for the aggregation
}
