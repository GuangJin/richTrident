package com.jin.storm.richTrident.state.map;

import java.util.List;
import java.util.Map;

import storm.trident.tuple.TridentTuple;

public interface IRichBackingMap {
    List<TridentTuple> multiGet(List<Object> keys);//in original Trident, the keys are represented by Tuples which are lists
    void multiPut(List<Object> keys, List<TridentTuple> tuples); //here, I simplify the keys as a list of Objects
    Map getRichMap();
}
