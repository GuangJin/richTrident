package com.jin.storm.richTrident.state.map;

import java.util.List;

import storm.trident.state.State;

public interface ReadOnlyRichMapState<T> extends State {
	List<T> multiGet(List<Object> ids);//in the original Trident, keys are represented by Tuple which is a Object list
}
