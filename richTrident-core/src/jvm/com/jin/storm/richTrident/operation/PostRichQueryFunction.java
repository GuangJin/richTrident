package com.jin.storm.richTrident.operation;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public interface PostRichQueryFunction extends Serializable{
	public List<TridentTuple> refine(Object value, HashSet<TridentTuple> coarseTupleSet);
	public void execute(Object value,  List<Object> result, TridentCollector collector);
	public void cleanup();
}
