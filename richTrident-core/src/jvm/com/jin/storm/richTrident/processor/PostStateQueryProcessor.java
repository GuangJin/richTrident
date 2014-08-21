package com.jin.storm.richTrident.processor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.jin.storm.richTrident.operation.PostRichQueryFunction;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.planner.ProcessorContext;
import storm.trident.planner.TridentProcessor;
import storm.trident.planner.processor.TridentContext;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTuple.Factory;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;

public class PostStateQueryProcessor implements TridentProcessor{
	private Fields _inputFields, _valueField, _functionField;
	private ReplaceCollector _replaceCollector;
	private TridentContext _tridentContext;
	private ProjectionFactory _inputFieldProjectionFactory, _valueFieldProjectionFactory, _functionFieldProjectionFactory;
	private PostRichQueryFunction _function;
	
	public PostStateQueryProcessor(Fields inputField, Fields valueField, Fields functionField, PostRichQueryFunction function){
		if (inputField==null||inputField.size() < 1) 
			throw new IllegalArgumentException(
					"PostStateQueryProcessor must at least have a single input field.");
		if (valueField.size() < 1) 
			throw new IllegalArgumentException(
					"PostStateQueryProcessor must at least have a single value field.");
		if (functionField.size() != 1) 
			throw new IllegalArgumentException(
					"PostStateQueryProcessor can only generate a single function field. Got this instead: " + functionField.toString());
		_inputFields = inputField;
		_valueField = valueField;
		_functionField = functionField;
		_function = function;
	}

	@Override
	public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
		//System.out.println("For stream@"+streamId+" PostStateQueryProcessor@"+Integer.toHexString(System.identityHashCode(this))+" receives a tuple:"+tuple);
		TridentTuple inputFields = _inputFieldProjectionFactory.create(tuple);
		HashSet<TridentTuple> tmpTupleSet = (HashSet<TridentTuple>) _functionFieldProjectionFactory.create(tuple).get(0);
		HashMap<TridentTuple, HashSet<TridentTuple>> map = (HashMap<TridentTuple, HashSet<TridentTuple>>) processorContext.state[_tridentContext.getStateIndex()];
		if (tmpTupleSet!=null)
			if(map.containsKey(inputFields)){
				HashSet<TridentTuple> cachedSet = map.get(inputFields);
				cachedSet.addAll(tmpTupleSet);
			} else
				map.put(inputFields, tmpTupleSet);
	}

	@Override
	public void cleanup() {
		_function.cleanup();
	}

	@Override
	public void finishBatch(ProcessorContext processorContext) {
		//System.out.println("PostStateQueryProcessor@"+Integer.toHexString(System.identityHashCode(this))+" finishes a batch");
		HashMap<TridentTuple, HashSet<TridentTuple>> map = (HashMap<TridentTuple, HashSet<TridentTuple>>) processorContext.state[_tridentContext.getStateIndex()];
		Set<TridentTuple> cachedTuples = map.keySet();
		for (TridentTuple tuple: cachedTuples){
			List tmpResult = _function.refine(_valueFieldProjectionFactory.create(tuple).get(0), map.get(tuple));
			_replaceCollector.setContext(processorContext, tuple);
			_function.execute(tuple, tmpResult, _replaceCollector);
		}
			
		
	}

	@Override
	public Factory getOutputFactory() {
		return _replaceCollector.getOutputFactory();
	}

	@Override
	public void prepare(Map conf, TopologyContext topologyContext, TridentContext tridentContext) {
		List<Factory> parents = tridentContext.getParentTupleFactories();
        if(parents.size()!=1) {
            throw new RuntimeException("PostStateQueryProcessor can only have one parent");
        }
		_tridentContext = tridentContext;
		_replaceCollector = new ReplaceCollector(tridentContext);
		_inputFieldProjectionFactory = new ProjectionFactory(parents.get(0), _inputFields);
		_valueFieldProjectionFactory = new ProjectionFactory(parents.get(0), _valueField);
		_functionFieldProjectionFactory = new ProjectionFactory(parents.get(0), _functionField);
	}

	@Override
	public void startBatch(ProcessorContext processorContext) {
		//System.out.println("PostStateQueryProcessor@"+Integer.toHexString(System.identityHashCode(this))+" starts a batch");
		processorContext.state[_tridentContext.getStateIndex()] = new HashMap<TridentTuple, HashSet<TridentTuple>>();//map a tuple with its functionField with coarse results
	}

}
