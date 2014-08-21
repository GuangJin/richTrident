/** 
 * @file DistributeProcessor.java
 * @class com.jin.storm.richTrident.processor.DistributeProcessor
 * @brief This is a TridentProcessor. Most relevant classes are AggregateProcessor and GroupedAggregator.
 * @note The processor works on batch; startBatch() creates a hashmap; execute() save the tuple to map based on its identity value; finishBatch() then outputs the batch to the right receiver.
 * @author Guang Jin
 * @email jin_guang@hotmail.com
 * 
 **/
package com.jin.storm.richTrident.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.operation.TridentOperationContext;
import storm.trident.partition.IndexHashGrouping;
import storm.trident.planner.ProcessorContext;
import storm.trident.planner.TridentProcessor;
import storm.trident.planner.processor.FreshCollector;
import storm.trident.planner.processor.TridentContext;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTuple.Factory;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;

public class DistributeProcessor implements TridentProcessor {
	
	private TridentContext _tridentContext;
	private FreshCollector _freshCollector;
	private Fields _identityField;
	private ProjectionFactory _identityProjectionFactory;
	private TridentOperationContext _tridentOperationContext;
	private int _totalPartitions, _myPartitionIndex;
	
	public DistributeProcessor(Fields identityField){
		if (identityField.size() != 1) 
			throw new IllegalArgumentException(
					"DistributeProcessor can only take a single identity field. Got this instead: " + identityField.toString());
		_identityField = identityField;
	}

	@Override
	public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
		//System.out.println("For stream@"+streamId+" DistributeProcessor@"+Integer.toHexString(System.identityHashCode(this))+" receives a tuple:"+tuple);
		//I try not to complicate things up
		//See startBatch() to understand how this hashmap is created
		HashMap<Object, TridentTuple> hashMap = (HashMap<Object, TridentTuple>) processorContext.state[_tridentContext.getStateIndex()];
		TridentTuple identiyTuple = _identityProjectionFactory.create(tuple);
		Object identity = identiyTuple.get(0);//I shall assume each identity is unique and has unique hashcode
		if(_myPartitionIndex == IndexHashGrouping.objectToIndex(identity, _totalPartitions))//this is how to distribute the tuple
			hashMap.put(identity, tuple);//update the hashmap
	}

	@Override
	public void cleanup() {
		// There shall be nothing to cleanup
	}

	@Override
	public void finishBatch(ProcessorContext processorContext) {
		//System.out.println("DistributeProcessor@"+Integer.toHexString(System.identityHashCode(this))+" finishBatch()");
		HashMap<Object, TridentTuple> hashMap = (HashMap<Object, TridentTuple>) processorContext.state[_tridentContext.getStateIndex()];
		for(Entry<Object, TridentTuple> e: hashMap.entrySet()) {
			_freshCollector.emit(e.getValue());//emit the tuple based on its identity field to the appropriate receiver
        }
	}

	@Override
	public Factory getOutputFactory() {//generate the output
		return _freshCollector.getOutputFactory();
	}

	@Override
	public void prepare(Map conf, TopologyContext topologyContext, TridentContext tridentContext) {
        List<Factory> parents = tridentContext.getParentTupleFactories();
		//_topologyContext = topologyContext;
		_tridentContext = tridentContext;
		_freshCollector = new FreshCollector(tridentContext);
		_identityProjectionFactory = new ProjectionFactory(parents.get(0), _identityField);
		_tridentOperationContext = new TridentOperationContext(topologyContext, _identityProjectionFactory);
		_totalPartitions = _tridentOperationContext.numPartitions();
		_myPartitionIndex = _tridentOperationContext.getPartitionIndex();
		//System.out.println("DistributeProcessor@"+Integer.toHexString(System.identityHashCode(this))+" is preparing with "+_myPartitionIndex+"/"+_totalPartitions);
	}

	@Override
	public void startBatch(ProcessorContext processorContext) {
		//this indicates a start of batch
		//processContext is used as a global variable to store context data
		//System.out.println("DistributeProcessor@"+Integer.toHexString(System.identityHashCode(this))+" startBatch()");
		processorContext.state[_tridentContext.getStateIndex()] = new HashMap<Object, TridentTuple>();
		//I try not to complicate the scenario
		//the hashmap is created when a batch starts
		_freshCollector.setContext(processorContext);
	}

}
