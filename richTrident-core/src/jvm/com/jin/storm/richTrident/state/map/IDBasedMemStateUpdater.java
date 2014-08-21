/** 
 * @file IDBasedMemStateUpdater.java
 * @class com.jin.storm.richTrident.IDBasedMemStateUpdater
 * @brief This class is used to convert a stateless stream into a TridentState. In its essential, this class maintains a
 * snapshot latest tuples based on ID field specified.
 * @author Guang Jin
 * @email jin_guang@hotmail.com
 * 
 **/

package com.jin.storm.richTrident.state.map;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Fields;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;

public class IDBasedMemStateUpdater implements StateUpdater<RichMapState>{
	Fields _idField;
	Fields _indexField;
	Fields _outputFields;
    ProjectionFactory _outputFactory;
    ProjectionFactory _idFactory;
    ProjectionFactory _indexFactory;
	
	public IDBasedMemStateUpdater(Fields idField, Fields indexField, Fields outputFields){
		this._idField = idField;
		this._outputFields = outputFields;
		this._indexField = indexField;
		if (_idField.size() != 1)
			throw new IllegalArgumentException("IDBasedMemStateUpdater only take a single field as the idField. Got this instead: "+ _idField.toString());
	}
	
	public Object clone() throws CloneNotSupportedException {
	    throw new CloneNotSupportedException(); 
	}
	
	@Override
	public void cleanup() {
		// nothing to be cleaned up
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
        _outputFactory = context.makeProjectionFactory(_outputFields);
        _idFactory = context.makeProjectionFactory(_idField);
        _indexFactory = context.makeProjectionFactory(_indexField);
	}

	/*!
	 * @brief update the background mapstate
	 *
	 * updateState(MapState, List<TridentTuple>, TridentCollector) is used to update the background MapState
	 *
	 * @param	map			the background mapSate, which can be treated as a distributed key:Value hashmap (or DB) 
	 * @param	tuples		the input tuples bonded by specified stream
	 * @param	collector	where the generated output tuples go to
	 * @return void
	 */
	@Override
	public void updateState(RichMapState map, List<TridentTuple> tuples, TridentCollector collector) {
        List<Object> ids = new ArrayList<Object>(tuples.size());
        List<TridentTuple> outputTuples = new ArrayList<TridentTuple>(tuples.size());
        
        for(TridentTuple t: tuples) {
        	//System.out.println("IDBasedMemStateUpdater@"+Integer.toHexString(System.identityHashCode(this))+" receives tuple:"+t);
        	ids.add(_idFactory.create(t).get(0));//get the identity object
        	outputTuples.add(_outputFactory.create(t));
        }
        map.multiUpdate(ids, outputTuples);
	}

}
