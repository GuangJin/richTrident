/** 
 * @file RichStream.java
 * @class com.jin.storm.richTrident.RichStream
 * @brief RichStream a subclass of Stream to support more query features.
 * @note New features are implemented through distributePersist() and stateRichQuery().
 * @author Guang Jin
 * @email jin_guang@hotmail.com
 **/

package com.jin.storm.richTrident;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.reflect.FieldUtils;

import com.jin.storm.richTrident.operation.PostRichQueryFunction;
import com.jin.storm.richTrident.processor.DistributeProcessor;
import com.jin.storm.richTrident.processor.PostStateQueryProcessor;
import com.jin.storm.richTrident.state.map.IDBasedMemStateUpdater;
import com.jin.storm.richTrident.state.map.MemoryRichMapState;

import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.operation.Function;
import storm.trident.planner.Node;
import storm.trident.planner.ProcessorNode;
import storm.trident.planner.processor.ProjectedProcessor;
import storm.trident.planner.processor.StateQueryProcessor;
import storm.trident.state.QueryFunction;
import storm.trident.tuple.TridentTuple;
import storm.trident.util.TridentUtils;

public class RichStream extends Stream {
	
	RichTridentTopology _topology;
	Node _node;
	String _name;
	
	/**
	 * @brief RichStream's constructor. 
	 * @param	topology	The richTridentTopology.
	 * @param	name		The name of this richStream.
	 * @param	node		The node working on this richStream.
	 **/
	public RichStream(RichTridentTopology topology, String name, Node node) {
		super(topology, name, node);
		_topology = topology;
		_node = node;
		_name = name;
	}
	
	/**
	 * @brief wrapper of Stream's broadcast().
	 * @return	The broadcasted RichStream to be received.
	 **/
	public RichStream broadcast() {
        return (RichStream) super.broadcast();
    }
	
	/**
	 * @brief verify the input fields; throw IllegalArgumentException if input fields are not valid.
	 * @param fields The fields to be verified.
	 **/
    private void fieldsValidation(Fields fields) {
    	Fields allFields = this.getOutputFields();
        if (fields == null)
        	 throw new IllegalArgumentException("Trying to distribute a null field: from richStream containing fields fields: <" + allFields + ">");
        
        for (String field : fields) {
            if (!allFields.contains(field)) {
                throw new IllegalArgumentException("Trying to select non-existent field: '" + field + "' from richStream containing fields fields: <" + allFields + ">");
            }
        }
    }
    
	/**
	 * @brief wrapper of Stream's each().
	 * @note generate New stream based on operating each tuple.
	 * @param	inputFields		The input fields.
	 * @param	function		An operation function works on inputFields and generates functionFields.
	 * @param	functionFields	The output fields from function.
	 * @return	A RichStream representing each tuple.
	 **/
    public RichStream each(Fields inputFields, Function function, Fields functionFields) {
    	return (RichStream) super.each(inputFields, function, functionFields);
    }
    
	/**
	 * @brief generate new RichStream to keep certain fields from original RichStreams.
	 * @param	keepFields		The fields that output richStream will keep.
	 * @return	RichStream		The output richStream.
	 **/
    public RichStream project(Fields keepFields) {
    	fieldsValidation(keepFields);
        return _topology.addSourcedNode(this, new ProcessorNode(_topology.getUniqueStreamId(), _name, keepFields, new Fields(), new ProjectedProcessor(keepFields)));
    }
	
	/**
	 * @brief distribute tuples based on their identities.
	 * @param	identityFields	The identity field, based on which the process will be distributed. 
	 * @param	indexField		The field to be indexed in the background map.
	 * @param	indexMap		The structure maps index values with the TridentTuples.
	 * @return	A TridentState where the state query can be used on.
	 **/
	public TridentState distributePersist(Fields identityFields, Fields indexField, Map<Object, HashSet<TridentTuple>> indexMap){
        fieldsValidation(identityFields);
        fieldsValidation(indexField);
       	if (indexField.size() != 1)
    		throw new IllegalArgumentException("RichStream can only index tuples based on a single field. Got this instead: "+ indexField.toString());
        
        Stream stream = this.broadcast();
        stream = _topology.addSourcedNode(stream,
                new ProcessorNode(
                	_topology.getUniqueStreamId(),
                    _name,
                    _node.allOutputFields,
                    _node.allOutputFields,
                    new DistributeProcessor(identityFields))
                    );//this is my distributor
        return stream.partitionPersist(new MemoryRichMapState.Factory(identityFields, indexField, indexMap), this.getOutputFields(), new IDBasedMemStateUpdater(identityFields, indexField, _node.allOutputFields),_node.allOutputFields);
	}
	
	/**
	 * @brief post rich features queries against a state generated from RichStreams. 
	 * @param state				The tridentState generated from RichStream's distribute(). 
	 * @param inputFields		The input fields from a RichStream.
	 * @param preFunction		The QueryFunction executed by distributed states to query on local index map.
	 * @param postQueryFunction	The PostRichQueryFunction to further refine the distributed query results.
	 * @param functionFields	The output fields to keep the final query results.
	 * @return A Stream representing the query results.
	 **/
	public Stream stateRichQuery(TridentState state, Fields inputFields, QueryFunction preFunction, PostRichQueryFunction postQueryFunction, Fields functionFields) {
		fieldsValidation(inputFields);
		RichStream broadcastedStream = this.broadcast();
        Node stateNode = null;
        Map<String, List<Node>> colocateMap = null;
		try {
			stateNode = (Node) FieldUtils.readField(state, "_node", true);//Reflection is used to get the unmodified field, which might cause issues if the securityManager is on
			colocateMap = (Map<String, List<Node>>) FieldUtils.readField(_topology, "_colocate", true);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		} 
        String stateId = stateNode.stateInfo.id;
        Node n = new ProcessorNode(_topology.getUniqueStreamId(),
                _name,
                TridentUtils.fieldsConcat(getOutputFields(), functionFields),
                functionFields,
                new StateQueryProcessor(stateId, inputFields, preFunction));
        colocateMap.get(stateId).add(n);
        RichStream stream = _topology.addSourcedNode(broadcastedStream, n);
        stream = (RichStream) stream.global();
        return _topology.addSourcedNode(stream,
				new ProcessorNode(_topology.getUniqueStreamId(), _name,
						TridentUtils.fieldsConcat(getOutputFields(), functionFields), functionFields,
						new PostStateQueryProcessor(getOutputFields(), inputFields, functionFields, postQueryFunction)));
	}   
}
