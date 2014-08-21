/** 
 * @file ReplaceCollector.java
 * @class com.jin.storm.richTrident.processor.ReplaceCollector
 * @brief This is a subclass of TridentCollector to support postRichQueryFunction
 * @note The functionField needs to be refined to generate the final query results.
 * @author Guang Jin
 * @email jin_guang@hotmail.com
 * 
 **/
package com.jin.storm.richTrident.processor;

import java.util.List;

import backtype.storm.tuple.Fields;
import storm.trident.operation.TridentCollector;
import storm.trident.planner.ProcessorContext;
import storm.trident.planner.TupleReceiver;
import storm.trident.planner.processor.TridentContext;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;
import storm.trident.tuple.TridentTuple.Factory;
import storm.trident.tuple.TridentTupleView.OperationOutputFactory;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;
import storm.trident.util.TridentUtils;

public class ReplaceCollector implements TridentCollector {
	TridentContext _triContext;
	OperationOutputFactory _factory;
    TridentTuple _tuple;
    ProcessorContext _context;
	
	
    public ReplaceCollector(TridentContext context) {
        _triContext = context;
        Factory parentFactory = context.getParentTupleFactories().get(0);
        ProjectionFactory projectionFactory = new ProjectionFactory(parentFactory, TridentUtils.fieldsSubtract(new Fields(parentFactory.getOutputFields()), context.getSelfOutputFields()));
        _factory = new OperationOutputFactory(projectionFactory, context.getSelfOutputFields());
    }
    
    public void setContext(ProcessorContext pc, TridentTuple t) {
        this._context = pc;
        this._tuple = t;
    }

	@Override
	public void emit(List<Object> values) {
        TridentTuple toEmit = _factory.create((TridentTupleView) _tuple, values);
        for(TupleReceiver r: _triContext.getReceivers()) {
            r.execute(_context, _triContext.getOutStreamId(), toEmit);
        }
	}

	@Override
	public void reportError(Throwable t) {
		_triContext.getDelegateCollector().reportError(t);
	}
	
    public Factory getOutputFactory() {
        return _factory;
    }

}
