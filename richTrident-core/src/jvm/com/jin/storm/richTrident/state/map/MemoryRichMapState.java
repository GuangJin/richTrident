/** 
 * @file MemoryTreeMapState.java
 * @class com.jin.storm.richTrident.MemoryRichMapState
 * @brief This state uses the TreeMap as the in-memory DB to support more types of queries.
 * @note The purpose here is to create a snap shot of latest tuples distributed by their identity.
 * @author Guang Jin
 * @email jin_guang@hotmail.com
 **/

package com.jin.storm.richTrident.state.map;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;
import storm.trident.state.ITupleCollection;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;

public class MemoryRichMapState implements ITupleCollection, RichMapState {

	public static class Factory implements StateFactory {
		String _id;
		Fields _idField;
		Fields _indexField;
		Map<Object, HashSet<TridentTuple>> _richMap;

		public Factory(Fields idField, Fields indexField,
				Map<Object, HashSet<TridentTuple>> richMap) {
			this._id = UUID.randomUUID().toString();
			this._idField = idField;
			this._indexField = indexField;
			this._richMap = richMap;
		}

		@Override
		public State makeState(Map conf, IMetricsContext context,
				int partitionIndex, int numPartitions) {
			return new MemoryRichMapState(_id + partitionIndex, _idField,
					_indexField, _richMap);
		}
	}

	static class MemoryRichMapStateBacking implements IRichBackingMap,
			ITupleCollection {

		public static void clearAll() {
			_dbs.clear();
		}

		Map[] _mapDBs;
		Fields _indexField;

		public MemoryRichMapStateBacking(String id, Fields indexField,
				Map<Object, HashSet<TridentTuple>> richMap) {
			if (!_dbs.containsKey(id)) {
				_dbs.put(id, new Map[2]);
			}
			this._mapDBs = (Map[]) _dbs.get(id);
			// there are two maps in the mapDBs
			// the first one is a HashMap mapping the idField to its tuple
			// the second one is an indexMap mapping the indexField to a set of tuples
			this._mapDBs[0] = new HashMap<Object, TridentTuple>();
			this._mapDBs[1] = richMap;
			this._indexField = indexField;
		}

		@Override
		public List<TridentTuple> multiGet(List<Object> keys) {
			List<TridentTuple> ret = new ArrayList();
			for (Object key : keys) {
				ret.add((TridentTuple) _mapDBs[0].get(key));
			}
			return ret;
		}

		@Override
		public void multiPut(List<Object> keys, List<TridentTuple> tuples) {
			for (int i = 0; i < keys.size(); i++) {
				Object key = keys.get(i);
				TridentTuple oldTuple = (TridentTuple) _mapDBs[0].remove(key);
				TridentTuple newTuple = tuples.get(i);
				_mapDBs[0].put(key, newTuple);
				HashSet<TridentTuple> tupleSet;
				if (oldTuple != null) {
					Object oldIndexVal = oldTuple.getValueByField(_indexField
							.get(0));
					tupleSet = (HashSet<TridentTuple>) _mapDBs[1].get(oldIndexVal);
					tupleSet.remove(oldTuple);
					// _mapDBs[1].put(oldIndexVal, tupleSet);
				}
				Object newIndexVal = newTuple.getValueByField(_indexField
						.get(0));
				if (!_mapDBs[1].containsKey(newIndexVal)) {
					tupleSet = new HashSet<TridentTuple>();
					_mapDBs[1].put(newIndexVal, tupleSet);
				} else {
					tupleSet = (HashSet<TridentTuple>) _mapDBs[1].get(newIndexVal);
					//System.out.println("MemoryRichMapStateBacking@"+ Integer.toHexString(System.identityHashCode(this))+ " for index:" + newIndexVal + " oldSet "+ tupleSet);
				}
				tupleSet.add(newTuple);
				//System.out.println("MemoryRichMapStateBacking@"+ Integer.toHexString(System.identityHashCode(this))+ " for index:" + newIndexVal + " newSet " + tupleSet);
				// System.out.println("MemoryRichMapStateBacking@"+Integer.toHexString(System.identityHashCode(this))+" works on TreeMap@"+Integer.toHexString(System.identityHashCode(_mapDBs[1])));
			}
		}

		@Override
		public Iterator<List<Object>> getTuples() {
			return new Iterator<List<Object>>() {

				private Iterator<Map.Entry<Object, TridentTuple>> it = _mapDBs[0].entrySet().iterator();

				public boolean hasNext() {
					return it.hasNext();
				}

				public List<Object> next() {
					Map.Entry<Object, TridentTuple> e = it.next();
					List<Object> ret = new ArrayList<Object>();
					ret.add(e.getKey());
					ret.add(e.getValue());
					return ret;
				}

				public void remove() {
					throw new UnsupportedOperationException("Not supported yet.");
				}
			};
		}

		@Override
		public Map getRichMap() {
			return  _mapDBs[1];
		}

	}

	static ConcurrentHashMap<String, Object[]> _dbs = new ConcurrentHashMap<String, Object[]>();
	Map[] mapDBs;
	Long _currTx = null;
	MemoryRichMapStateBacking _backing;
	Fields _idField;
	Fields _indexField;

	public MemoryRichMapState(String id, Fields idField, Fields indexField,
			Map<Object, HashSet<TridentTuple>> richMap) {
		_backing = new MemoryRichMapStateBacking(id, indexField, richMap);
		_idField = idField;
		_indexField = indexField;
	}

	@Override
	public void beginCommit(Long txid) {
		_currTx = txid;// txid is not that important in this case
	}

	@Override
	public void commit(Long txid) {
		// Nothing to work on
	}

	@Override
	public List<TridentTuple> multiGet(List<Object> ids) {
		return (List<TridentTuple>) _backing.multiGet(ids);
	}

	@Override
	public void multiUpdate(List<Object> keys, List<TridentTuple> vals) {
		_backing.multiPut(keys, vals);
	}

	@Override
	public Iterator<List<Object>> getTuples() {
		return _backing.getTuples();
	}

	public Map getRichMap() {
		return _backing.getRichMap();
	}

}