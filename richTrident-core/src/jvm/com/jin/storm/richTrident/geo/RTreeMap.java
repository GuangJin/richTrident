/** 
 * @file RTreeMap.java
 * @class com.jin.storm.richTrident.state.map.RTreeMap
 * @brief This class implements Map and uses the RTree to index GeoData.
 * @note The local data shall contain a RTree as the spatial index. RTree and SpatialIndex are from the Java Spatial Index (RTree) Library (See http://sourceforge.net/projects/jsi/).  
 * @author Guang Jin
 * @email jin_guang@hotmail.com
 **/

package com.jin.storm.richTrident.geo;

import gnu.trove.TIntArrayList;
import gnu.trove.TIntObjectHashMap;
import gnu.trove.TIntProcedure;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.infomatiq.jsi.Rectangle;
import com.infomatiq.jsi.SpatialIndex;
import com.infomatiq.jsi.rtree.RTree;

public class RTreeMap<V> implements Map<GeoData, V>, Serializable{
	private class WorkerProcedure implements TIntProcedure{//My TIntProcedure to generate RTree query results.
		private TIntArrayList _idList;
		
		public WorkerProcedure(TIntArrayList idList){
			_idList = idList;
		}
		
		@Override
		public boolean execute(int rectID) {
			this._idList.add(rectID);
			return true;
		}
	}
	
	private SpatialIndex _spatialIndex;//the Rtree which is non-serializable
	private TIntObjectHashMap<GeoData> _idMap;//a TIntObjectHashMap mapping the integer ID with the GeoData
	private HashMap<GeoData, V> _valueMap;//a hashmap mapping the GeoData as the key with its value V
	
	public RTreeMap(){
		//the Rtree cannot be created here
		_idMap = new TIntObjectHashMap<GeoData>();
		_valueMap = new HashMap<GeoData, V>();
	}

	@Override
	public int size() {
		return _valueMap.size();
	}

	@Override
	public boolean isEmpty() {
		return _valueMap.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		if(key instanceof GeoData){
			GeoData geoData = (GeoData) key;
			return _valueMap.containsKey(geoData);
		} else
			return false;
	}

	@Override
	public boolean containsValue(Object value) {
		return _valueMap.containsKey(value);
	}

	@Override
	public V get(Object key) {
		return _valueMap.get(key);
	}

	/**
	 * @brief add a GeoData as the key with its value into the map
	 * @param	key		The GeoData key.
	 * @param	value	The V value.
	 * @return	the previous value associated with key, or null if there was no mapping for key. (A null return can also indicate that the map previously associated null with key.).
	 **/
	@Override
	public V put(GeoData key, V value) {
		int keyID = System.identityHashCode(key);
		Rectangle boundBox = key.getBoundBox();
		if (_spatialIndex==null) {
			_spatialIndex = new RTree();
			_spatialIndex.init(null);
		}
		_spatialIndex.add(boundBox, keyID);//add the geoData to the RTree
		_idMap.put(keyID, key);//update the idMap
		return _valueMap.put(key, value);
	}

	/**
	 * @brief remove a GeoData as the key with its value from the map
	 * @param	key		The GeoData key.
	 * @return	the previous value associated with key, or null if there was no mapping for key. (A null return can also indicate that the map previously associated null with key.)
	 **/
	@Override
	public V remove(Object key) {
		if(key instanceof GeoData){
			int keyID = System.identityHashCode(key);
			GeoData geoData = (GeoData) key;
			Rectangle boundBox = geoData.getBoundBox();
			_spatialIndex.delete(boundBox, keyID);//remove the geoData to the RTree
			_idMap.remove(keyID);
			return _valueMap.remove(geoData);
		} else
			return null;
	}

	@Override
	public void putAll(Map<? extends GeoData, ? extends V> m) {
		throw new UnsupportedOperationException("RTreeMap does not support putAll() yet.");
	}

	@Override
	public void clear() {
		_spatialIndex = new RTree();
		_spatialIndex.init(null);
		_idMap.clear();
		_valueMap.clear();
	}

	@Override
	public Set<GeoData> keySet() {
		return _valueMap.keySet();
	}

	@Override
	public Collection<V> values() {
		return _valueMap.values();
	}

	@Override
	public Set<Entry<GeoData, V>> entrySet() {
		return _valueMap.entrySet();
	}

	/**
	 * @brief return the nearest Value to the input point without a distance constraint
	 * @param	p	The input point gets compared against.
	 * @return	The value associated with the geoData whose bounding box is nearest to the input point p. A null return indicates an empty map.
	 **/
	public V nearest(GeoData p){
		return nearest(p, Float.MAX_VALUE);
	}
	
	/**
	 * @brief return the nearest Value to the input point with a distance constraint
	 * @param	p					The input point gets compared against.
	 * @param	furthestDistance	The distance constraint.
	 * @return	The value associated with the geoData whose bounding box is nearest to the input point p. A null return indicates no value is found within the distance constraint.
	 **/
	public V nearest(GeoData p,  float furthestDistance){
		if (p==null) return null;//cannot work on null input
		if (p instanceof GeoPoint){
			GeoPoint geoPoint = (GeoPoint) p;
			TIntArrayList idResult = new TIntArrayList();
			WorkerProcedure _workerProcedure = new WorkerProcedure(idResult);
			if(_spatialIndex==null) return null;
			_spatialIndex.nearest(geoPoint.getPoint(), _workerProcedure, furthestDistance);
			if (idResult.size() == 0)
				return null;
			else {
				int id = idResult.get(0);
				GeoData geoData = _idMap.get(id);
				return _valueMap.get(geoData);
			}
		} else
			return null;
	}
}
