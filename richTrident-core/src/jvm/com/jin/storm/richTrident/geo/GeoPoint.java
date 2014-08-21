package com.jin.storm.richTrident.geo;

import java.io.Serializable;

import com.infomatiq.jsi.Point;
import com.infomatiq.jsi.Rectangle;

public class GeoPoint implements GeoData, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	float _x;
	float _y;
	
	public GeoPoint(float x, float y){
		this._x = x;
		this._y = y;
	}
	
	public Point getPoint(){
		return new Point(_x,_y);
	}
	
	@Override
	public Rectangle getBoundBox() {
		return new Rectangle(_x, _y, _x, _y);
	}

	@Override
	public float distanceTo(GeoData anotherGeoData) {
		Rectangle rect = anotherGeoData.getBoundBox();
		return rect.distance(new Point(_x,_y));
	}
	
	public String toString(){
		return "("+_x+","+_y+")";
	}
}
