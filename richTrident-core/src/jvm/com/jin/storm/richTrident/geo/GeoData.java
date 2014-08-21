package com.jin.storm.richTrident.geo;

import com.infomatiq.jsi.Rectangle;

public interface GeoData {
	Rectangle getBoundBox();
	float distanceTo(GeoData anotherGeoData);
}
