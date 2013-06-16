package com.nofatclips.thumbtack.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BucketCounter {
	
	private Map<String, Integer> index;
	
	public BucketCounter() {
		this.index = new HashMap<String, Integer>();
	}
	
	public Integer get(String value) {
		return this.index.get(value);
	}
	
	public boolean containsKey(String value) {
		return this.index.containsKey(value);
	}

	public void putOne(String value) {
		if (!containsKey(value)) {
			initKey(value);
		}
		this.index.put(value, get(value)+1);
	}
	
	public void mergeMap(BucketCounter that) {
		for (Map.Entry<String, Integer> entry : that.entrySet()) {
			initKey(entry.getKey(), entry.getValue());
		}
	}
	
	private Set<Map.Entry<String, Integer>> entrySet() {
		return this.index.entrySet();
	}

	public void initKey(String value, Integer count) {
		this.index.put(value, count);
	}

	public void initKey(String value) {
		initKey(value, 0);
	}

	public void removeOne(String value) {
		if (containsKey(value)) {
			this.index.put(value, get(value)-1);	
		}
	}

	public int numEqualTo(String value) {
		Integer bucketSize = this.index.get(value);
		if (bucketSize == null) return 0;
		return bucketSize;
	}
	
	public String toString() {
		String ret = "{";
		String sep = "";
		for (Map.Entry<String, Integer> entry : entrySet()) {
			ret += sep + entry.getKey() + ": '" + entry.getValue() + "'";
			sep = ", ";
		}
		return ret + "}";
	}

}
