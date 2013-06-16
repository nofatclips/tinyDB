package com.nofatclips.thumbtack.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 
 * The base layer for the DB. Implementation should be straightforward.
 * 
 * It has an inverted (value to key) index for fast (<O(n)) implementation of numEqualTo()
 * 
 * @author DeK
 *
 */

public class BaseDataLayer implements DataLayer {
	
	private Map<String, String> layerStore;
	private BucketCounter layerIndex;
	
	public BaseDataLayer() {
		this.layerStore = new HashMap<String, String>();
		this.layerIndex = new BucketCounter();
	}

	@Override
	public void set(String name, String value) {
		index().removeOne(get(name)); // Remove old value from index if exists
		store().put(name, value);
		index().putOne(value);
	}

	@Override
	public String get(String name) {
		return store().get(name);
	}

	@Override
	public void unset(String name) {
		index().removeOne(get(name));
		store().remove(name);
	}

	@Override
	public int numEqualTo(String value) {
		return index().numEqualTo(value);
	}
	
	public boolean containsValue(String value) {
		return index().containsKey(value);
	}
	
	public void mergeFrom(IncrementalDataLayer that) {
		this.store().putAll(that.store());
		this.unsetAll(that.deletedKeys);
		this.index().mergeMap(that.index());
	}
	
	protected void unsetAll(Set<String> deletedKeys) {
		for (String deletedKey: deletedKeys) {
			unset(deletedKey);
		}		
	}

	public BucketCounter index() {
		return this.layerIndex;
	}
	
	public Map<String, String> store() {
		return this.layerStore;
	}
	
}
