package com.nofatclips.thumbtack.db;

import java.util.HashSet;
import java.util.Set;

/**
 * 
 * Stores the delta with the underlying layer for a given level of a nested transaction.
 * On commit, it gets merged with the underlying layer.
 * On rollback, it just gets discarded from the layer stack.
 * 
 * The main difference with the BaseDataLayer is the special handling for deleted
 * values: can't just remove the key from the data store, since it could still
 * be found in one of the underlying layers. Can't delete from the underlying
 * layer 'cause it makes rollback too difficult (or costly).
 * Additionally, to make commit and search by value easily, on set, all keys
 * with the same value are imported from the underlying layer.
 * 
 * Also, set() and unset() will also update the deletedKeys set accordingly.
 * 
 * The get() method will recursively try the underlying layer when failing
 * on the current layer, unless the key is stored in the deletedKeys set.
 * Same for numEqualTo().
 * 
 * @author DeK
 *
 */

public class IncrementalDataLayer extends BaseDataLayer {

	protected Set<String> deletedKeys;
	private BaseDataLayer underlyingLayer;
	
	public IncrementalDataLayer(BaseDataLayer dataLayer) {
		this.deletedKeys = new HashSet<String>();
		this.underlyingLayer = dataLayer;
	}
	
	@Override
	public void set(String name, String value) {
		importValue(value);
		this.deletedKeys.remove(name);
		super.set(name, value);
	}
	
	@Override
	public void unset(String name) {
		importValue(get(name));
		super.unset(name);
		this.deletedKeys.add(name);
	}

	/** 
	 *  
	 * Import all keys with value "value" from the underlyingLayer.index to this.index
	 * This overrides the count from the underlying index without changing it.
	 * In case of rollback, the old count will return available.
	 * In case of commit, this count overwrites the old one.
	 * 
	 * @param value 
	 * 
	 */
	protected void importValue(String value) {
//		if (this.underlyingLayer.containsValue(value)) {
		index().initKey(value, numEqualTo(value));
//		}
	}
	
	/**
	 *
	 * Recursively calls get() on the underlying layer if fails on this.
	 * Recursion will end when underlying layer is the base layer. 
	 * 
	 */	
	@Override
	public String get(String name) {
		if (isDeleted(name)) return null;
		String ret = super.get(name);
		if (ret!=null) return ret;
		return this.underlyingLayer.get(name);
	}
	
	/**
	 *
	 * Recursively calls numEqualTo() on the underlying layer if fails on this.
	 * Recursion will end when underlying layer is the base layer. 
	 * 
	 */
	@Override
	public int numEqualTo(String value) {
		if (containsValue(value)) {
			return super.numEqualTo(value);
		}
		return this.underlyingLayer.numEqualTo(value);
	}
	
	public boolean isDeleted(String name) {
		return this.deletedKeys.contains(name);
	}
		
	@Override
	protected void unsetAll(Set<String> deletedKeys) {
		this.deletedKeys.addAll(deletedKeys);
	}
	
	public void mergeBack() {
		this.underlyingLayer.mergeFrom(this);
	}

}
