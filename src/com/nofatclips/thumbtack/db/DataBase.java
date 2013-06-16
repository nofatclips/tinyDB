package com.nofatclips.thumbtack.db;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * 
 * Classic composite pattern. It has a stack rather than a list of items and
 * the head is stored separately. It mostly delegates every operation
 * to the current item (the one on top of the stack, or the head if
 * the stack is empty).
 * 
 * Each item provides incremental information to be added to the head
 * on commit.
 * 
 * @author DeK
 *
 */

public class DataBase implements DataLayer {
	
	private BaseDataLayer baseLayer;
	private Deque<IncrementalDataLayer> layerStack;
	
	public DataBase() {
		this.baseLayer = new BaseDataLayer();
		this.layerStack = new ArrayDeque<IncrementalDataLayer>();
	}
	
	@Override
	public void set(String name, String value) {
		currentLayer().set(name, value);
	}

	@Override
	public String get(String name) {
//		Iterator<DataLayer> inStackingOrder = this.layerStack.descendingIterator();
//		while(inStackingOrder.hasNext()){
//			String ret = inStackingOrder.next().get(name);
//			if (ret != null) return ret;
//		}
		return currentLayer().get(name);
	}

	@Override
	public void unset(String name) {
		currentLayer().unset(name);
	}

	@Override
	public int numEqualTo(String value) {
		return currentLayer().numEqualTo(value);
	}
	
	public void begin() {
		// Creates a new layer, uses the current one as its underlying layer, pushes onto stack
		push(new IncrementalDataLayer(currentLayer()));
	}
	
	public void rollback() throws InvalidRollbackException {
		if (transactionRunning()) {
			pop();
		} else {
			throw new InvalidRollbackException();
		}
	}
	
	protected boolean transactionRunning() {
		return !this.layerStack.isEmpty();
	}

	public void commit() {
		while (transactionRunning()) {
			pop().mergeBack();
		}
	}
	
	public BaseDataLayer currentLayer() {
		if (transactionRunning()) return this.layerStack.peekFirst();
		return this.baseLayer;
	}
	
	public IncrementalDataLayer pop() {
		return this.layerStack.pop();
	}
	
	public void push(IncrementalDataLayer newLayer) {
		this.layerStack.push(newLayer);
	}

}
