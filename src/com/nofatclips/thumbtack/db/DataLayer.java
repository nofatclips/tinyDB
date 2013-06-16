package com.nofatclips.thumbtack.db;

public interface DataLayer {
	
	public void set(String name, String value);
	public String get(String name);
	public void unset(String name);
	public int numEqualTo(String value);

}
