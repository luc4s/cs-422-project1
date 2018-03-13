package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {
	private final Object[] mColumn;
	
	public DBColumn(Object[] data) {
		if (data == null)
			throw new NullPointerException();

		mColumn = data;
	}
	
	public int length() {
		return mColumn.length;
	}

	public Integer[] getAsInteger() {
		return (Integer[]) mColumn;
	}
	
	public Boolean[] getAsBoolean() {
		return (Boolean[]) mColumn;
	}
	
	public String[] getAsString() {
		return (String[]) mColumn;
	}
	
	public Double[] getAsDouble() {
		return (Double[]) mColumn;
	}
}
