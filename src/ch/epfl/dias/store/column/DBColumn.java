package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {
	private final Object[] mColumn;
	private final DataType mType;
	
	public DBColumn(Object[] data, DataType type) {
		if (data == null)
			throw new NullPointerException();

		switch (type) {
			case INT: 
				Integer[] intColumn = new Integer[data.length];
				for (int i = 0; i < data.length; ++i)
					intColumn[i] = (Integer)data[i];
				mColumn = intColumn;
				break;
			case DOUBLE:
				Double[] doubleColumn = new Double[data.length];
				for (int i = 0; i < data.length; ++i)
					doubleColumn[i] = (Double)data[i];
				mColumn = doubleColumn;
				break;
			case BOOLEAN:
				Boolean[] booleanColumn = new Boolean[data.length];
				for (int i = 0; i < data.length; ++i)
					booleanColumn[i] = (Boolean)data[i];
				mColumn = booleanColumn;
				break;
			case STRING:
				String[] stringColumn = new String[data.length];
				for (int i = 0; i < data.length; ++i)
					stringColumn[i] = (String)data[i];
				mColumn = stringColumn;
				break;
			default:
				throw new RuntimeException("Unrecognized type");
		}
		mType = type;
	}
	
	public int length() {
		return mColumn.length;
	}
	
	public DataType type() {
		return mType;
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
	
	public Object[] get() {
		return mColumn;
	}
	
	public Object get(int i) {
		if (i < 0 || i > mColumn.length)
			throw new IllegalArgumentException();
		
		return mColumn[i];
	}
}
