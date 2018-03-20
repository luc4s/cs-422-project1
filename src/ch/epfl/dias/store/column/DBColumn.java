package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {
	private ArrayList<Object> mColumn;
	private final DataType mType;
	
	public DBColumn(DataType type) {
		if (type == null)
			throw new NullPointerException();

		mColumn = new ArrayList<>();
		mType = type;
	}

	public DBColumn(Object[] data, DataType type) {
		if (data == null || type == null)
			throw new NullPointerException();

		mColumn = new ArrayList<>();
		switch (type) {
			case INT: 
				Integer[] intColumn = new Integer[data.length];
				for (int i = 0; i < data.length; ++i)
					intColumn[i] = (Integer)data[i];
				mColumn.addAll(Arrays.asList(intColumn));
				break;
			case DOUBLE:
				Double[] doubleColumn = new Double[data.length];
				for (int i = 0; i < data.length; ++i)
					doubleColumn[i] = (Double)data[i];
				mColumn.addAll(Arrays.asList(doubleColumn));
				break;
			case BOOLEAN:
				Boolean[] booleanColumn = new Boolean[data.length];
				for (int i = 0; i < data.length; ++i)
					booleanColumn[i] = (Boolean)data[i];
				mColumn.addAll(Arrays.asList(booleanColumn));
				break;
			case STRING:
				String[] stringColumn = new String[data.length];
				for (int i = 0; i < data.length; ++i)
					stringColumn[i] = (String)data[i];
				mColumn.addAll(Arrays.asList(stringColumn));
				break;
			default:
				throw new RuntimeException("Unrecognized type");
		}
		mType = type;
	}
	
	public int length() {
		return mColumn.size();
	}
	
	public DataType type() {
		return mType;
	}

	public Integer[] getAsInteger() {
		return mColumn.toArray(new Integer[] {});
	}
	
	public Boolean[] getAsBoolean() {
		return mColumn.toArray(new Boolean[] {});
	}
	
	public String[] getAsString() {
		return mColumn.toArray(new String[] {});
	}
	
	public Double[] getAsDouble() {
		return mColumn.toArray(new Double[] {});
	}
	
	public Object[] get() {
		return mColumn.toArray();
	}
	
	public Object get(int i) {
		if (i < 0 || i > mColumn.size())
			throw new IllegalArgumentException();
		
		return mColumn.get(i);
	}
	
	public void append(Object o) {
		mColumn.add(o);
	}
}
