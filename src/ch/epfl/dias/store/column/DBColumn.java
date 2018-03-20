package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import ch.epfl.dias.store.DataType;

public class DBColumn {
	private ArrayList<Object> mColumn;
	
	private final DataType mType;
	
	private boolean mDirty;
	private IntStream mIntStream;
	private DoubleStream mDoubleStream;
	
	public DBColumn(DataType type) {
		if (type == null)
			throw new NullPointerException();

		mColumn = new ArrayList<>();
		mType = type;
		mDirty = false;
		mIntStream = null;
		mDoubleStream = null;
	}
	
	public DBColumn(DataType type, int reserved) {
		this(type);
		mColumn = new ArrayList<>(reserved);
	}

	public DBColumn(Object[] data, DataType type) {
		this(type);

		mColumn = new ArrayList<>(data.length);
		for (int i = 0; i < data.length; ++i)
			mColumn.add(data[i]);
		createStreams();
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
		return mColumn.get(i);
	}
	
	public void append(Object o) {
		mColumn.add(o);
		mDirty = true;
	}
	
	public IntStream intStream() {
		if (mDirty)
			createStreams();
		
		return mIntStream;
	}
	
	public DoubleStream doubleStream() {
		if (mDirty)
			createStreams();
		
		return mDoubleStream;
	}
	
	private void createStreams() {
		mDirty = false;
		if (mType == DataType.INT)
			mIntStream = mColumn.stream().mapToInt(i -> ((Integer) i).intValue()).parallel();
		else if (mType == DataType.DOUBLE)
			mDoubleStream = mColumn.stream().mapToDouble(i -> ((Double) i).intValue()).parallel();
	}
}
