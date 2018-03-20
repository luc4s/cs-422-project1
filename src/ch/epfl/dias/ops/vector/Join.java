package ch.epfl.dias.ops.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class Join implements VectorOperator {

	private final static class Row {
		private final Object[] row;
	
		public Row(int index, DBColumn[] vector) {
			row = new Object[vector.length];
			for (int i = 0; i < vector.length; ++i)
				row[i] = vector[i].get(index);
		}
		
		public Object[] getRow() {
			return row;
		}
	}
	private final VectorOperator mLeftChild;
	private final VectorOperator mRightChild;
	private final int mLeftFieldNo;
	private final int mRightFieldNo;
	
	private DataType[] mLeftSchema;
	private DataType[] mFinalSchema;
	
	private HashMap<Object, LinkedList<Row> > mHashTable;
	
	private LinkedList<DBColumn[]> mBuffer;


	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		if (leftChild == null || rightChild == null)
			throw new NullPointerException();
		
		if (leftFieldNo < 0 || rightFieldNo < 0)
			throw new IllegalArgumentException("Field number must be positive.");
		
		mLeftChild = leftChild;
		mRightChild = rightChild;
		mLeftFieldNo = leftFieldNo;
		mRightFieldNo = rightFieldNo;
		mBuffer = new LinkedList<>();
	}

	@Override
	public void open() {
		mLeftChild.open();
		mRightChild.open();
		mHashTable = new HashMap<>();

		DBColumn[] cols;
		DataType[] leftSchema = null;
		while ((cols = mLeftChild.next())[0].length() > 0) {
			final DBColumn column = cols[mLeftFieldNo];
			
			for (int i = 0; i < column.length(); ++i) {
				final Object key = column.get(i);
				LinkedList<Row> bucket = mHashTable.get(key);
				if (bucket == null) {
					bucket = new LinkedList<>();
					mHashTable.put(key, bucket);
				}
				bucket.addLast(new Row(i, cols));
			}
			
			if (leftSchema == null) {
				leftSchema = new DataType[cols.length];
				for (int i = 0; i < leftSchema.length; ++i)
					leftSchema[i] = cols[i].type();
			}
		}
		

		mLeftSchema = leftSchema;
		DBColumn[] right = mRightChild.next();
		if (right[0].length() == 0)
			return;

		mFinalSchema = new DataType[mLeftSchema.length + right.length];
		for (int i = 0; i < leftSchema.length; i++)
			mFinalSchema[i] = leftSchema[i];
		
		for (int i = 0, j = leftSchema.length; i < right.length; ++i)
			mFinalSchema[j++] = right[i].type();
		
		DBColumn[] vector = new DBColumn[mFinalSchema.length];
		for (int i = 0; i < vector.length; ++i)
			vector[i] = new DBColumn(mFinalSchema[i], right[0].length());

		DBColumn rightField = right[mRightFieldNo];
		int counter = 0;
		int vecSize = rightField.length();
		for (int i = 0; i < rightField.length(); ++i) {
			LinkedList<Row> bucket = mHashTable.get(rightField.get(i));
			if (bucket == null || bucket.isEmpty())
				continue;
			
			for (Row row : bucket) {
				++counter;
				Object[] leftRow = row.getRow();
				for (int j = 0; j < leftRow.length; ++j)
					vector[j].append(leftRow[j]);
				
				for (int j = 0; j < right.length; ++j)
					vector[leftRow.length + j].append(right[j].get(i));
				
				if (counter >= vecSize) {
					counter = 0;
					mBuffer.addLast(vector);
					vector = new DBColumn[right.length + mLeftSchema.length];
					for (int j = 0; j < mLeftSchema.length; ++j)
						vector[j] = new DBColumn(mLeftSchema[j], right[0].length());
					for (int j = 0; j < right.length; ++j)
						vector[j + mLeftSchema.length] = new DBColumn(right[j].type(), right[0].length());
				}
			}
		}
		
		if (vector[0].length() > 0)
			mBuffer.add(vector);
	}

	@Override
	public DBColumn[] next() {
		if (!mBuffer.isEmpty())
			return mBuffer.removeFirst();

		DBColumn[] rightCols = mRightChild.next();
		DBColumn rightField = rightCols[mRightFieldNo];
		
		DBColumn[] vector = new DBColumn[mFinalSchema.length];
		for (int i = 0; i < mFinalSchema.length; ++i)
			vector[i] = new DBColumn(mFinalSchema[i], rightField.length());

		int vecSize = rightCols[0].length();
		if (vecSize == 0 || mHashTable.isEmpty())
			return vector; // Return empty vector in case one of the operand is empty

		int counter = 0;
		boolean stop = false;
		while (!stop && rightField.length() > 0) {
			for (int i = 0; i < rightField.length(); ++i) {
				LinkedList<Row> bucket = mHashTable.get(rightField.get(i));
				if (bucket == null || bucket.isEmpty())
					continue;
				
				for (Row row : bucket) {
					++counter;
					Object[] leftRow = row.getRow();
					for (int j = 0; j < leftRow.length; ++j)
						vector[j].append(leftRow[j]);
					
					for (int j = 0; j < rightCols.length; ++j)
						vector[leftRow.length + j].append(rightCols[j].get(i));
					
					if (counter >= vecSize) {
						counter = 0;
						mBuffer.addLast(vector);
						vector = new DBColumn[rightCols.length + mLeftSchema.length];
						for (int j = 0; j < mLeftSchema.length; ++j)
							vector[j] = new DBColumn(mLeftSchema[j], rightCols[0].length());
						for (int j = 0; j < rightCols.length; ++j)
							vector[j + mLeftSchema.length] = new DBColumn(rightCols[j].type(), rightCols[0].length());
						
						stop = true;
					}
				}
			}
			
			if (!stop) {
				rightCols = mRightChild.next();
				rightField = rightCols[mRightFieldNo];
			}
		}
		
		if (mBuffer.isEmpty())
			return vector;
		else {
			if (vector[0].length() > 0)
				mBuffer.addLast(vector);

			return mBuffer.removeFirst();
		}
	}

	@Override
	public void close() {
		mLeftChild.close();
		mRightChild.close();
		mHashTable.clear();
	}
}
