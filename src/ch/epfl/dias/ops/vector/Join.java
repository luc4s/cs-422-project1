package ch.epfl.dias.ops.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class Join implements VectorOperator {

	private final VectorOperator mLeftChild;
	private final VectorOperator mRightChild;
	private final int mLeftFieldNo;
	private final int mRightFieldNo;
	
	private DataType[] mLeftSchema;
	
	private HashMap<Object, LinkedList<Integer> > mHashTable;
	private ArrayList<DBColumn[]> mLeftColumns;
	
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
		mLeftColumns = new ArrayList<>();

		DBColumn[] cols;
		int counter = 0;
		while ((cols = mLeftChild.next())[0].length() > 0) {
			mLeftColumns.add(cols);
			final DBColumn column = cols[mLeftFieldNo];
			
			for (int i = 0; i < column.length(); ++i) {
				final Object key = column.get()[i];
				LinkedList<Integer> bucket = mHashTable.get(key);
				if (bucket == null) {
					bucket = new LinkedList<>();
					mHashTable.put(key, bucket);
				}
				bucket.addLast(counter + i);
			}
			counter += column.length();
		}
		
		if (mLeftColumns.isEmpty())
			return;

		DBColumn[] vector = mLeftColumns.get(0);
		mLeftSchema = new DataType[vector.length];
		for (int i = 0; i < vector.length; i++)
			mLeftSchema[i] = vector[i].type();
	}

	@Override
	public DBColumn[] next() {
		if (!mBuffer.isEmpty())
			return mBuffer.removeFirst();

		DBColumn[] rightCols = mRightChild.next();
		DBColumn rightField = rightCols[mRightFieldNo];
		
		DBColumn[] vector = new DBColumn[rightCols.length + mLeftSchema.length];
		for (int i = 0; i < mLeftSchema.length; ++i)
			vector[i] = new DBColumn(mLeftSchema[i]);
		for (int i = 0; i < rightCols.length; ++i)
			vector[i + mLeftSchema.length] = new DBColumn(rightCols[i].type());

		if (rightCols[0].length() == 0 || mLeftColumns.isEmpty())
			return vector; // Return empty vector in case one of the operand is empty

		int counter = 0;
		int vecSize = rightCols[0].length();
		while (counter < vecSize && rightCols[0].length() > 0) {
			for (int i = 0; i < rightField.length(); ++i) {
				LinkedList<Integer> bucket = mHashTable.get(rightField.get(i));
				if (bucket == null || bucket.isEmpty())
					continue;
				
				for (Integer index : bucket) {
					counter++;
					Object[] leftRow = getLeftRow(index);
					for (int j = 0; j < leftRow.length; ++j)
						vector[j].append(leftRow[j]);
					
					for (int j = 0; j < rightCols.length; ++j)
						vector[leftRow.length + j].append(rightCols[j].get(i));
					
					if (counter >= vecSize) {
						mBuffer.addLast(vector);
						vector = new DBColumn[rightCols.length + mLeftSchema.length];
						for (int j = 0; j < mLeftSchema.length; ++j)
							vector[j] = new DBColumn(mLeftSchema[j]);
						for (int j = 0; j < rightCols.length; ++j)
							vector[j + mLeftSchema.length] = new DBColumn(rightCols[j].type());
					}
				}
			}
			
			if (counter < vecSize) {
				rightCols = mRightChild.next();
				rightField = rightCols[mRightFieldNo];
			}
		}
		
		if (mBuffer.isEmpty())
			return vector;
		else {
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
	
	private Object[] getLeftRow(int index) {
		int counter = 0;
		
		for (DBColumn[] cols : mLeftColumns) {
			final int vecSize = cols[0].length();
			if (index < counter + vecSize) {
				Object[] row = new Object[cols.length];
				for (int i = 0; i < cols.length; ++i)
					row[i] = cols[i].get(index - counter);
				
				return row;
			} else
				counter += vecSize;
			
			// Safety check
			if (vecSize == 0)
				break;
		}
		throw new IndexOutOfBoundsException(index);
	}
}
