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


	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		if (leftChild == null || rightChild == null)
			throw new NullPointerException();
		
		if (leftFieldNo < 0 || rightFieldNo < 0)
			throw new IllegalArgumentException("Field number must be positive.");
		
		mLeftChild = leftChild;
		mRightChild = rightChild;
		mLeftFieldNo = leftFieldNo;
		mRightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		mLeftChild.open();
		mRightChild.open();
		mHashTable = new HashMap<>();
		mLeftColumns = new ArrayList<>();

		DBColumn[] cols;
		int counter = 0;
		while ((cols = mLeftChild.next()).length > 0) {
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
		DBColumn[] rightCols = mRightChild.next();
		if (rightCols.length == 0 || mLeftColumns.isEmpty())
			return new DBColumn[0]; // Return empty vector in case one of the operand is empty

		DBColumn rightField = rightCols[mRightFieldNo];
		
		ArrayList< ArrayList<Object> > fieldsList = new ArrayList<>();
		for (int i = 0; i < mLeftSchema.length + rightCols.length; ++i)
			fieldsList.add(new ArrayList<Object>());
		
		for (int i = 0; i < rightField.length(); ++i) {
			LinkedList<Integer> bucket = mHashTable.get(rightField.get(i));
			if (bucket == null || bucket.isEmpty())
				continue;
			
			for (Integer index : bucket) {
				Object[] leftRow = getLeftRow(index);
				for (int j = 0; j < leftRow.length; ++j)
					fieldsList.get(j).add(leftRow[j]);
				
				for (int j = 0; j < rightCols.length; ++j)
					fieldsList.get(leftRow.length + j).add(rightCols[j].get(i));
			}
		}

		DBColumn[] cols = new DBColumn[fieldsList.size()];
		for (int i = 0; i < mLeftSchema.length; ++i)
			cols[i] = new DBColumn(fieldsList.get(i).toArray(), mLeftSchema[i]);

		for (int i = 0; i < rightCols.length; ++i)
			cols[mLeftSchema.length + i] = new DBColumn(fieldsList.get(mLeftSchema.length + i).toArray(), rightCols[i].type());
		
		return cols;
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
