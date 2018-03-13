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

		DBColumn[] cols;
		while ((cols = mLeftChild.next()) != null) {
			DBColumn column = cols[mLeftFieldNo];
			
			for (int i = 0; i < column.length(); ++i) {
				final Object key = column.get()[i];
				LinkedList<Integer> bucket = mHashTable.get(key);
				if (bucket == null) {
					bucket = new LinkedList<>();
					mHashTable.put(key, bucket);
				}
				bucket.addLast(i);
			}
		}
		
		if (cols == null || cols.length < 1)
			return;

		mLeftSchema = new DataType[cols.length];
		for (int i = 0; i < cols.length; i++)
			mLeftSchema[i] = cols[i].type();
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] rightCols = mRightChild.next();
		DBColumn rightField = rightCols[mRightFieldNo];
		
		ArrayList< ArrayList<Object> > fieldsList = new ArrayList<>();
		for (int i = 0; i < mLeftSchema.length + rightCols.length; ++i)
			fieldsList.add(new ArrayList<Object>());
		
		for (int i = 0; i < rightField.length(); ++i) {
			LinkedList<Integer> bucket = mHashTable.get(rightField.get()[i]);
			if (bucket == null || bucket.size() < 1)
				continue;
			
			for (Integer index : bucket) {
				for (int j = 0; j < mLeftSchema.length; ++j)
					fieldsList.get(j).add(left[j].get()[index]);

				for (int j = left.length; j < joinedColumns.size(); ++j)
					fieldsList.get(j).add(right[j - left.length].get()[i]);
			}
		}
	}

	@Override
	public void close() {
		mLeftChild.close();
		mRightChild.close();
		mHashTable.clear();
	}
}
