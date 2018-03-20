package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.LinkedList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Join implements BlockOperator {

	private final BlockOperator mLeftChild;
	private final BlockOperator mRightChild;
	private int mLeftFieldNo;
	private int mRightFieldNo;

	public Join(BlockOperator leftChild, BlockOperator rightChild, int leftFieldNo, int rightFieldNo) {
		if (leftChild == null || rightChild == null)
			throw new NullPointerException();

		if (leftFieldNo < 0 || rightFieldNo < 0)
			throw new IllegalArgumentException("JOIN: Field number must be positive.");

		mLeftChild = leftChild;
		mRightChild = rightChild;
		mLeftFieldNo = leftFieldNo;
		mRightFieldNo = rightFieldNo;
	}

	public DBColumn[] execute() {
		DBColumn[] left = mLeftChild.execute();
		DBColumn[] right = mRightChild.execute();
		
		HashMap<Object, LinkedList<Integer> > hashTable = new HashMap<>();
		Object[] leftCol = left[mLeftFieldNo].get();
		for (int i = 0; i < leftCol.length; ++i) {
			LinkedList<Integer> bucket = hashTable.get(leftCol[i]);
			if (bucket == null) {
				bucket = new LinkedList<>();
				hashTable.put(leftCol[i], bucket);
			}
			bucket.addLast(i);
		}

		DBColumn[] joinedColumns = new DBColumn[left.length + right.length];
		for (int i = 0; i < left.length; ++i)
			joinedColumns[i] = new DBColumn(left[i].type());
		for (int i = 0; i < right.length; ++i)
			joinedColumns[left.length + i] = new DBColumn(right[i].type());

		DBColumn rightCol = right[mRightFieldNo];
		for (int i = 0; i < rightCol.length(); ++i) {
			LinkedList<Integer> bucket = hashTable.get(rightCol.get(i));
			if (bucket == null || bucket.isEmpty())
				continue;
			
			for (Integer index : bucket) {
				for (int j = 0; j < left.length; ++j)
					joinedColumns[j].append(left[j].get(index));

				for (int j = left.length; j < joinedColumns.length; ++j)
					joinedColumns[j].append(right[j - left.length].get(i));
			}
		}
		
		return joinedColumns;
	}
}
