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

		ArrayList< ArrayList<Object> > joinedColumns = new ArrayList<>();
		for (int i = 0; i < left.length + right.length; ++i)
			joinedColumns.add(new ArrayList<Object>());

		Object[] rightCol = right[mRightFieldNo].get();
		for (int i = 0; i < rightCol.length; ++i) {
			LinkedList<Integer> bucket = hashTable.get(rightCol[i]);
			if (bucket == null || bucket.size() < 1)
				continue;
			
			for (Integer index : bucket) {
				for (int j = 0; j < left.length; ++j)
					joinedColumns.get(j).add(left[j].get()[index]);

				for (int j = left.length; j < joinedColumns.size(); ++j)
					joinedColumns.get(j).add(right[j].get()[i]);
			}
		}
		
		DBColumn[] cols = new DBColumn[left.length + right.length];
		for (int i = 0; i < left.length; ++i)
			cols[i] = new DBColumn(joinedColumns.get(i).toArray(), left[i].type());

		for (int i = 0; i < right.length; ++i)
			cols[left.length + i] = new DBColumn(joinedColumns.get(left.length + i).toArray(), right[i].type());
		
		return cols;
	}
}
