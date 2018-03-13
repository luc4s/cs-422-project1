package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class HashJoin implements VolcanoOperator {

	private final VolcanoOperator mLeftChild;
	private final VolcanoOperator mRightChild;
	private final int mLeftFieldNo;
	private final int mRightFieldNo;
	
	private HashMap<Object, DBTuple> mHashTable;
	private boolean mLeftRelationHashed;

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		mLeftChild = leftChild;
		mRightChild = rightChild;
		mLeftFieldNo = leftFieldNo;
		mRightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		mLeftChild.open();
		mRightChild.open();
		
		HashMap<Object, DBTuple>  	leftHashTable = new HashMap<>(),
									rightHashTable = new HashMap<>();

		DBTuple tuple = mLeftChild.next();
		while(!tuple.eof)
			leftHashTable.put(tuple.fields[mLeftFieldNo], tuple);
		
		tuple = mRightChild.next();
		while(!tuple.eof)
			rightHashTable.put(tuple.fields[mRightFieldNo], tuple);
		
		if (leftHashTable.size() < rightHashTable.size()) {
			mHashTable = leftHashTable;
			mLeftRelationHashed = true;
		} else {
			mHashTable = rightHashTable;
			mLeftRelationHashed = false;
		}
	}

	@Override
	public DBTuple next() {
		DBTuple tuple = null;
		do {
			Object field;
			if (mLeftRelationHashed) {
				tuple = mRightChild.next();
				field = tuple.fields[mRightFieldNo];
			} else {
				tuple = mLeftChild.next();
				field = tuple.fields[mLeftFieldNo];
			}
			
			DBTuple hashedTuple = mHashTable.get(field);
			if (hashedTuple != null) {
				DBTuple left = mLeftRelationHashed ? hashedTuple : tuple;
				DBTuple right = mLeftRelationHashed ? tuple : hashedTuple;

				List<Object> fieldList = Arrays.asList(left.fields);
				fieldList.addAll(Arrays.asList(right.fields));
				
				List<DataType> typeList = Arrays.asList(left.types);
				typeList.addAll(Arrays.asList(right.types));
				
				return new DBTuple(fieldList.toArray(), (DataType[])typeList.toArray());
			}
		} while (!tuple.eof);
		return tuple;
	}

	@Override
	public void close() {
		mHashTable.clear();
		mLeftChild.close();
		mRightChild.close();
	}
}
