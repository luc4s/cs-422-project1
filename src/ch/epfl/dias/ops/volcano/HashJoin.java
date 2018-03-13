package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.util.LinkedList;
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
	
	private HashMap<Object, LinkedList<DBTuple>> mHashTable;
	private LinkedList<DBTuple> mBuffer;

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
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

		DBTuple tuple = mLeftChild.next();
		while (!tuple.eof) {
			final Object key = tuple.fields[mLeftFieldNo];
			LinkedList<DBTuple> bucket = mHashTable.get(key);
			if (bucket == null) {
				bucket = new LinkedList<>();
				bucket.add(tuple);
				mHashTable.put(key, bucket);
			} else
				bucket.add(tuple);

			tuple = mLeftChild.next();
		}
	}

	@Override
	public DBTuple next() {
		DBTuple right = mRightChild.next();
		while (!right.eof) {
			LinkedList<DBTuple> bucket = mHashTable.get(right.fields[mRightFieldNo]);
			if (bucket != null) {
				for (DBTuple left : bucket) {
					if (compareFields(left, right)) {
						Object[] fieldList = new Object[left.fields.length + right.fields.length];
						System.arraycopy(left.fields, 0, fieldList, 0, left.fields.length);
						System.arraycopy(right.fields, 0, fieldList, left.fields.length, right.fields.length);
						
						DataType[] typeList = new DataType[left.types.length + right.types.length];
						System.arraycopy(left.types, 0, typeList, 0, left.types.length);
						System.arraycopy(right.types, 0, typeList, left.types.length, right.types.length);
						
						mBuffer.addLast(new DBTuple(fieldList, typeList));
					}
				}
			}
			right = mRightChild.next();
		}

		if (mBuffer.size() > 0)
			return mBuffer.removeFirst();
		else
			return right; // Return EOF
	}

	@Override
	public void close() {
		mHashTable.clear();
		mBuffer.clear();
		mLeftChild.close();
		mRightChild.close();
	}
	
	private boolean compareFields(DBTuple left, DBTuple right) {
		switch (left.types[mLeftFieldNo]) {
		case INT:
			if (right.types[mRightFieldNo] != DataType.INT)
				throw new RuntimeException("HASH-JOIN: Join on fields with different types (Integer & " + right.types[mRightFieldNo].toString());
			
			return left.getFieldAsInt(mLeftFieldNo) == right.getFieldAsInt(mRightFieldNo);
		case DOUBLE:
			if (right.types[mRightFieldNo] != DataType.DOUBLE)
				throw new RuntimeException("HASH-JOIN: Join on fields with different types (Double & " + right.types[mRightFieldNo].toString());
			
			return left.getFieldAsDouble(mLeftFieldNo) == right.getFieldAsDouble(mRightFieldNo);
		case BOOLEAN:
			if (right.types[mRightFieldNo] != DataType.BOOLEAN)
				throw new RuntimeException("HASH-JOIN: Join on fields with different types (Boolean & " + right.types[mRightFieldNo].toString());
			
			return left.getFieldAsBoolean(mLeftFieldNo) == right.getFieldAsBoolean(mRightFieldNo);
		case STRING:
			if (right.types[mRightFieldNo] != DataType.INT)
				throw new RuntimeException("HASH-JOIN: Join on fields with different types (String & " + right.types[mRightFieldNo].toString());
			
			return left.getFieldAsString(mLeftFieldNo).equals(right.getFieldAsString(mRightFieldNo));
		default:
			throw new RuntimeException("HASH-JOIN: Undefined type");
		}
	}
}
