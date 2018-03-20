package ch.epfl.dias.ops.vector;

import java.util.LinkedList;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements VectorOperator {

	private final VectorOperator mChild;
	private final BinaryOp mOp;
	private final int mFieldNo;
	private final int mValue;
	
	private LinkedList<DBColumn[]> mBuffer;

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		if (child == null || op == null)
			throw new NullPointerException();
		
		if (fieldNo < 0)
			throw new IllegalArgumentException("SELECT: field number must be positive.");
		
		mChild = child;
		mFieldNo = fieldNo;
		mValue = value;
		mOp = op;
		mBuffer = new LinkedList<>();
	}
	
	@Override
	public void open() {
		mChild.open();
	}

	@Override
	public DBColumn[] next() {
		if (!mBuffer.isEmpty())
			return mBuffer.removeFirst();

		DBColumn[] cols = mChild.next();
		int counter = 0;
		int vecSize = cols[0].length();
		
		if (vecSize == 0)
			return cols;

		if (cols[mFieldNo].type() != DataType.INT)
			throw new UnsupportedOperationException("SELECT: Can only perform on Integer fields");

		DBColumn[] filtered = new DBColumn[cols.length];
		for (int i = 0; i < cols.length; ++i)
			filtered[i] = new DBColumn(cols[i].type(), cols[0].length());

		while (counter < vecSize && cols[0].length() > 0) {
	
			for (int i = 0; i < cols[mFieldNo].length(); ++i) {
				final int value = (Integer)cols[mFieldNo].get(i);
				boolean result = false;
				switch (mOp) {
					case LT: result = value < mValue;  break;
					case LE: result = value <= mValue; break;				
					case EQ: result = value == mValue; break;
					case NE: result = value != mValue; break;
					case GT: result = value > mValue;  break;
					case GE: result = value >= mValue; break;
					default:
						throw new RuntimeException("SELECT: Unsupported operator");
				}
				if (result) {
					counter++;
					for (int j = 0; j < cols.length; ++j)
						filtered[j].append(cols[j].get(i));
				}
				
				if (counter >= vecSize) {
					mBuffer.addLast(filtered);

					filtered = new DBColumn[cols.length];
					for (int j = 0; j < cols.length; ++j)
						filtered[j] = new DBColumn(cols[j].type(), cols[0].length());
				}
			}

			if (counter < vecSize)
				cols = mChild.next();
		}
		

		if (!mBuffer.isEmpty()) {
			if (filtered[0].length() > 0)
				mBuffer.addLast(filtered);

			return mBuffer.removeFirst();
		}
		else
			return filtered;
	}

	@Override
	public void close() {
		mChild.close();
		mBuffer.clear();
	}
}
