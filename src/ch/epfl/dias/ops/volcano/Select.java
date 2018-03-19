package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	private final VolcanoOperator mChild;
	private final BinaryOp mOp;
	private final int mFieldNo;
	private final int mValue;

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		if (child == null)
			throw new NullPointerException("SELECT: Null child operator");

		mChild = child;
		mOp = op;
		mFieldNo = fieldNo;
		mValue = value;
	}

	@Override
	public void open() {
		mChild.open();
	}

	@Override
	public DBTuple next() {
		DBTuple tuple = mChild.next();
		while (!tuple.eof) {
			if (mFieldNo >= tuple.fields.length)
				throw new RuntimeException("SELECT: Invalid field index. Expected < " + tuple.fields.length + ", received " + mFieldNo);
				
			if (tuple.types[mFieldNo] != DataType.INT)
				throw new RuntimeException("SELECT: Cannot compare Integer with " + tuple.types[mFieldNo].toString());

			Integer value = tuple.getFieldAsInt(mFieldNo);
			boolean result = false;
			switch (mOp) {
				case LT: result = value < mValue;  break;
				case LE: result = value <= mValue; break;				
				case EQ: result = value == mValue; break;
				case NE: result = value != mValue; break;
				case GT: result = value > mValue;  break;
				case GE: result = value >= mValue; break;
				default:
					throw new RuntimeException("SELECT: Unrecognized operator");
			}
			if (result)
				return tuple;
			else
				tuple = mChild.next();
		}
		return tuple;
	}

	@Override
	public void close() {
		mChild.close();
	}
}
