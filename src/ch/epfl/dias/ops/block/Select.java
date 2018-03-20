package ch.epfl.dias.ops.block;

import java.util.ArrayList;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements BlockOperator {

	private final BlockOperator mChild;
	private final BinaryOp mOp;
	private final int mFieldNo;
	private final int mValue;

	public Select(BlockOperator child, BinaryOp op, int fieldNo, int value) {
		if (child == null || op == null)
			throw new NullPointerException();
		
		if (fieldNo < 0)
			throw new IllegalArgumentException();

		mChild = child;
		mOp = op;
		mFieldNo = fieldNo;
		mValue = value;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] cols = mChild.execute();
		if (cols[mFieldNo].type() != DataType.INT)
			throw new UnsupportedOperationException("SELECT: Only Integer comparison is supported (provided: " + cols[mFieldNo].type().toString() + ")");

		DBColumn[] filtered = new DBColumn[cols.length];
		for (int i = 0; i < cols.length; ++i)
			filtered[i] = new DBColumn(cols[i].type(), cols[0].length());
		
		for (int i = 0; i < cols[0].length(); ++i) {
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
					throw new RuntimeException("SELECT: Unsupported binary operator.");
			}
			if (result) {
				for (int j = 0; j < filtered.length; ++j)
					filtered[j].append(cols[j].get(i));
			}
		}
		
		return filtered;
	}
}
