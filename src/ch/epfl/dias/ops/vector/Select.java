package ch.epfl.dias.ops.vector;

import java.util.ArrayList;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements VectorOperator {

	private final VectorOperator mChild;
	private final BinaryOp mOp;
	private final int mFieldNo;
	private final int mValue;

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		if (child == null || op == null)
			throw new NullPointerException();
		
		if (fieldNo < 0)
			throw new IllegalArgumentException("SELECT: field number must be positive.");
		
		mChild = child;
		mFieldNo = fieldNo;
		mValue = value;
		mOp = op;
	}
	
	@Override
	public void open() {
		mChild.open();
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] cols = mChild.next();
		if (cols.length == 0)
			return cols;

		if (cols[mFieldNo].type() != DataType.INT)
			throw new UnsupportedOperationException("SELECT: Can only perform on Integer fields");

		Integer[] values = cols[mFieldNo].getAsInteger();
		ArrayList< ArrayList<Object> > filtered = new ArrayList<>();
		for (int i = 0; i < cols.length; ++i)
			filtered.add(new ArrayList<>());

		for (int i = 0; i < cols[mFieldNo].length();++i) {
			final int value = values[i];
			boolean result = false;
			switch (mOp) {
				case LT: result = value < mValue;  break;
				case LE: result = value <= mValue; break;
				case EQ: result = value == mValue; break;
				case NE: result = value != mValue; break;
				case GE: result = value >= mValue; break;
				case GT: result = value > mValue;  break;
				default:
					throw new RuntimeException("SELECT: Unsupported operator");
			}
			if (result) {
				for (int j = 0; j < filtered.size(); ++j)
					filtered.get(j).add(cols[j].get()[i]);
			}
		}
		
		DBColumn[] filteredCols = new DBColumn[cols.length];
		for (int i = 0; i < filtered.size(); ++i)
			filteredCols[i] = new DBColumn(filtered.get(i).toArray(), cols[i].type());
		
		return filteredCols;
	}

	@Override
	public void close() {
		mChild.close();
	}
}
