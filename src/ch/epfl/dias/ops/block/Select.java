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
		
		ArrayList< ArrayList<Object> > filtered = new ArrayList<>();
		for (int i = 0; i < cols.length; ++i)
			filtered.add(new ArrayList<>());
		
		for (int i = 0; i < cols[0].length(); ++i) {
			final int value = cols[mFieldNo].getAsInteger()[i].intValue();
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
				for (int j = 0; j < filtered.size(); ++j)
					filtered.get(j).add(cols[j].get()[i]);
			}
		}
		
		DBColumn[] filteredCols = new DBColumn[cols.length];
		for (int i = 0; i < filteredCols.length; ++i)
			filteredCols[i] = new DBColumn(filtered.get(i).toArray(), cols[i].type());

		return filteredCols;
	}
}
