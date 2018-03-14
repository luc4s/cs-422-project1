package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VectorOperator {

	private final VectorOperator mChild;
	private final int[] mFieldsNo;

	public Project(VectorOperator child, int[] fieldsNo) {
		if (child == null || fieldsNo == null)
			throw new NullPointerException();
		
		if (fieldsNo.length < 1)
			throw new IllegalArgumentException("PROJECT: Must have at least one field");
		
		for (Integer i : fieldsNo) {
			if (i.intValue() < 0)
				throw new IllegalArgumentException("PROJECT: Field number must be positive.");
		}
		
		mChild = child;
		mFieldsNo = fieldsNo;
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

		DBColumn[] selected = new DBColumn[mFieldsNo.length];
		for (int i = 0; i < selected.length; i++)
			selected[i] = cols[mFieldsNo[i]];

		return selected;
	}

	@Override
	public void close() {
		mChild.close();
	}
}
