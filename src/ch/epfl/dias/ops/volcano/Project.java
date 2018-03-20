package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	private final VolcanoOperator mChild;
	private final int[] mFieldNo;

	public Project(VolcanoOperator child, int[] fieldNo) {
		if (fieldNo == null)
			throw new NullPointerException("PROJECT: Null fields");

		if (fieldNo.length == 0)
			throw new IllegalArgumentException("PROJECT: operator should have at least one field.");

		mChild = child;
		mFieldNo = fieldNo;
	}

	@Override
	public void open() {
		mChild.open();
	}

	@Override
	public DBTuple next() {
		DBTuple tuple = mChild.next();
		if (tuple.eof)
			return tuple;

		Object[] fields = new Object[mFieldNo.length];
		for (int i = 0; i < mFieldNo.length; ++i)
			fields[i] = tuple.fields[i];
		
		return new DBTuple(fields, tuple.types);	
	}

	@Override
	public void close() {
		mChild.close();
	}
}
