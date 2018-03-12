package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	private final Store mStore;
	private int mCounter;

	public Scan(Store store) {
		if (store == null)
			throw new NullPointerException();

		mCounter = 0;
		mStore = store;
	}

	@Override
	public void open() {
		mCounter = 0;
	}

	@Override
	public DBTuple next() {
		return mStore.getRow(mCounter++);
	}

	@Override
	public void close() {}
}