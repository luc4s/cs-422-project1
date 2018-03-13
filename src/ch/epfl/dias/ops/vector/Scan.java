package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements VectorOperator {

	private final Store mStore;
	private final int mVectorSize;
	private int mCounter;

	public Scan(Store store, int vectorSize) {
		if (store == null)
			throw new NullPointerException();
		
		mStore = store;
		mVectorSize = vectorSize;
	}
	
	@Override
	public void open() {
		mCounter = 0;
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] cols = mStore.getColumns(null);
		final int startIndex = mCounter++ * mVectorSize;
		
		DBColumn[] vector = new DBColumn[cols.length];
		for (int i = 0; i < cols.length; ++i) {
			final int colLength = cols[i].length();
			Object[] objects = new Object[mVectorSize < colLength ? mVectorSize : colLength];
			for (int j = startIndex; j < mVectorSize && j < colLength; ++j)
				objects[j] = cols[i].get()[j];
			
			vector[i] = new DBColumn(objects, cols[i].type());
		}
		return vector;
	}

	@Override
	public void close() {}
}
