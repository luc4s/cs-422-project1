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
		final int startIndex = (mCounter++) * mVectorSize;
		
		if (startIndex >= cols[0].length()) {
			DBColumn[] emptyVec = new DBColumn[cols.length];
			for (int i = 0; i < emptyVec.length; ++i)
				emptyVec[i] = new DBColumn(cols[i].type(), mVectorSize);
			
			return emptyVec;
		}
		
		DBColumn[] vector = new DBColumn[cols.length];
		for (int i = 0; i < cols.length; ++i) {
			int colLength = cols[i].length();
			int newLength = mVectorSize < colLength - startIndex ? mVectorSize : colLength - startIndex;
			DBColumn dest = new DBColumn(cols[i].type(), newLength);
			DBColumn src = cols[i];
			for (int j = 0; j < newLength; ++j)
				dest.append(src.get(startIndex + j));
			
			vector[i] = dest;
		}
		return vector;
	}

	@Override
	public void close() {}
}
