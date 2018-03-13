package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements BlockOperator {

	private final ColumnStore mStore;

	public Scan(ColumnStore store) {
		if (store == null)
			throw new NullPointerException();

		mStore = store;
	}

	@Override
	public DBColumn[] execute() {
		int[] columnsToGet = new int[mStore.columnsCount()];
		for (int i = 0; i < columnsToGet.length; ++i)
			columnsToGet[i] = i;

		return mStore.getColumns(columnsToGet);
	}
}
