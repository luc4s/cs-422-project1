package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.DBColumn;

public class Project implements BlockOperator {

	private final BlockOperator mChild;
	private final int[] mColumns;

	public Project(BlockOperator child, int[] columns) {
		if (child == null || columns == null)
			throw new NullPointerException();
		if (columns.length < 1)
			throw new IllegalArgumentException("PROJECT: Must have at lease one column.");
		
		mChild = child;
		mColumns = columns;
	}

	public DBColumn[] execute() {
		DBColumn[] cols = mChild.execute();
		DBColumn[] filteredCols = new DBColumn[mColumns.length];
		for (int i = 0; i < filteredCols.length; ++i)
			filteredCols[i] = cols[mColumns[i]];
		
		return filteredCols;
	}
}
