package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class DBPAXpage {
	private final DataType[] mSchema;
	private final DBColumn[] mMinipages;

	public DBPAXpage(DBColumn[] columns, DataType[] schema) {
		if (columns == null || schema == null)
			throw new NullPointerException();
		
		if (columns.length != schema.length)
			throw new IllegalArgumentException("Schema does not match with number of fields");

		mMinipages = columns;
		mSchema = schema;
	}
	
	public DBColumn getColumn(int index) {
		if (index < 0 || index >= mMinipages.length)
			throw new IllegalArgumentException();

		return mMinipages[index];
	}
	
	public DBTuple getRow(int index) {
		if (index >= mMinipages[0].length())
			return new DBTuple();
			
		Object[] fields = new Object[mMinipages.length];
		for (int i = 0; i < fields.length; ++i) {
			Object value = null;
			switch (mSchema[i]) {
				case INT:
					value = mMinipages[i].getAsInteger()[index];
					break;
				case DOUBLE:
					value = mMinipages[i].getAsDouble()[index];
					break;
				case BOOLEAN:
					value = mMinipages[i].getAsBoolean()[index];
					break;
				case STRING:
					value = mMinipages[i].getAsString()[index];
					break;
			}
			fields[i] = value;	
		}

		return new DBTuple(fields, mSchema);
	}
}
