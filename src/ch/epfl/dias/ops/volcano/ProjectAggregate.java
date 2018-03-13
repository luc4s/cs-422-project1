package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VolcanoOperator {

	private final VolcanoOperator mChild;
	private final Aggregate mOp;
	private final DataType mType;
	private final int mFieldNo;

	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
		if (child == null || agg == null || dt == null)
			throw new NullPointerException("PROJECT-AGGREGATE: Null parameter");
	
		if (fieldNo < 0)
			throw new IllegalArgumentException("PROJECT-AGGREGATE: Negative field index");

		mChild = child;
		mOp = agg;
		mType = dt;
		mFieldNo = fieldNo;
	}

	@Override
	public void open() {
		mChild.open();
	}
	

	@Override
	public DBTuple next() {
		double value = 0.0;
		DBTuple tuple = mChild.next();
		int counter = 0;
		while (!tuple.eof) {
			switch (mOp) {
				case AVG:
					// Incremental average
					value = value + (getFieldValue(tuple) - value) / ++counter;
					tuple = mChild.next();
					break;
				case COUNT:
					value = ++counter;
					break;
				case MIN:
					final double minValue = getFieldValue(tuple);
					if (counter == 0 || minValue < value)
						value = minValue;

					break;
				case MAX:
					final double maxValue = getFieldValue(tuple);
					if (counter == 0 || maxValue > value)
						value = maxValue;

					break;
				case SUM:
					value += getFieldValue(tuple);
					break;
				default:
					throw new RuntimeException("PROJECT-AGGREGATE: Unrecognized aggregate operator");
			}
		}
		
		Object tupleValue = null;
		switch (mType) {
			case INT: 		tupleValue = Integer.valueOf((int) value); 		break;
			case DOUBLE:	tupleValue = Double.valueOf(value); 			break;
			case STRING:	tupleValue = Double.toString(value);        	break;
			case BOOLEAN:	tupleValue = Boolean.valueOf(value > 0);	 	break;
		}
		return new DBTuple(new Object[] { tupleValue }, new DataType[] { mType });
	}

	@Override
	public void close() {
		mChild.close();
	}

	private double getFieldValue(DBTuple tuple) {
		switch (tuple.types[mFieldNo]) {
			case INT:
				return tuple.getFieldAsInt(mFieldNo);
			case DOUBLE:
				return tuple.getFieldAsDouble(mFieldNo);
			case BOOLEAN:
			case STRING:
				throw new RuntimeException("Cannot compute average on type " + tuple.types[mFieldNo].toString());
		}
		return 0;
	}
}
