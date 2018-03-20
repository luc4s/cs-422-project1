package ch.epfl.dias.ops.vector;

import java.util.Arrays;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class ProjectAggregate implements VectorOperator {

	private final VectorOperator mChild;
	private final Aggregate mOp;
	private final DataType mType;
	private final int mFieldNo;

	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
		if (child == null || agg == null || dt == null)
			throw new NullPointerException();
		
		if (fieldNo < 0)
			throw new IllegalArgumentException("PROJECT-AGGREGATE: Field number must be positive.");
		
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
	public DBColumn[] next() {
		double value = 0.0;
		int counter = 0;
		DBColumn[] cols = mChild.next();
		if (cols[0].length() == 0)
			return mOp == Aggregate.COUNT ? new DBColumn[] { new DBColumn(new Object[] { 0 }, mType) } : cols;

		if (mFieldNo > cols.length)
			throw new RuntimeException("PROJECT-AGGREGATE: Field number exceeds columns count");
		
		while (cols[0].length() > 0) {
			DBColumn col = cols[mFieldNo];
			double temp = Double.NaN;
			switch (mOp) {
				case AVG: 
					if (col.type() == DataType.INT)
						value += (Arrays.stream(col.getAsInteger()).mapToInt(i -> i.intValue()).average().getAsDouble() - value) / ++counter;
					else if(col.type() == DataType.DOUBLE)
						value += (Arrays.stream(col.getAsDouble()).mapToDouble(d -> d.doubleValue()).average().getAsDouble() - value) / ++counter;
					else
						throw new RuntimeException("PROJECT-AGGREGATE: Cannot compute average of type " + col.type());
					break;
				case COUNT:
					value += col.length();
					break;
				case MIN:
					if (col.type() == DataType.INT)
						temp = Arrays.stream(col.getAsInteger()).mapToInt(i -> i.intValue()).min().getAsInt();
					else if(col.type() == DataType.DOUBLE)
						temp = Arrays.stream(col.getAsInteger()).mapToDouble(d -> d.doubleValue()).min().getAsDouble();
					else
						throw new RuntimeException("PROJECT-AGGREGATE: Cannot compute minimum of type " + col.type());
	
					if (temp < value || counter++ == 0)
						value = temp;
					break;
				case MAX:
					if (col.type() == DataType.INT)
						temp = Arrays.stream(col.getAsInteger()).mapToInt(i -> i.intValue()).max().getAsInt();
					else if(col.type() == DataType.DOUBLE)
						temp = Arrays.stream(col.getAsInteger()).mapToDouble(d -> d.doubleValue()).max().getAsDouble();
					else
						throw new RuntimeException("PROJECT-AGGREGATE: Cannot compute maximum of type " + col.type());
	
					if (temp > value || counter++ == 0)
						value = temp;
					break;
				case SUM:
					if (col.type() == DataType.INT)
						value += Arrays.stream(col.getAsInteger()).mapToInt(i -> i.intValue()).sum();
					else if(col.type() == DataType.DOUBLE)
						value += Arrays.stream(col.getAsInteger()).mapToDouble(d -> d.doubleValue()).sum();
					else
						throw new RuntimeException("PROJECT-AGGREGATE: Cannot compute sum of type " + col.type());
					break;
			}
			cols = mChild.next();
		}
		Object tupleValue = null;
		switch (mType) {
			case INT: 		tupleValue = Integer.valueOf((int) value); 		break;
			case DOUBLE:	tupleValue = Double.valueOf(value); 			break;
			case STRING:	tupleValue = Double.toString(value);        	break;
			case BOOLEAN:	tupleValue = Boolean.valueOf(value != 0);	 	break;
		}

		return new DBColumn[] { new DBColumn(new Object[] { tupleValue }, mType) };
	}

	@Override
	public void close() {
		mChild.close();
	}

}
