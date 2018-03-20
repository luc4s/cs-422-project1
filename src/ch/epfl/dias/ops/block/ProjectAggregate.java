package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class ProjectAggregate implements BlockOperator {

	private final BlockOperator mChild;
	private final Aggregate mOp;
	private final DataType mType;
	private final int mFieldNo;
	
	public ProjectAggregate(BlockOperator child, Aggregate agg, DataType dt, int fieldNo) {
		if (child == null || agg == null || agg == null)
			throw new NullPointerException();
		
		if (fieldNo < 0)
			throw new IllegalArgumentException("PROJECT-AGGREGATE: Field number must be positive.");
		
		mChild = child;
		mOp = agg;
		mType = dt;
		mFieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute() {
		double value = 0.0;
		DBColumn[] cols = mChild.execute();
		if (mFieldNo > cols.length)
			throw new RuntimeException("PROJECT-AGGREGATE: Field number exceeds columns count");
		
		if (cols[0].length() == 0)
			return mOp == Aggregate.COUNT ?  new DBColumn[] { new DBColumn(new Object[] { 0 }, mType) } : cols;
		
		DBColumn col = cols[mFieldNo];
	
		switch (mOp) {
			case AVG: 
				if (col.type() == DataType.INT)
					value = col.intStream().average().getAsDouble();
				else if(col.type() == DataType.DOUBLE)
					value = col.doubleStream().average().getAsDouble();
				else
					throw new RuntimeException("PROJECT-AGGREGATE: Cannot compute average of type " + col.type());
				break;
			case COUNT:
				value = col.length();
				break;
			case MIN:
				if (col.type() == DataType.INT)
					value = col.intStream().min().getAsInt();
				else if(col.type() == DataType.DOUBLE)
					value = col.doubleStream().min().getAsDouble();
				else
					throw new RuntimeException("PROJECT-AGGREGATE: Cannot compute minimum of type " + col.type());
				break;
			case MAX:
				if (col.type() == DataType.INT)
					value = col.intStream().max().getAsInt();
				else if(col.type() == DataType.DOUBLE)
					value = col.doubleStream().max().getAsDouble();
				else
					throw new RuntimeException("PROJECT-AGGREGATE: Cannot compute maximum of type " + col.type());
				break;
			case SUM:
				if (col.type() == DataType.INT)
					value = col.intStream().sum();
				else if(col.type() == DataType.DOUBLE)
					value = col.doubleStream().sum();
				else
					throw new RuntimeException("PROJECT-AGGREGATE: Cannot compute sum of type " + col.type());
				break;
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
}
