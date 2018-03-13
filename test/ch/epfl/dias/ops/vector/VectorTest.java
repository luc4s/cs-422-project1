package ch.epfl.dias.ops.vector;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;


public class VectorTest {

	DataType[] orderSchema;
	DataType[] lineitemSchema;
	DataType[] schema;

	ColumnStore columnstoreData;
	ColumnStore columnstoreOrder;
	ColumnStore columnstoreLineItem;

	@Before
	public void setUp() throws Exception {
		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
				DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

		columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		columnstoreData.load();

		columnstoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|");
		columnstoreOrder.load();

		columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
		columnstoreLineItem.load();
	}

	@Test
	public void spTestData() {
		/* SELECT COUNT(*) FROM data WHERE col4 == 6 */
		Scan scan = new Scan(columnstoreData, 10);
		Select sel = new Select(scan, BinaryOp.EQ, 3, 6);
		ProjectAggregate agg = new ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

	@Test
	public void spTestOrder() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 6 */
		Scan scan = new Scan(columnstoreOrder, 10);
		Select sel = new Select(scan, BinaryOp.EQ, 0, 6);
		ProjectAggregate agg = new ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 1);
	}

	@Test
	public void spTestLineItem() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
		Scan scan = new Scan(columnstoreLineItem, 10);
		Select sel = new Select(scan, BinaryOp.EQ, 0, 3);
		ProjectAggregate agg = new ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

	@Test
	public void joinTest1() {
		/*
		 * SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */

		Scan scanOrder = new Scan(columnstoreOrder, 10);
		Scan scanLineitem = new Scan(columnstoreLineItem, 10);

		/* Filtering on both sides */
		Select selOrder = new Select(scanOrder, BinaryOp.EQ, 0, 3);
		Select selLineitem = new Select(scanLineitem, BinaryOp.EQ, 0, 3);

		Join join = new Join(selOrder, selLineitem, 0, 0);
		ProjectAggregate agg = new ProjectAggregate(join, Aggregate.COUNT,
				DataType.INT, 0);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

	@Test
	public void joinTest2() {
		/*
		 * SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */

		Scan scanOrder = new Scan(columnstoreOrder, 10);
		Scan scanLineitem = new Scan(columnstoreLineItem, 10);

		/* Filtering on both sides */
		Select selOrder = new Select(scanOrder, BinaryOp.EQ, 0, 3);
		Select selLineitem = new Select(scanLineitem, BinaryOp.EQ, 0, 3);

		Join join = new Join(selLineitem, selOrder, 0, 0);
		ProjectAggregate agg = new ProjectAggregate(join, Aggregate.COUNT,
				DataType.INT, 0);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

}
