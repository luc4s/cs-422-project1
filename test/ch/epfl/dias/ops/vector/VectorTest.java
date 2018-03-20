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
	public void testScan() {
		Scan scan = new Scan(columnstoreData, 2);

		scan.open();
		for (int i = 0; i < 5; i++)
			scan.next();

		assertEquals(0, scan.next()[0].length());
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

		assertEquals(3, output);
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

		assertEquals(3, output);
	}
	
	@Test
	public void testMin() {
		Scan scan = new Scan(columnstoreData, 10);
		ProjectAggregate agg = new ProjectAggregate(scan, Aggregate.MIN, DataType.INT, 0);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertEquals(1, output);
		assertEquals(0, agg.next()[0].length());
	}
	
	@Test
	public void testMax() {
		Scan scan = new Scan(columnstoreData, 10);
		ProjectAggregate agg = new ProjectAggregate(scan, Aggregate.MAX, DataType.INT, 0);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertEquals(10, output);
		assertEquals(0, agg.next()[0].length());
	}
	
	@Test
	public void testAverage() {
		Scan scan = new Scan(columnstoreData, 10);
		ProjectAggregate agg = new ProjectAggregate(scan, Aggregate.AVG, DataType.DOUBLE, 1);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		double output = result[0].getAsDouble()[0];

		assertEquals(2.0, output, 0.0);
		assertEquals(0, agg.next()[0].length());
	}
	
	@Test
	public void testSum() {
		Scan scan = new Scan(columnstoreData, 10);
		ProjectAggregate agg = new ProjectAggregate(scan, Aggregate.SUM, DataType.STRING, 0);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		String output = result[0].getAsString()[0];

		assertEquals("55.0", output);
		assertEquals(0, agg.next()[0].length());
	}
	
	@Test
	public void customTest() {
		Scan scanOrder = new Scan(columnstoreData, 10);
		Scan scanLineitem = new Scan(columnstoreData, 10);

		/* Filtering on both sides */
		Select selOrder = new Select(scanOrder, BinaryOp.GT, 0, 0);
		Select selLineitem = new Select(scanLineitem, BinaryOp.LE, 0, 10);

		Join join = new Join(selLineitem, selOrder, 1, 1);
		ProjectAggregate agg = new ProjectAggregate(join, Aggregate.COUNT,
				DataType.INT, 0);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertEquals(100, output);
	}
}
