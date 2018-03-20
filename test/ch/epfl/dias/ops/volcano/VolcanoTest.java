package ch.epfl.dias.ops.volcano;

import static org.junit.Assert.*;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

import org.junit.Before;
import org.junit.Test;

public class VolcanoTest {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    DataType[] schema;
    
    RowStore rowstoreData;
    RowStore rowstoreOrder;
    RowStore rowstoreLineItem;
    
    @Before
    public void init()  {
    	
		schema = new DataType[]{ 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT,
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT };
    	
        orderSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.STRING,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.INT,
                DataType.STRING};

        lineitemSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING};
        
        rowstoreData = new RowStore(schema, "input/data.csv", ",");
        rowstoreData.load();
        
        rowstoreOrder = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
        rowstoreOrder.load();
        
        rowstoreLineItem = new RowStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
        rowstoreLineItem.load();        
    }
    
    @Test
    public void scanTest() {
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
	    scan.open();

	    for (int i = 0; i < 10; ++i) {
	    	DBTuple tuple = scan.next();
	    	assertEquals(tuple.getFieldAsInt(0).intValue(), i + 1);
	    	
	    	for (int j = 5; j < 11; ++j)
	    		assertEquals(tuple.getFieldAsInt(j - 1).intValue(), j);
	    }
	    assertTrue(scan.next().eof);
    }
    
	@Test
	public void spTestData(){
	    /* SELECT COUNT(*) FROM data WHERE col4 == 6 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 3, 6);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 3);
	}
	
	@Test
	public void spTestOrder(){
	    /* SELECT COUNT(*) FROM data WHERE col0 == 6 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 6);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 1);
	}
	
	@Test
	public void spTestLineItem(){
	    /* SELECT COUNT(*) FROM data WHERE col0 == 3 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 3);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 3);
	}

	@Test
	public void joinTest1(){
	    /* SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey) WHERE orderkey = 3;*/
	
		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	
	    /*Filtering on both sides */
	    Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
	    Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);
	
	    HashJoin join = new HashJoin(selOrder,selLineitem,0,0);
	    ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
	
	    agg.open();
	    //This query should return only one result
	    DBTuple result = agg.next();
	    int output = result.getFieldAsInt(0);
	    assertTrue(output == 3);
		assertTrue(agg.next().eof);
	}
	
	@Test
	public void joinTest2(){
	    /* SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey) WHERE orderkey = 3;*/
	
		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	
	    /*Filtering on both sides */
	    Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
	    Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);
	
	    HashJoin join = new HashJoin(selLineitem,selOrder,0,0);
	    ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
	
	    agg.open();
	    //This query should return only one result
	    DBTuple result = agg.next();
	    int output = result.getFieldAsInt(0);
	    assertEquals(3, output);
		assertTrue(agg.next().eof);
	}
	
	@Test
	public void testMin() {
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.MIN, DataType.INT, 0);
	    
	    agg.open();
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertEquals(1, output);
		assertTrue(agg.next().eof);
	}

	@Test
	public void testMax() {
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.MAX, DataType.INT, 0);
	    
	    agg.open();
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertEquals(10, output);
		assertTrue(agg.next().eof);
	}

	@Test
	public void testAverage() {
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.AVG, DataType.DOUBLE, 1);
	    
	    agg.open();
		DBTuple result = agg.next();
		double output = result.getFieldAsDouble(0);
		assertEquals(2.0, output, 0.0);
		assertTrue(agg.next().eof);
	}

	@Test
	public void testSum() {
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.SUM, DataType.INT, 0);
	    
	    agg.open();
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertEquals(55, output);
		assertTrue(agg.next().eof);
	}

	@Test
	public void customTest(){  
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.LE, 0, 4);

	    ch.epfl.dias.ops.volcano.Scan scan2 = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
	    ch.epfl.dias.ops.volcano.Select sel2 = new ch.epfl.dias.ops.volcano.Select(scan2, BinaryOp.GT, 0, 8);
	    

	    HashJoin join = new HashJoin(sel, sel2, 1, 1);
	    ProjectAggregate agg = new ProjectAggregate(join, Aggregate.COUNT, DataType.INT,0);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertEquals(8, output);
		assertTrue(agg.next().eof);
	}
}
