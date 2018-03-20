package ch.epfl;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

public class Main {

    public static DataType[] lineitemSchema = new DataType[]{
            DataType.INT,
            DataType.INT,
            DataType.INT,
            DataType.INT,
            DataType.INT,
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

	public static DataType[] orderSchema = new DataType[]{
	                DataType.INT,
	                DataType.INT,
	                DataType.STRING,
	                DataType.DOUBLE,
	                DataType.STRING,
	                DataType.STRING,
	                DataType.STRING,
	                DataType.INT,
	                DataType.STRING};
	
	public static DataType[] schema = new DataType[]{ 
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

	public static void main(String[] args) {


		 RowStore rowstore = new RowStore(orderSchema, "input/orders_big.csv", "\\|");
		 rowstore.load();

		 
		 ColumnStore columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		 columnstoreData.load();

//		 testDSMvector();
//		 testNSMVolcano();
//		 testPAXVolcano();
		 testDSMcolumnar();

		
		 ch.epfl.dias.ops.block.Scan blockScan = new ch.epfl.dias.ops.block.Scan(columnstoreData);
		 ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(blockScan, BinaryOp.EQ, 3, 6);
		 ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
		 DBColumn[] result = agg.execute();
		 int output = result[0].getAsInteger()[0];
	}
	
	public static void testNSMVolcano() {
		 System.out.println("_NSM BENCHMARK_");
		 RowStore orders = new RowStore(orderSchema, "input/orders_big.csv", "\\|");
		 RowStore lines = new RowStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
		 System.out.println("[NSM] Loading datasets...");
		 lines.load();
		 orders.load();
		 System.out.println("[NSM] Datasets loaded.");

		 /* QUERY 1 */
		 ch.epfl.dias.ops.volcano.Scan scanLines = new ch.epfl.dias.ops.volcano.Scan(lines);
		 ch.epfl.dias.ops.volcano.Select select = new ch.epfl.dias.ops.volcano.Select(scanLines, BinaryOp.LT, 4, 50);
		 ch.epfl.dias.ops.volcano.Project project = new ch.epfl.dias.ops.volcano.Project(select, new int[] { 0, 1, 2, 14, 15 } );
		 
		 System.out.println("SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_SHIPMODE, L_COMMENT FROM LINEITEM WHERE L_QUANTITY < 50");
		 long time = System.nanoTime();
		 project.open();
		 while (!(project.next()).eof) {}
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));
		 
		 /* QUERY 2 */
		 scanLines = new ch.epfl.dias.ops.volcano.Scan(lines);
		 ch.epfl.dias.ops.volcano.Scan scanOrders = new ch.epfl.dias.ops.volcano.Scan(lines);
		 ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(scanLines, scanOrders, 0, 0);
		 project = new ch.epfl.dias.ops.volcano.Project(join, new int[] { 4, 19 } );

		 time = System.nanoTime();
		 System.out.println("SELECT L.L_QUANTITY, O.O_TOTALPRICE FROM LINEITEM L, ORDERS O WHERE L.ORDERKEY = O.ORDERKEY");
		 project.open();
		 while (!(project.next()).eof) {}
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));
		 
		 /*  QUERY 3 */
		 scanLines = new ch.epfl.dias.ops.volcano.Scan(lines);
		 select = new ch.epfl.dias.ops.volcano.Select(scanLines, BinaryOp.LT, 4, 50);
		 ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(select, Aggregate.AVG, DataType.DOUBLE, 7);

		 time = System.nanoTime();
		 System.out.println("SELECT AVG(L_TAX) FROM LINEITEM WHERE L_QUANTITY < 50");
		 agg.open();
		 while (!(agg.next()).eof) {}
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));

		 /* QUERY 4 */
		 scanLines = new ch.epfl.dias.ops.volcano.Scan(lines);
		 scanOrders = new ch.epfl.dias.ops.volcano.Scan(lines);
		 join = new ch.epfl.dias.ops.volcano.HashJoin(scanLines, scanOrders, 0, 0);
		 agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(join, Aggregate.MAX, DataType.DOUBLE, 19);

		 time = System.nanoTime();
		 System.out.println("SELECT MAX(O.O_TOTALPRICE) FROM LINEITEM L, ORDERS O WHERE L.ORDERKEY = O.ORDERKEY");
		 agg.open();
		 while (!(agg.next()).eof) {}
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));
	}
	
	public static void testPAXVolcano() {
		 System.out.println("_PAX BENCHMARK_");
		 PAXStore orders = new PAXStore(orderSchema, "input/orders_big.csv", "\\|", 100);
		 PAXStore lines = new PAXStore(lineitemSchema, "input/lineitem_big.csv", "\\|", 100);
		 System.out.println("[PAX] Loading datasets...");
		 lines.load();
		 orders.load();
		 System.out.println("[PAX] Datasets loaded.");

		 /* QUERY 1 */
		 ch.epfl.dias.ops.volcano.Scan scanLines = new ch.epfl.dias.ops.volcano.Scan(lines);
		 ch.epfl.dias.ops.volcano.Select select = new ch.epfl.dias.ops.volcano.Select(scanLines, BinaryOp.LT, 4, 50);
		 ch.epfl.dias.ops.volcano.Project project = new ch.epfl.dias.ops.volcano.Project(select, new int[] { 0, 1, 2, 14, 15 } );
		 
		 System.out.println("SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_SHIPMODE, L_COMMENT FROM LINEITEM WHERE L_QUANTITY < 50");
		 long time = System.nanoTime();
		 project.open();
		 while (!(project.next()).eof) {}
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));
		 
		 /* QUERY 2 */
		 scanLines = new ch.epfl.dias.ops.volcano.Scan(lines);
		 ch.epfl.dias.ops.volcano.Scan scanOrders = new ch.epfl.dias.ops.volcano.Scan(lines);
		 ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(scanLines, scanOrders, 0, 0);
		 project = new ch.epfl.dias.ops.volcano.Project(join, new int[] { 4, 19 } );

		 time = System.nanoTime();
		 System.out.println("SELECT L.L_QUANTITY, O.O_TOTALPRICE FROM LINEITEM L, ORDERS O WHERE L.ORDERKEY = O.ORDERKEY");
		 project.open();
		 while (!(project.next()).eof) {}
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));
		 
		 /*  QUERY 3 */
		 scanLines = new ch.epfl.dias.ops.volcano.Scan(lines);
		 select = new ch.epfl.dias.ops.volcano.Select(scanLines, BinaryOp.LT, 4, 50);
		 ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(select, Aggregate.AVG, DataType.DOUBLE, 7);

		 time = System.nanoTime();
		 System.out.println("SELECT AVG(L_TAX) FROM LINEITEM WHERE L_QUANTITY < 50");
		 agg.open();
		 while (!(agg.next()).eof) {}
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));

		 /* QUERY 4 */
		 scanLines = new ch.epfl.dias.ops.volcano.Scan(lines);
		 scanOrders = new ch.epfl.dias.ops.volcano.Scan(lines);
		 join = new ch.epfl.dias.ops.volcano.HashJoin(scanLines, scanOrders, 0, 0);
		 agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(join, Aggregate.MAX, DataType.DOUBLE, 19);

		 time = System.nanoTime();
		 System.out.println("SELECT MAX(O.O_TOTALPRICE) FROM LINEITEM L, ORDERS O WHERE L.ORDERKEY = O.ORDERKEY");
		 agg.open();
		 while (!(agg.next()).eof) {}
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));
	}

	public static void testDSMcolumnar() {
		 System.out.println("_DSM COLUMNAR BENCHMARK_");
		 ColumnStore orders = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|");
		 ColumnStore lines = new ColumnStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
		 System.out.println("[DSM] Loading datasets...");
		 lines.load();
		 orders.load();
		 System.out.println("[DSM] Datasets loaded.");

		 /* QUERY 1 */
		 ch.epfl.dias.ops.block.Scan scanLines = new ch.epfl.dias.ops.block.Scan(lines);
		 ch.epfl.dias.ops.block.Select select = new ch.epfl.dias.ops.block.Select(scanLines, BinaryOp.LT, 4, 50);
		 ch.epfl.dias.ops.block.Project project = new ch.epfl.dias.ops.block.Project(select, new int[] { 0, 1, 2, 14, 15 } );
		 
		 System.out.println("SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_SHIPMODE, L_COMMENT FROM LINEITEM WHERE L_QUANTITY < 50");
		 long time = System.nanoTime();
		 project.execute();
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));
		 
		 /* QUERY 2 */
		 scanLines = new ch.epfl.dias.ops.block.Scan(lines);
		 ch.epfl.dias.ops.block.Scan scanOrders = new ch.epfl.dias.ops.block.Scan(lines);
		 ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(scanLines, scanOrders, 0, 0);
		 project = new ch.epfl.dias.ops.block.Project(join, new int[] { 4, 19 } );

		 time = System.nanoTime();
		 System.out.println("SELECT L.L_QUANTITY, O.O_TOTALPRICE FROM LINEITEM L, ORDERS O WHERE L.ORDERKEY = O.ORDERKEY");
		 project.execute();
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));
		 
		 /*  QUERY 3 */
		 scanLines = new ch.epfl.dias.ops.block.Scan(lines);
		 select = new ch.epfl.dias.ops.block.Select(scanLines, BinaryOp.LT, 4, 50);
		 ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(select, Aggregate.AVG, DataType.DOUBLE, 7);

		 time = System.nanoTime();
		 System.out.println("SELECT AVG(L_TAX) FROM LINEITEM WHERE L_QUANTITY < 50");
		 agg.execute();
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));

		 /* QUERY 4 */
		 scanLines = new ch.epfl.dias.ops.block.Scan(lines);
		 scanOrders = new ch.epfl.dias.ops.block.Scan(lines);
		 join = new ch.epfl.dias.ops.block.Join(scanLines, scanOrders, 0, 0);
		 agg = new ch.epfl.dias.ops.block.ProjectAggregate(join, Aggregate.MAX, DataType.DOUBLE, 19);

		 time = System.nanoTime();
		 System.out.println("SELECT MAX(O.O_TOTALPRICE) FROM LINEITEM L, ORDERS O WHERE L.ORDERKEY = O.ORDERKEY");
		 agg.execute();
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));
	}

	public static void testDSMvector() {
		 System.out.println("_DSM VECTOR BENCHMARK_");
		 ColumnStore orders = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|");
		 ColumnStore lines = new ColumnStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
		 System.out.println("[DSM] Loading datasets...");
		 lines.load();
		 orders.load();
		 System.out.println("[DSM] Datasets loaded.");

		 /* QUERY 1 */
		 ch.epfl.dias.ops.vector.Scan scanLines = new ch.epfl.dias.ops.vector.Scan(lines, 1000);
		 ch.epfl.dias.ops.vector.Select select = new ch.epfl.dias.ops.vector.Select(scanLines, BinaryOp.LT, 4, 50);
		 ch.epfl.dias.ops.vector.Project project = new ch.epfl.dias.ops.vector.Project(select, new int[] { 0, 1, 2, 14, 15 } );
		 
		 System.out.println("SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_SHIPMODE, L_COMMENT FROM LINEITEM WHERE L_QUANTITY < 50");
		 long time = System.nanoTime();
		 project.open();
		 while ((project.next()).length > 0) {}
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));
		 
		 /* QUERY 2 */
		 scanLines = new ch.epfl.dias.ops.vector.Scan(lines, 1000);
		 ch.epfl.dias.ops.vector.Scan scanOrders = new ch.epfl.dias.ops.vector.Scan(lines, 1000);
		 ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(scanLines, scanOrders, 0, 0);
		 project = new ch.epfl.dias.ops.vector.Project(join, new int[] { 4, 19 } );

		 time = System.nanoTime();
		 System.out.println("SELECT L.L_QUANTITY, O.O_TOTALPRICE FROM LINEITEM L, ORDERS O WHERE L.ORDERKEY = O.ORDERKEY");
		 project.open();
		 while ((project.next()).length > 0) {}
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));
		 
		 /*  QUERY 3 */
		 scanLines = new ch.epfl.dias.ops.vector.Scan(lines, 1000);
		 select = new ch.epfl.dias.ops.vector.Select(scanLines, BinaryOp.LT, 4, 50);
		 ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(select, Aggregate.AVG, DataType.DOUBLE, 7);

		 time = System.nanoTime();
		 System.out.println("SELECT AVG(L_TAX) FROM LINEITEM WHERE L_QUANTITY < 50");
		 agg.open();
		 while ((agg.next()).length > 0) {}
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));

		 /* QUERY 4 */
		 scanLines = new ch.epfl.dias.ops.vector.Scan(lines, 1000);
		 scanOrders = new ch.epfl.dias.ops.vector.Scan(lines, 1000);
		 join = new ch.epfl.dias.ops.vector.Join(scanLines, scanOrders, 0, 0);
		 agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.MAX, DataType.DOUBLE, 19);

		 time = System.nanoTime();
		 System.out.println("SELECT MAX(O.O_TOTALPRICE) FROM LINEITEM L, ORDERS O WHERE L.ORDERKEY = O.ORDERKEY");
		 agg.open();
		 while ((agg.next()).length > 0) {}
		 System.out.println("\t executed in " + parseTime(System.nanoTime() - time));
	}
	
	private static String parseTime(long time) {
		String[] units = new String[] { "ns", "ms", "s", "min", "h" };
		double[] scale  = new double[] { 1000000.0, 1000.0, 60.0, 60.0 };
		
		double t = time;
		for (int i = 0; i < scale.length; i++) {
			if (t < scale[i])
				return Double.toString(t) + units[i];
			else 
				t /= scale[i];
		}
		return Double.toString(t) + "h";
	}
}
