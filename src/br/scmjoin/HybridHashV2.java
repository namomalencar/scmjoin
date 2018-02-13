package br.scmjoin;

import gnu.trove.map.TMap;
import gnu.trove.map.hash.THashMap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

public class HybridHashV2 {

	private static LogFile resultlog;
	public static int blockSize = 8192;
	byte block[] = new byte[blockSize];
	byte tupleBlock[], keys[], rowid[], keysSmaller[], blockAuxBigger[],
			keysBigger[];
	int keyJoin, hashResult, numbersOfTuplesInMemorySmaller,
			numbersOfKeysInMemorySmaller, numbersOfTuplesInMemoryBigger,
			numbersOfKeysInMemoryBigger, memoryUsed = 0, keyHashMapSmaller,
			numblockreaded, currentTupleId, numblockreadedAuxBigger;
	ByteBuffer bb;
	boolean isThere;
	long rrelapsedtime;

	public static String name_Tb1 = "";
	public static String name_Tb2 = "";
	public static String tuple_DB = "";
	public static int vazao_Tb1 = 0;
	public static int vazao_Tb2 = 0;

	double timeJoinKernel = 0.0;
	double totaltimeJoinKernel = 0.0;
	double timeFetchKernel = 0.0;
	double totaltimeFetchKernel = 0.0;
	int escritasAcumuladas = 0;
	int releiturasAcumuladas = 0;
	int atribReRead = 0;
	String path = "C:/TPCH10/";

	byte[] reReadTuple;
	String[] reRead = new String[] {};

	// Auxiliares no Overflow
	String line, linhaToAux = "";
	int numblockwritten;
	ArrayList<Integer> vetOfhashResult;
	ArrayList<Integer> vetOfhashResultAuxBigger;

	TMap<Integer, ArrayList<Integer>> mapOfTableSmaller = new THashMap<Integer, ArrayList<Integer>>();
	TMap<Integer, TMap<ByteBuffer, ArrayList<byte[]>>> bucketsInMemoryForSmaller = new THashMap<Integer, TMap<ByteBuffer, ArrayList<byte[]>>>();

	TMap<Integer, ArrayList<Integer>> mapOfTableBigger = new THashMap<Integer, ArrayList<Integer>>();
	TMap<Integer, TMap<ByteBuffer, ArrayList<byte[]>>> bucketsInMemoryForBigger = new THashMap<Integer, TMap<ByteBuffer, ArrayList<byte[]>>>();

	// Auxiliares
	TMap<ByteBuffer, ArrayList<byte[]>> auxBucketsInMemory = new THashMap<ByteBuffer, ArrayList<byte[]>>();
	ArrayList<byte[]> auxBucketsInMemoryInside = new ArrayList<byte[]>();

	TMap<ByteBuffer, ArrayList<byte[]>> auxBucketsInMemoryToHash = new THashMap<ByteBuffer, ArrayList<byte[]>>();
	ArrayList<byte[]> auxBucketsInMemoryInsideToHash = new ArrayList<byte[]>();

	public static void main(String[] args) throws Exception {
		HybridHashV2 fjV3 = new HybridHashV2();
		String[] joinColumnsFirstRelation = null;
		String[] joinColumnsSecondRelation = null;
		String[] columnsFirstRelation = null;
		String[] columnsSecondRelation = null;
		String[] auxHeaderOverFlow = null;
		String arquivo = null;
		boolean firstJoinTable1 = false;
		boolean firstJoinTable2 = false;
		String[] selectFirstRelation = null;
		String[] selectSecondRelation = null;
		int memorySize = Integer.parseInt("104857600");
		String headerJoin = null;
		String teste = "Q10";
		String anmom = "";
		// 3 ta dando erro na segunda
		String nada = "1";
		// ok 1,2, 2.1,3, 3.1, 4.1
		// 4 parou na 2
		String result;
		String data;
		result = "C:/TPCH10/RESULT/";
		data = "C:/TPCH10/";

		if (teste.equals("Q5")) {

			// SELECT count (*)
			// FROM CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION
			// WHERE C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND
			// L_SUPPKEY = S_SUPPKEY
			// AND C_NATIONKEY = N_NATIONKEY AND S_NATIONKEY = N_NATIONKEY AND
			// N_REGIONKEY = R_REGIONKEY
			// AND R_NAME = 'ASIA' AND O_ORDERDATE >= '1994-01-01'
			// AND O_ORDERDATE < '1995-01-01'

			resultlog = new LogFile(result + "Q5Flash.txt");

			FlashObj flobj01 = new FlashObj();
			joinColumnsFirstRelation = new String[] { "regionkey" };
			flobj01.setJoinColumnsFirstRelation(joinColumnsFirstRelation);

			joinColumnsSecondRelation = new String[] { "regionkey" };
			flobj01.setJoinColumnsSecondRelation(joinColumnsSecondRelation);

			firstJoinTable1 = true;
			flobj01.setFirstJoinTable1(firstJoinTable1);

			firstJoinTable2 = true;
			flobj01.setFirstJoinTable2(firstJoinTable2);

			selectFirstRelation = null;
			flobj01.setSelectFirstRelation(selectFirstRelation);

			selectSecondRelation = new String[] { "r_name=ASIA" };
			flobj01.setSelectSecondRelation(selectSecondRelation);

			columnsFirstRelation = new String[] { "nation" };
			flobj01.setColumnsFirstRelation(columnsFirstRelation);

			columnsSecondRelation = new String[] { "region" };
			flobj01.setColumnsSecondRelation(columnsSecondRelation);

			auxHeaderOverFlow = new String[] { "nation[A(55)]|",
					"region[A(55)]|" };
			flobj01.setAuxHeaderOverFlow(auxHeaderOverFlow);

			headerJoin = "nation[A(55)]|region[A(55)]|";
			flobj01.setHeaderJoin(headerJoin);

			flobj01.setTable1(data + "nation.b");

			flobj01.setTable2(data + "region.b");

			flobj01.setIntermediateTableJoin(data + "intermediateTable1Q5.b");

			flobj01.setMemorySizeJoinKernel(memorySize);

			flobj01.kernel.tableFromJoinKernel = data + "intermediateTableQ5.b";
			flobj01.kernel.tablesReRead = new String[] { data + "nation.b" };
			flobj01.kernel.ColReRead = new String[] { "nation" };
			flobj01.kernel.atbReRead = new String[] { "nationkey" };
			flobj01.kernel.lastJoin = false;
			flobj01.kernel.tableToNextJoin = data + "NextJoinQ5.b";
			flobj01.kernel.headerToNextJoin = "nationkey[I(18)]|nation[A(55)]|region[A(55)]|";

			flobj01.setTable1_BD("Nation");
			flobj01.setTable2_BD("Region");

			// Segunda Chamada
			FlashObj flobj02 = new FlashObj();

			joinColumnsFirstRelation = new String[] { "nationkey" };
			flobj02.setJoinColumnsFirstRelation(joinColumnsFirstRelation);

			joinColumnsSecondRelation = new String[] { "nationkey" };
			flobj02.setJoinColumnsSecondRelation(joinColumnsSecondRelation);

			firstJoinTable1 = false;
			flobj02.setFirstJoinTable1(firstJoinTable1);

			firstJoinTable2 = true;
			flobj02.setFirstJoinTable2(firstJoinTable2);

			selectFirstRelation = null;
			flobj02.setSelectFirstRelation(selectFirstRelation);

			selectSecondRelation = null;
			flobj02.setSelectSecondRelation(selectSecondRelation);

			columnsFirstRelation = new String[] { "nation", "region" };
			flobj02.setColumnsFirstRelation(columnsFirstRelation);

			columnsSecondRelation = new String[] { "customer" };
			flobj02.setColumnsSecondRelation(columnsSecondRelation);

			auxHeaderOverFlow = new String[] { "nation[A(55)]|region[A(55)]|",
					"customer[A(55)]|" };
			flobj02.setAuxHeaderOverFlow(auxHeaderOverFlow);

			headerJoin = "nation[A(55)]|region[A(55)]|customer[A(55)]|";
			flobj02.setHeaderJoin(headerJoin);

			flobj02.setTable1(data + "intermediateTable1Q5.b");

			flobj02.setTable2(data + "customer.b");

			flobj02.setIntermediateTableJoin(data + "intermediateTable2Q5.b");

			flobj02.setMemorySizeJoinKernel(memorySize);

			flobj02.kernel.tableFromJoinKernel = data + "intermediateTableQ5.b";
			flobj02.kernel.tablesReRead = new String[] { data + "customer.b" };
			flobj02.kernel.ColReRead = new String[] { "customer" };
			flobj02.kernel.atbReRead = new String[] { "custkey" };
			flobj02.kernel.lastJoin = false;
			flobj02.kernel.tableToNextJoin = data + "NextJoinQ5.b";
			flobj02.kernel.headerToNextJoin = "custkey[I(18)]|nation[A(55)]|region[A(55)]|customer[A(55)]|";

			flobj02.setTable1_BD("Nation_Region");
			flobj02.setTable2_BD("Customer");

			// Terceira Chamada
			FlashObj flobj03 = new FlashObj();

			joinColumnsFirstRelation = new String[] { "custkey" };
			flobj03.setJoinColumnsFirstRelation(joinColumnsFirstRelation);

			joinColumnsSecondRelation = new String[] { "custkey" };
			flobj03.setJoinColumnsSecondRelation(joinColumnsSecondRelation);

			firstJoinTable1 = false;
			flobj03.setFirstJoinTable1(firstJoinTable1);

			firstJoinTable2 = true;
			flobj03.setFirstJoinTable2(firstJoinTable2);

			selectFirstRelation = null;
			flobj03.setSelectFirstRelation(selectFirstRelation);

			selectSecondRelation = new String[] { "o_orderdate>=1994-01-01 and o_orderdate<1995-01-01" };
			flobj03.setSelectSecondRelation(selectSecondRelation);

			columnsFirstRelation = new String[] { "nation", "region",
					"customer" };
			flobj03.setColumnsFirstRelation(columnsFirstRelation);

			columnsSecondRelation = new String[] { "orders" };
			flobj03.setColumnsSecondRelation(columnsSecondRelation);

			auxHeaderOverFlow = new String[] {
					"nation[A(55)]|region[A(55)]|customer[A(55)]|",
					"orders[A(55)]|" };
			flobj03.setAuxHeaderOverFlow(auxHeaderOverFlow);

			headerJoin = "nation[A(55)]|region[A(55)]|customer[A(55)]|orders[A(55)]|";
			flobj03.setHeaderJoin(headerJoin);

			flobj03.setTable1(data + "intermediateTable2Q5.b");

			flobj03.setTable2(data + "orders.b");

			flobj03.setIntermediateTableJoin(data + "intermediateTable3Q5.b");

			flobj03.setMemorySizeJoinKernel(memorySize);

			flobj03.kernel.tableFromJoinKernel = data + "intermediateTableQ5.b";
			flobj03.kernel.tablesReRead = new String[] { data + "orders.b" };
			flobj03.kernel.ColReRead = new String[] { "orders" };
			flobj03.kernel.atbReRead = new String[] { "orderkey" };
			flobj03.kernel.lastJoin = false;
			flobj03.kernel.tableToNextJoin = data + "NextJoinQ5.b";
			flobj03.kernel.headerToNextJoin = "orderkey[I(18)]|nation[A(55)]|region[A(55)]|customer[A(55)]|orders[A(55)]|";

			flobj03.setTable1_BD("Nation_Region_Customer");
			flobj03.setTable2_BD("Orders");

			// Quarta Chamada
			FlashObj flobj04 = new FlashObj();

			joinColumnsFirstRelation = new String[] { "orderkey" };
			flobj04.setJoinColumnsFirstRelation(joinColumnsFirstRelation);

			joinColumnsSecondRelation = new String[] { "orderkey" };
			flobj04.setJoinColumnsSecondRelation(joinColumnsSecondRelation);

			firstJoinTable1 = false;
			flobj04.setFirstJoinTable1(firstJoinTable1);

			firstJoinTable2 = true;
			flobj04.setFirstJoinTable2(firstJoinTable2);

			selectFirstRelation = null;
			flobj04.setSelectFirstRelation(selectFirstRelation);

			selectSecondRelation = null;
			flobj04.setSelectSecondRelation(selectSecondRelation);

			columnsFirstRelation = new String[] { "nation", "region",
					"customer", "orders" };
			flobj04.setColumnsFirstRelation(columnsFirstRelation);

			columnsSecondRelation = new String[] { "lineitem" };
			flobj04.setColumnsSecondRelation(columnsSecondRelation);

			auxHeaderOverFlow = new String[] {
					"nation[A(55)]|region[A(55)]|customer[A(55)]|orders[A(55)]|",
					"lineitem[A(55)]|" };
			flobj04.setAuxHeaderOverFlow(auxHeaderOverFlow);

			headerJoin = "nation[A(55)]|region[A(55)]|customer[A(55)]|orders[A(55)]|lineitem[A(55)]|";
			flobj04.setHeaderJoin(headerJoin);

			flobj04.setTable1(data + "intermediateTable3Q5.b");

			flobj04.setTable2(data + "lineitem.b");

			flobj04.setIntermediateTableJoin(data + "intermediateTable4Q5.b");

			flobj04.setMemorySizeJoinKernel(memorySize);

			flobj04.kernel.tableFromJoinKernel = data + "intermediateTableQ5.b";
			flobj04.kernel.tablesReRead = new String[] { data + "lineitem.b" };
			flobj04.kernel.ColReRead = new String[] { "lineitem" };
			flobj04.kernel.atbReRead = new String[] { "suppkey" };
			flobj04.kernel.lastJoin = false;
			flobj04.kernel.tableToNextJoin = data + "NextJoinQ5.b";
			flobj04.kernel.headerToNextJoin = "suppkey[I(18)]|nation[A(55)]|region[A(55)]|customer[A(55)]|orders[A(55)]|lineitem[A(55)]|";

			flobj04.setTable1_BD("Nation_Region_Customer_Orders");
			flobj04.setTable2_BD("Lineitem");

			// Quinta Chamada
			FlashObj flobj05 = new FlashObj();

			joinColumnsFirstRelation = new String[] { "suppkey" };
			flobj05.setJoinColumnsFirstRelation(joinColumnsFirstRelation);

			joinColumnsSecondRelation = new String[] { "suppkey" };
			flobj05.setJoinColumnsSecondRelation(joinColumnsSecondRelation);

			firstJoinTable1 = false;
			flobj05.setFirstJoinTable1(firstJoinTable1);

			firstJoinTable2 = true;
			flobj05.setFirstJoinTable2(firstJoinTable2);

			selectFirstRelation = null;
			flobj05.setSelectFirstRelation(selectFirstRelation);

			selectSecondRelation = null;
			flobj05.setSelectSecondRelation(selectSecondRelation);

			columnsFirstRelation = new String[] { "nation", "region",
					"customer", "orders", "lineitem" };
			flobj05.setColumnsFirstRelation(columnsFirstRelation);

			columnsSecondRelation = new String[] { "supplier" };
			flobj05.setColumnsSecondRelation(columnsSecondRelation);

			auxHeaderOverFlow = new String[] {
					"nation[A(55)]|region[A(55)]|customer[A(55)]|orders[A(55)]|lineitem[A(55)]|",
					"supplier[A(55)]|" };
			flobj05.setAuxHeaderOverFlow(auxHeaderOverFlow);

			headerJoin = "nation[A(55)]|region[A(55)]|customer[A(55)]|orders[A(55)]|lineitem[A(55)]|supplier[A(55)]|";
			flobj05.setHeaderJoin(headerJoin);

			flobj05.setTable1(data + "intermediateTable4Q5.b");

			flobj05.setTable2(data + "supplier.b");

			flobj05.setIntermediateTableJoin(data + "intermediateTable5Q5.b");

			flobj05.setMemorySizeJoinKernel(memorySize);

			flobj05.kernel.tableFromJoinKernel = data + "intermediateTableQ5.b";
			flobj05.kernel.tablesReRead = new String[] { data + "nation.b",
					data + "region.b", data + "customer.b", data + "orders.b",
					data + "lineitem.b", data + "supplier.b" };
			flobj05.kernel.ColReRead = new String[] { "nation", "region",
					"customer", "orders", "lineitem", "supplier" };
			flobj05.kernel.atbReRead = null;
			flobj05.kernel.lastJoin = true;
			flobj05.kernel.tableToNextJoin = data + "NextJoinQ5.b";
			flobj05.kernel.headerToNextJoin = "Projecao Final";

			flobj05.setTable1_BD("Nation_Region_Customer_Orders_Lineitem");
			flobj05.setTable2_BD("Supplier");

			ArrayList<FlashObj> join = new ArrayList<>();
			join.add(flobj01);
			join.add(flobj02);
			join.add(flobj03);
			join.add(flobj04);
			join.add(flobj05);

			fjV3.hybridJoin(join);

			resultlog.writeLog(tuple_DB);
		}

		if (teste.equals("Q10")) {

			// select * from orders, lineitem where lineitem.l orderkey =
			// orders.o orderkey and orders.o orderdate >= 1993-10-01 and
			// orders.o orderdate < 1994-01-01 and lineitem.l returnflag=R

			resultlog = new LogFile(result + "Q10Flash.txt");

			FlashObj flobj01 = new FlashObj();
			joinColumnsFirstRelation = new String[] { "custkey" };
			flobj01.setJoinColumnsFirstRelation(joinColumnsFirstRelation);

			joinColumnsSecondRelation = new String[] { "custkey" };
			flobj01.setJoinColumnsSecondRelation(joinColumnsSecondRelation);

			firstJoinTable1 = true;
			flobj01.setFirstJoinTable1(firstJoinTable1);

			firstJoinTable2 = true;
			flobj01.setFirstJoinTable2(firstJoinTable2);

			selectFirstRelation = null;
			flobj01.setSelectFirstRelation(selectFirstRelation);

			selectSecondRelation = new String[] { "o_orderdate>=1993-10-01 and o_orderdate<1994-01-01" };
			flobj01.setSelectSecondRelation(selectSecondRelation);

			columnsFirstRelation = new String[] { "customer" };
			flobj01.setColumnsFirstRelation(columnsFirstRelation);

			columnsSecondRelation = new String[] { "orders" };
			flobj01.setColumnsSecondRelation(columnsSecondRelation);

			auxHeaderOverFlow = new String[] { "customer[A(55)]|",
					"orders[A(55)]|" };
			flobj01.setAuxHeaderOverFlow(auxHeaderOverFlow);

			headerJoin = "customer[A(55)]|orders[A(55)]|";
			flobj01.setHeaderJoin(headerJoin);

			flobj01.setTable1(data + "customer.b");

			flobj01.setTable2(data + "orders.b");

			flobj01.setIntermediateTableJoin(data + "intermediateTable1Q10.b");

			flobj01.setMemorySizeJoinKernel(memorySize);

			flobj01.kernel.tableFromJoinKernel = data
					+ "intermediateTableQ10.b";
			flobj01.kernel.tablesReRead = new String[] { data + "orders.b" };
			flobj01.kernel.ColReRead = new String[] { "orders" };
			flobj01.kernel.atbReRead = new String[] { "orderkey" };
			flobj01.kernel.lastJoin = false;
			flobj01.kernel.tableToNextJoin = data + "NextJoinQ10.b";
			flobj01.kernel.headerToNextJoin = "orderkey[I(18)]|customer[A(55)]|orders[A(55)]|";

			flobj01.setTable1_BD("Customer");
			flobj01.setTable2_BD("Orders");

			// Segunda Chamada
			FlashObj flobj02 = new FlashObj();

			joinColumnsFirstRelation = new String[] { "orderkey" };
			flobj02.setJoinColumnsFirstRelation(joinColumnsFirstRelation);

			joinColumnsSecondRelation = new String[] { "orderkey" };
			flobj02.setJoinColumnsSecondRelation(joinColumnsSecondRelation);

			firstJoinTable1 = false;
			flobj02.setFirstJoinTable1(firstJoinTable1);

			firstJoinTable2 = true;
			flobj02.setFirstJoinTable2(firstJoinTable2);

			selectFirstRelation = null;
			flobj02.setSelectFirstRelation(selectFirstRelation);

			selectSecondRelation = new String[] { "l_returnflag=R" };
			flobj02.setSelectSecondRelation(selectSecondRelation);

			columnsFirstRelation = new String[] { "customer", "orders" };
			flobj02.setColumnsFirstRelation(columnsFirstRelation);

			columnsSecondRelation = new String[] { "lineitem" };
			flobj02.setColumnsSecondRelation(columnsSecondRelation);

			auxHeaderOverFlow = new String[] {
					"customer[A(55)]|orders[A(55)]|", "lineitem[A(55)]|" };
			flobj02.setAuxHeaderOverFlow(auxHeaderOverFlow);

			headerJoin = "customer[A(55)]|orders[A(55)]|lineitem[A(55)]|";
			flobj02.setHeaderJoin(headerJoin);

			flobj02.setTable1(data + "intermediateTable1Q10.b");

			flobj02.setTable2(data + "lineitem.b");

			flobj02.setIntermediateTableJoin(data + "intermediateTable2Q10.b");

			flobj02.setMemorySizeJoinKernel(memorySize);

			flobj02.kernel.tableFromJoinKernel = data
					+ "intermediateTableQ10.b";
			flobj02.kernel.tablesReRead = new String[] { data + "customer.b" };
			flobj02.kernel.ColReRead = new String[] { "customer" };
			flobj02.kernel.atbReRead = new String[] { "nationkey" };
			flobj02.kernel.lastJoin = false;
			flobj02.kernel.tableToNextJoin = data + "NextJoinQ10.b";
			flobj02.kernel.headerToNextJoin = "nationkey[I(18)]|customer[A(55)]|orders[A(55)]|lineitem[A(55)]|";

			flobj02.setTable1_BD("Customer_Orders");
			flobj02.setTable2_BD("Lineitem");

			// Segunda Chamada
			FlashObj flobj03 = new FlashObj();

			joinColumnsFirstRelation = new String[] { "nationkey" };
			flobj03.setJoinColumnsFirstRelation(joinColumnsFirstRelation);

			joinColumnsSecondRelation = new String[] { "nationkey" };
			flobj03.setJoinColumnsSecondRelation(joinColumnsSecondRelation);

			firstJoinTable1 = false;
			flobj03.setFirstJoinTable1(firstJoinTable1);

			firstJoinTable2 = true;
			flobj03.setFirstJoinTable2(firstJoinTable2);

			selectFirstRelation = null;
			flobj03.setSelectFirstRelation(selectFirstRelation);

			selectSecondRelation = null;
			flobj03.setSelectSecondRelation(selectSecondRelation);

			columnsFirstRelation = new String[] { "customer", "orders",
					"lineitem" };
			flobj03.setColumnsFirstRelation(columnsFirstRelation);

			columnsSecondRelation = new String[] { "nation" };
			flobj03.setColumnsSecondRelation(columnsSecondRelation);

			auxHeaderOverFlow = new String[] {
					"customer[A(55)]|orders[A(55)]|lineitem[A(55)]|",
					"nation[A(55)]|" };
			flobj03.setAuxHeaderOverFlow(auxHeaderOverFlow);

			headerJoin = "customer[A(55)]|orders[A(55)]|lineitem[A(55)]|nation[A(55)]|";
			flobj03.setHeaderJoin(headerJoin);

			flobj03.setTable1(data + "intermediateTable2Q10.b");

			flobj03.setTable2(data + "nation.b");

			flobj03.setIntermediateTableJoin(data + "intermediateTable3Q10.b");

			flobj03.setMemorySizeJoinKernel(memorySize);

			flobj03.kernel.tableFromJoinKernel = data
					+ "intermediateTableQ10.b";
			flobj03.kernel.tablesReRead = new String[] { data + "customer.b",
					data + "orders.b", data + "lineitem.b", data + "nation.b" };
			flobj03.kernel.ColReRead = new String[] { "customer", "orders",
					"lineitem" };
			flobj03.kernel.atbReRead = null;
			flobj03.kernel.lastJoin = true;
			flobj03.kernel.tableToNextJoin = data + "NextJoinQ10.b";
			flobj03.kernel.headerToNextJoin = "Projecao Final";

			flobj03.setTable1_BD("Customer_Orders_Lineitem");
			flobj03.setTable2_BD("Nation");

			ArrayList<FlashObj> join = new ArrayList<>();
			join.add(flobj01);
			join.add(flobj02);
			join.add(flobj03);

			fjV3.hybridJoin(join);

			resultlog.writeLog(tuple_DB);
		}

		if (teste.equals("Q3")) {

			// select * from orders, lineitem where lineitem.l orderkey =
			// orders.o orderkey and orders.o orderdate >= 1993-10-01 and
			// orders.o orderdate < 1994-01-01 and lineitem.l returnflag=R

			FlashObj flobj01 = new FlashObj();

			resultlog = new LogFile(result + "Q3Flash.txt");

			joinColumnsFirstRelation = new String[] { "custkey" };
			flobj01.setJoinColumnsFirstRelation(joinColumnsFirstRelation);

			joinColumnsSecondRelation = new String[] { "custkey" };
			flobj01.setJoinColumnsSecondRelation(joinColumnsSecondRelation);

			firstJoinTable1 = true;
			flobj01.setFirstJoinTable1(firstJoinTable1);

			firstJoinTable2 = true;
			flobj01.setFirstJoinTable2(firstJoinTable2);

			selectFirstRelation = new String[] { "c_mktsegment=BUILDING" };
			flobj01.setSelectFirstRelation(selectFirstRelation);

			selectSecondRelation = new String[] { "o_orderdate<1995-03-15" };
			flobj01.setSelectSecondRelation(selectSecondRelation);

			columnsFirstRelation = new String[] { "customer" };
			flobj01.setColumnsFirstRelation(columnsFirstRelation);

			columnsSecondRelation = new String[] { "orders" };
			flobj01.setColumnsSecondRelation(columnsSecondRelation);

			headerJoin = "customer[A(55)]|orders[A(55)]|";
			flobj01.setHeaderJoin(headerJoin);

			flobj01.setTable1(data + "customer.b");

			flobj01.setTable2(data + "orders.b");

			auxHeaderOverFlow = new String[] { "customer[A(55)]|",
					"orders[A(55)]|" };
			flobj01.setAuxHeaderOverFlow(auxHeaderOverFlow);

			flobj01.setIntermediateTableJoin(data + "intermediateTable1Q3.b");

			flobj01.setMemorySizeJoinKernel(memorySize);

			flobj01.kernel.tableFromJoinKernel = data
					+ "intermediateTable1Q3.b";
			flobj01.kernel.tablesReRead = new String[] { data + "orders.b" };
			flobj01.kernel.ColReRead = new String[] { "orders" };
			flobj01.kernel.atbReRead = new String[] { "orderkey" };
			flobj01.kernel.lastJoin = false;
			flobj01.kernel.tableToNextJoin = data + "NextJoinQ3.b";
			flobj01.kernel.headerToNextJoin = "orderkey[I(18)]|customer[A(55)]|orders[A(55)]|";

			flobj01.setTable1_BD("Customer");
			flobj01.setTable2_BD("Orders");

			// NextJoin
			FlashObj flobj02 = new FlashObj();

			System.out.println("JOIN 2 = Customer x Orders x Lineitem");

			joinColumnsFirstRelation = new String[] { "orderkey" };
			flobj02.setJoinColumnsFirstRelation(joinColumnsFirstRelation);

			joinColumnsSecondRelation = new String[] { "orderkey" };
			flobj02.setJoinColumnsSecondRelation(joinColumnsSecondRelation);

			firstJoinTable1 = false;
			flobj02.setFirstJoinTable1(firstJoinTable1);

			firstJoinTable2 = true;
			flobj02.setFirstJoinTable2(firstJoinTable2);

			columnsFirstRelation = new String[] { "customer", "orders" };
			flobj02.setColumnsFirstRelation(columnsFirstRelation);

			columnsSecondRelation = new String[] { "lineitem" };
			flobj02.setColumnsSecondRelation(columnsSecondRelation);

			auxHeaderOverFlow = new String[] {
					"customer[A(55)]|orders[A(55)]|", "lineitem[A(55)]|" };
			flobj02.setAuxHeaderOverFlow(auxHeaderOverFlow);

			selectFirstRelation = null;
			flobj02.setSelectFirstRelation(selectFirstRelation);

			selectSecondRelation = new String[] { "l_shipdate>1995-03-15" };
			flobj02.setSelectSecondRelation(selectSecondRelation);

			headerJoin = "customer[A(55)]|orders[A(55)]|lineitem[A(55)]|";
			flobj02.setHeaderJoin(headerJoin);

			flobj02.setTable1(data + "intermediateTable1Q3.b");

			flobj02.setTable2(data + "lineitem.b");

			flobj02.setIntermediateTableJoin(data + "intermediateTable2Q3.b");

			flobj02.setMemorySizeJoinKernel(memorySize);

			flobj02.kernel.tableFromJoinKernel = data
					+ "intermediateTable2Q3.b";
			flobj02.kernel.tablesReRead = new String[] { data + "customer.b",
					data + "orders.b", data + "lineitem.b" };
			flobj02.kernel.ColReRead = new String[] { "customer", "orders",
					"lineitem" };
			flobj02.kernel.lastJoin = true;
			flobj02.kernel.tableToNextJoin = data + "NextJoinQ3.b";
			flobj02.kernel.headerToNextJoin = "Final Projection";

			flobj02.setTable1_BD("Customer_Orders");
			flobj02.setTable2_BD("Lineitem");

			// Call FlashJoin
			ArrayList<FlashObj> join = new ArrayList<>();

			join.add(flobj01);
			join.add(flobj02);

			fjV3.hybridJoin(join);

			resultlog.writeLog(tuple_DB);
		}

		if (teste.equals("QA")) {

			// select * from orders, lineitem where lineitem.l orderkey =
			// orders.o orderkey and orders.o orderdate >= 1993-10-01 and
			// orders.o orderdate < 1994-01-01 and lineitem.l returnflag=R

			FlashObj flobj01 = new FlashObj();

			resultlog = new LogFile(result + "QAFlash.txt");

			joinColumnsFirstRelation = new String[] { "orderkey" };
			flobj01.setJoinColumnsFirstRelation(joinColumnsFirstRelation);

			joinColumnsSecondRelation = new String[] { "orderkey" };
			flobj01.setJoinColumnsSecondRelation(joinColumnsSecondRelation);

			firstJoinTable1 = true;
			flobj01.setFirstJoinTable1(firstJoinTable1);

			firstJoinTable2 = true;
			flobj01.setFirstJoinTable2(firstJoinTable2);

			selectSecondRelation = new String[] { "l_returnflag=R" };
			flobj01.setSelectSecondRelation(selectSecondRelation);

			selectFirstRelation = new String[] { "o_orderdate>=1993-10-01",
					"o_orderdate<1994-01-01" };
			flobj01.setSelectFirstRelation(selectFirstRelation);

			columnsFirstRelation = new String[] { "orders" };
			flobj01.setColumnsFirstRelation(columnsFirstRelation);

			columnsSecondRelation = new String[] { "lineitem" };
			flobj01.setColumnsSecondRelation(columnsSecondRelation);

			auxHeaderOverFlow = new String[] { "orders[A(55)]|",
					"lineitem[A(55)]|" };
			flobj01.setAuxHeaderOverFlow(auxHeaderOverFlow);

			headerJoin = "orders[A(55)]|lineitem[A(55)]|";
			flobj01.setHeaderJoin(headerJoin);

			flobj01.setTable1(data + "orders.b");

			flobj01.setTable2(data + "lineitem.b");

			flobj01.setIntermediateTableJoin(data + "intermediateTableQB.b");

			flobj01.setMemorySizeJoinKernel(memorySize);

			flobj01.kernel.tableFromJoinKernel = data + "intermediateTableQB.b";
			flobj01.kernel.tablesReRead = new String[] { data + "orders.b",
					data + "lineitem.b" };
			flobj01.kernel.ColReRead = new String[] { "orders", "lineitem" };
			flobj01.kernel.lastJoin = true;
			flobj01.kernel.tableToNextJoin = data + "NextJoinQB.b";
			flobj01.kernel.headerToNextJoin = "HeaderOrders e HeaderLineitem";

			flobj01.setTable1_BD("Orders");
			flobj01.setTable2_BD("Lineitem");

			ArrayList<FlashObj> join = new ArrayList<>();
			join.add(flobj01);

			fjV3.hybridJoin(join);
			resultlog.writeLog(tuple_DB);
		}
		if (teste.equals("QB")) {

			// select * from orders, lineitem
			// where lineitem.l orderkey = orders.o orderkey

			FlashObj flobj01 = new FlashObj();

			resultlog = new LogFile(result + "QBFlash.txt");

			joinColumnsFirstRelation = new String[] { "orderkey" };
			flobj01.setJoinColumnsFirstRelation(joinColumnsFirstRelation);

			joinColumnsSecondRelation = new String[] { "orderkey" };
			flobj01.setJoinColumnsSecondRelation(joinColumnsSecondRelation);

			firstJoinTable1 = true;
			flobj01.setFirstJoinTable1(firstJoinTable1);

			firstJoinTable2 = true;
			flobj01.setFirstJoinTable2(firstJoinTable2);

			selectSecondRelation = null;
			flobj01.setSelectSecondRelation(selectSecondRelation);

			selectFirstRelation = null;
			flobj01.setSelectFirstRelation(selectFirstRelation);

			columnsFirstRelation = new String[] { "orders" };
			flobj01.setColumnsFirstRelation(columnsFirstRelation);

			columnsSecondRelation = new String[] { "lineitem" };
			flobj01.setColumnsSecondRelation(columnsSecondRelation);

			auxHeaderOverFlow = new String[] { "orders[A(55)]|",
					"lineitem[A(55)]|" };
			flobj01.setAuxHeaderOverFlow(auxHeaderOverFlow);

			headerJoin = "orders[A(55)]|lineitem[A(55)]|";
			flobj01.setHeaderJoin(headerJoin);

			flobj01.setTable1(data + "orders.b");

			flobj01.setTable2(data + "lineitem.b");

			flobj01.setIntermediateTableJoin(data + "intermediateTableQB.b");

			flobj01.setMemorySizeJoinKernel(memorySize);

			flobj01.kernel.tableFromJoinKernel = data + "intermediateTableQB.b";
			flobj01.kernel.tablesReRead = new String[] { data + "orders.b",
					data + "lineitem.b" };
			flobj01.kernel.ColReRead = new String[] { "orders", "lineitem" };
			flobj01.kernel.lastJoin = true;
			flobj01.kernel.tableToNextJoin = data + "NextJoinQB.b";
			flobj01.kernel.headerToNextJoin = "HeaderOrders e HeaderLineitem";

			flobj01.setTable1_BD("Orders");
			flobj01.setTable2_BD("Lineitem");

			ArrayList<FlashObj> join = new ArrayList<>();
			join.add(flobj01);

			fjV3.hybridJoin(join);
			resultlog.writeLog(tuple_DB);
		}

	}

	private void hybridJoin(ArrayList<FlashObj> listjoin) throws Exception {
		double rrStartTime, rrStopTime;

		HandleFile hafSmaller = new HandleFile(blockSize);
		HandleFile hafSmallerOverFlow = new HandleFile(blockSize);

		HandleFile hafBigger = new HandleFile(blockSize);
		HandleFile hafBiggerOverFlow = new HandleFile(blockSize);

		// Variaveis para Smaller
		int[] indJoinColsSmaller;
		int indcolSmaller;
		String headerSmaller = "";
		boolean selectSmaller = false;

		// Variaveis para Bigger
		int[] indJoinColsBigger;
		int indcolBigger;
		String headerBigger = "";
		boolean selectBigger = false;

		// Variaveis Join
		String linhaToAux = "";
		String linhaAuxSmaller = "";
		String linhaAuxBigger = "";
		byte tupleBlockAuxBigger[] = null;
		int numberOfTupleFinalJoin = 0;

		for (int w = 0; w < listjoin.size(); w++) {
			memoryUsed = 0;

			mapOfTableSmaller = new THashMap<Integer, ArrayList<Integer>>();
			bucketsInMemoryForSmaller = new THashMap<Integer, TMap<ByteBuffer, ArrayList<byte[]>>>();

			mapOfTableBigger = new THashMap<Integer, ArrayList<Integer>>();
			bucketsInMemoryForBigger = new THashMap<Integer, TMap<ByteBuffer, ArrayList<byte[]>>>();

			// Auxiliares
			auxBucketsInMemory = new THashMap<ByteBuffer, ArrayList<byte[]>>();
			auxBucketsInMemoryInside = new ArrayList<byte[]>();

			auxBucketsInMemoryToHash = new THashMap<ByteBuffer, ArrayList<byte[]>>();
			auxBucketsInMemoryInsideToHash = new ArrayList<byte[]>();

			numbersOfTuplesInMemorySmaller = 0;
			numbersOfKeysInMemorySmaller = 0;
			numbersOfTuplesInMemoryBigger = 0;
			numbersOfKeysInMemoryBigger = 0;

			FlashObj join = new FlashObj();
			join = listjoin.get(w);
			HashFunction.initialize(join.memorySizeJoinKernel / blockSize);

			rrStartTime = System.currentTimeMillis();
			// Iniciar 1 fase pela tabela1
			hafSmaller.open(join.table1);
			indJoinColsSmaller = new int[join.joinColumnsFirstRelation.length];
			for (int i = 0; i < join.joinColumnsFirstRelation.length; i++) {
				indJoinColsSmaller[i] = hafSmaller
						.getColumnPos(join.joinColumnsFirstRelation[i]);
				headerSmaller = headerSmaller
						+ join.joinColumnsFirstRelation[i] + "[I(18)]|";
			}

			indcolSmaller = hafSmaller
					.getColumnPos(join.joinColumnsFirstRelation[0]);
			headerSmaller = headerSmaller + join.auxHeaderOverFlow[0];
			hafSmallerOverFlow.create(path + "OverFlowSmaller.b",
					RafIOCalc.getHeaderString(hafSmaller));
			headerSmaller = "";

			// Começando a ler a tabela1
			block = hafSmaller.nextBlock();
			while (block != null) {
				tupleBlock = hafSmaller.nextTuple(block);
				while (tupleBlock != null) {
					// Verificar a seletividade antes de particionar
					if (join.getSelectFirstRelation() != null) {
						for (int i = 0; i < join.getSelectFirstRelation().length; i++) {
							selectSmaller = hafSmaller.checkCondition(
									join.getSelectFirstRelation()[i],
									tupleBlock);
							if (!selectSmaller) {
								i = join.getSelectFirstRelation().length;
							}
						}
					} else {
						selectSmaller = true;
					}
					if (selectSmaller) {
						vazao_Tb1++;
						if (join.memorySizeJoinKernel > memoryUsed) {
							sendToMemorySmaller(tupleBlock, hafSmaller,
									indcolSmaller, indJoinColsSmaller, join);
						} else {
							sendToDiskSmaller(tupleBlock, hafSmaller,
									indcolSmaller, indJoinColsSmaller,
									hafSmallerOverFlow, join);
						}
					}
					// calcMemory(join);
					selectSmaller = false;
					tupleBlock = hafSmaller.nextTuple(block);
				}
				block = hafSmaller.nextBlock();
			}
			sendToDiskSmallerFinish(hafSmallerOverFlow, join);

			// System.out.println("Memory Smaller");
			// System.out.println(numbersOfTuplesInMemorySmaller);

			// Finalizou a tabela 1
			hafSmaller.close();

			// Iniciar fase pela tabela2
			hafBigger.open(join.table2);
			indJoinColsBigger = new int[join.joinColumnsSecondRelation.length];
			for (int i = 0; i < join.joinColumnsSecondRelation.length; i++) {
				indJoinColsBigger[i] = hafBigger
						.getColumnPos(join.joinColumnsSecondRelation[i]);
				headerBigger = headerBigger + join.joinColumnsSecondRelation[i]
						+ "[I(18)]|";
			}

			indcolBigger = hafBigger
					.getColumnPos(join.joinColumnsSecondRelation[0]);
			headerBigger = headerBigger + join.auxHeaderOverFlow[1];
			hafBiggerOverFlow.create(path + "OverFlowBigger.b",
					RafIOCalc.getHeaderString(hafBigger));
			headerBigger = "";
			// Começando a ler a tabela2
			block = hafBigger.nextBlock();
			while (block != null) {
				tupleBlock = hafBigger.nextTuple(block);
				while (tupleBlock != null) {
					// Verificar a seletividade antes de particionar
					if (join.getSelectSecondRelation() != null) {
						for (int i = 0; i < join.getSelectSecondRelation().length; i++) {
							selectBigger = hafBigger.checkCondition(
									join.getSelectSecondRelation()[i],
									tupleBlock);
							if (!selectBigger) {
								i = join.getSelectSecondRelation().length;
							}
						}
					} else {
						selectBigger = true;
					}
					if (selectBigger) {
						vazao_Tb2++;
						if (join.memorySizeJoinKernel > memoryUsed) {
							sendToMemoryBigger(tupleBlock, hafBigger,
									indcolBigger, indJoinColsBigger, join);
						} else {
							sendToDiskBigger(tupleBlock, hafBigger,
									indcolBigger, indJoinColsBigger,
									hafSmallerOverFlow, hafBiggerOverFlow,
									join.memorySizeJoinKernel, join);
						}
					}
					// calcMemory(join);
					selectBigger = false;
					tupleBlock = hafBigger.nextTuple(block);
				}
				block = hafBigger.nextBlock();
			}
			sendToDiskBiggerFinish(hafSmallerOverFlow, hafBiggerOverFlow, join);
			// Finalizou a tabela 2
			hafBigger.close();

			linhaToAux = "";
			linhaAuxSmaller = "";
			linhaAuxBigger = "";

			HandleFile hafFlashJoin = new HandleFile(blockSize);
			hafFlashJoin.create(
					join.intermediateTableJoin,
					RafIOCalc.getHeaderString(hafSmaller)
							+ RafIOCalc.getHeaderString(hafBigger));
			// System.out.println(headerJoin);

			// System.out
			// .println("Verificando os buckets em memoria e escrevendo");
			// Estou sempre fazendo o Smaller para o Bigger
			for (Integer hashResultInMapForSmaller : bucketsInMemoryForSmaller
					.keySet()) {
				if (bucketsInMemoryForBigger
						.containsKey(hashResultInMapForSmaller)) {
					auxBucketsInMemoryToHash = bucketsInMemoryForSmaller
							.get(hashResultInMapForSmaller);
					auxBucketsInMemory = bucketsInMemoryForBigger
							.get(hashResultInMapForSmaller);
					// Eu já estou com os dois buckets Smaller e Bigger
					// Vou percorrer o smaller verificando se tem no bigger
					for (ByteBuffer hashResultInMapForSmallerInside : auxBucketsInMemoryToHash
							.keySet()) {
						// Verificação já a nível de atributo de junção
						if (auxBucketsInMemory
								.containsKey(hashResultInMapForSmallerInside)) {
							auxBucketsInMemoryInsideToHash = auxBucketsInMemoryToHash
									.get(hashResultInMapForSmallerInside);
							auxBucketsInMemoryInside = auxBucketsInMemory
									.get(hashResultInMapForSmallerInside);
							// System.out.println("Atributo de Junção: "+hashResultInMapForSmallerInside);
							keys = new byte[hashResultInMapForSmallerInside
									.remaining()];
							hashResultInMapForSmallerInside.get(keys);

							for (int i = 0; i < auxBucketsInMemoryInsideToHash
									.size(); i++) {
								// por se tem é porque eles tem o mesmo
								// valor de
								// key
								tupleBlock = auxBucketsInMemoryInsideToHash
										.get(i);
								linhaAuxSmaller = RafIOCalc.getLineString(
										hafSmallerOverFlow,
										auxBucketsInMemoryInsideToHash.get(i),
										hafSmallerOverFlow.getQtCols());
								for (int j = 0; j < auxBucketsInMemoryInside
										.size(); j++) {
									tupleBlock = auxBucketsInMemoryInside
											.get(j);
									// Como é first a linha já é o rowid
									linhaAuxBigger = RafIOCalc.getLineString(
											hafBiggerOverFlow,
											auxBucketsInMemoryInside.get(j),
											hafBiggerOverFlow.getQtCols());
									linhaToAux = linhaAuxSmaller
											+ linhaAuxBigger;
									linhaToAux = linhaToAux.replace("||", "|");
									numberOfTupleFinalJoin++;
									// System.out.println(linhaToAux);
									if (!join.kernel.lastJoin) {
										hafFlashJoin.writeTuple(linhaToAux);
									}
									linhaToAux = "";
									linhaAuxBigger = "";
									// Preciso voltar linha para o que ela
									// era
								}
								linhaAuxSmaller = "";
							}
						}
					}
				} else {
					// Só um verificação de erro, em todos os meus teste não
					// entrou
					// System.out.println("Não esta na Bigger: "
					// + hashResultInMapForSmaller);
				}
			}
			// System.out.println("Fim da junção em memoria");
			// System.out.println("Smaller");
			// System.out.println(numbersOfKeysInMemorySmaller);
			// System.out.println(numbersOfTuplesInMemorySmaller);
			// System.out.println("Bigger");
			// System.out.println(numbersOfKeysInMemoryBigger);
			// System.out.println(numbersOfTuplesInMemoryBigger);

			// System.out.println("Escritas Smaller");
			// System.out.println(hafSmallerOverFlow.numberofWrite);
			// System.out.println("Escritas Bigger");
			// System.out.println(hafBiggerOverFlow.numberofWrite);
			// System.out.println("MemoryJoin");
			// System.out.println(numberOfTupleFinalJoin);
			// System.out.println("+++++++++++++++++++++++++++++++++");

			// Variaveis
			String linha = "";

			HashMap<ByteBuffer, ArrayList<Integer>> hashIndice = new HashMap<ByteBuffer, ArrayList<Integer>>();

			// System.out
			// .println("Generating hash index for smaller table and making join");
			// Join: for each entry in small table map
			// add entry in hash index
			// for each blockNo

			ArrayList<Integer> auxHash = new ArrayList<>();

			for (int i = 0; i < join.joinColumnsSecondRelation.length; i++) {
				indJoinColsBigger[i] = hafBiggerOverFlow
						.getColumnPos(join.joinColumnsSecondRelation[i]);
			}

			for (int i = 0; i < join.joinColumnsFirstRelation.length; i++) {
				indJoinColsSmaller[i] = hafSmallerOverFlow
						.getColumnPos(join.joinColumnsFirstRelation[i]);
			}

			for (Entry<Integer, ArrayList<Integer>> entry : mapOfTableSmaller
					.entrySet()) {
				// hash(key)
				keyHashMapSmaller = entry.getKey();
				// blockNo of tuples of this hash
				vetOfhashResult = entry.getValue();
				// for each blockNo
				for (int i = 0; i < vetOfhashResult.size(); i++) {
					numblockreaded = vetOfhashResult.get(i);
					block = hafSmallerOverFlow.readBlock(numblockreaded);
					tupleBlock = hafSmallerOverFlow.nextTuple(block);
					if (tupleBlock != null) {
						linha = RafIOCalc.getLineString(hafSmallerOverFlow,
								tupleBlock, hafSmallerOverFlow.getQtCols());
					} else {
						linha = null;
					}
					while (linha != null) {
						keysSmaller = RafIOCalc.getKeys(tupleBlock,
								indJoinColsSmaller,
								hafSmallerOverFlow.getQtCols());
						bb = ByteBuffer.wrap(keysSmaller);
						currentTupleId = hafSmallerOverFlow.getCurrentTupleId();
						isThere = hashIndice.containsKey(bb);
						if (!isThere) {
							auxHash = new ArrayList<>();
							auxHash.add(numblockreaded);
							auxHash.add(currentTupleId);
							hashIndice.put(bb, auxHash);
						} else {
							auxHash = hashIndice.get(bb);
							auxHash.add(numblockreaded);
							auxHash.add(currentTupleId);
							hashIndice.put(bb, auxHash);
						}

						tupleBlock = hafSmallerOverFlow.nextTuple(block);

						if (tupleBlock != null) {
							linha = RafIOCalc.getLineString(hafSmallerOverFlow,
									tupleBlock, hafSmallerOverFlow.getQtCols());
						} else {
							linha = null;
						}
					}
				}

				// Chech if keyHashMapSmaller exists on mapOfTableBigger
				if (mapOfTableBigger.containsKey(keyHashMapSmaller)) {
					vetOfhashResultAuxBigger = mapOfTableBigger
							.get(keyHashMapSmaller);
					for (int i = 0; i < vetOfhashResultAuxBigger.size(); i++) {
						numblockreadedAuxBigger = vetOfhashResultAuxBigger
								.get(i);
						blockAuxBigger = hafBiggerOverFlow
								.readBlock(numblockreadedAuxBigger);
						tupleBlockAuxBigger = hafBiggerOverFlow
								.nextTuple(blockAuxBigger);
						if (tupleBlockAuxBigger != null) {
							linhaAuxBigger = RafIOCalc.getLineString(
									hafBiggerOverFlow, tupleBlockAuxBigger,
									hafBiggerOverFlow.getQtCols());
						} else {
							linhaAuxBigger = null;
						}
						while (linhaAuxBigger != null) {
							keysBigger = RafIOCalc.getKeys(tupleBlockAuxBigger,
									indJoinColsBigger,
									hafBiggerOverFlow.getQtCols());
							bb = ByteBuffer.wrap(keysBigger);
							auxHash = hashIndice.get(bb);
							if (auxHash != null) {
								for (int j = 0; j < auxHash.size(); j = j + 2) {
									block = hafSmallerOverFlow.raf
											.readBlock(auxHash.get(j));
									tupleBlock = hafSmallerOverFlow
											.readTupleById(block,
													auxHash.get(j + 1));

									for (int k = 0; k < join.columnsFirstRelation.length; k++) {
										linhaAuxSmaller = RafIOCalc
												.getLineString(
														hafSmallerOverFlow,
														tupleBlock,
														hafSmallerOverFlow
																.getQtCols());
									}

									linhaToAux = linhaAuxSmaller
											+ linhaAuxBigger;
									linhaToAux = linhaToAux.replace("||", "|");
									if (!join.kernel.lastJoin) {
										hafFlashJoin.writeTuple(linhaToAux);
									}
									linhaToAux = "";
									linhaAuxSmaller = "";
									numberOfTupleFinalJoin++;

								}
							} else {
								// System.out.println("Nao achou na HashIndice");
							}

							linhaAuxBigger = "";
							tupleBlockAuxBigger = hafBiggerOverFlow
									.nextTuple(blockAuxBigger);
							if (tupleBlockAuxBigger != null) {
								linhaAuxBigger = RafIOCalc.getLineString(
										hafBiggerOverFlow, tupleBlockAuxBigger,
										hafBiggerOverFlow.getQtCols());
							} else {
								linhaAuxBigger = null;
							}
						}

					}

				}
				hashIndice.clear();
			}
			hafFlashJoin.flush();
			System.out.println("Fim Join");
			System.out.println(numberOfTupleFinalJoin);

			rrStopTime = System.currentTimeMillis();
			timeJoinKernel = (rrStopTime - rrStartTime) / 1000;

			tuple_DB = tuple_DB + join.table1_BD + "|" + vazao_Tb1 + "|"
					+ join.table2_BD + "|" + vazao_Tb2 + "|";

			// Escrita Tempo
			// resultlog.writeLog("Time Join: " + timeJoinKernel);
			// resultlog.writeLog("Escritas OverFlow Tabela 1: "
			// + hafSmallerOverFlow.numberofWrite);
			// resultlog.writeLog("Escritas OverFlow Tabela 2: "
			// + hafBiggerOverFlow.numberofWrite);
			// resultlog.writeLog("Escritas Join: " +
			// hafFlashJoin.numberofWrite);
			totaltimeJoinKernel = totaltimeJoinKernel + timeJoinKernel;
			escritasAcumuladas += hafFlashJoin.numberofWrite;
			escritasAcumuladas += (hafBiggerOverFlow.numberofWrite + hafSmallerOverFlow.numberofWrite);
			tuple_DB = tuple_DB + timeJoinKernel + "|"
					+ hafSmallerOverFlow.numberofWrite + "|"
					+ hafBiggerOverFlow.numberofWrite + "|";

			tuple_DB = tuple_DB + escritasAcumuladas + "|";
			// Fecha o HafFLashJoin depois de escrever o número de escritas
			hafFlashJoin.close();
			// hafFlashJoin = null;

			String feeeetchKernel = "";
			int atribReRead = 0;

			// FETCH KERNEL
			// byte[] reReadTuple;
			// String[] reRead = new String[] {};
			// hafFlashJoin = new HandleFile(blockSize);
			// hafFlashJoin.open(join.kernel.tableFromJoinKernel);
			// HandleFile haf0 = new HandleFile(blockSize);
			// HandleFile haf1 = new HandleFile(blockSize);
			// HandleFile haf2 = new HandleFile(blockSize);
			// HandleFile haf3 = new HandleFile(blockSize);
			// HandleFile haf4 = new HandleFile(blockSize);
			// HandleFile hafNextFlashJoin = new HandleFile(blockSize);
			// Abrir o número de hafs necessarios para releitura
			// if (!join.kernel.lastJoin) {
			// hafNextFlashJoin.create(join.kernel.tableToNextJoin,
			// join.kernel.headerToNextJoin);
			// atribReRead = hafFlashJoin
			// .getColumnPos(join.kernel.ColReRead[0]);
			// }

			// Tempo de Leitura
			// rrStartTime = System.currentTimeMillis();
			// block = hafFlashJoin.nextBlock();
			// while (block != null) {
			// tupleBlock = hafFlashJoin.nextTuple(block);
			// while (tupleBlock != null) {
			// line = RafIOCalc.getLineString(hafFlashJoin, tupleBlock,
			// hafFlashJoin.getQtCols());
			// reRead = line.split("\\|");
			// // Start reRead
			// if (!join.kernel.lastJoin) {
			// // A releitura de atributo é de uma só tabela
			// reReadTuple = haf0.readRowid(reRead[atribReRead]);
			// for (int i = 0; i < join.kernel.atbReRead.length; i++) {
			// line = RafIOCalc
			// .getColumn(
			// haf0,
			// reReadTuple,
			// haf0.getColumnPos(join.kernel.atbReRead[i]),
			// haf0.getQtCols())
			// + "|" + line;
			// }
			// } else {
			// line = "";
			// for (int i = 0; i < join.kernel.tablesReRead.length; i++) {
			// if (i == 0) {
			// reReadTuple = haf0.readRowid(reRead[i]);
			// line = line
			// + RafIOCalc.getLineString(haf0,
			// reReadTuple, haf0.getQtCols());
			// }
			// if (i == 1) {
			// reReadTuple = haf1.readRowid(reRead[i]);
			// line = line
			// + RafIOCalc.getLineString(haf1,
			// reReadTuple, haf1.getQtCols());
			// }
			// if (i == 2) {
			// reReadTuple = haf2.readRowid(reRead[i]);
			// line = line
			// + RafIOCalc.getLineString(haf2,
			// reReadTuple, haf2.getQtCols());
			// }
			// if (i == 3) {
			// reReadTuple = haf3.readRowid(reRead[i]);
			// line = line
			// + RafIOCalc.getLineString(haf3,
			// reReadTuple, haf3.getQtCols());
			// }
			// if (i == 4) {
			// reReadTuple = haf4.readRowid(reRead[i]);
			// line = line
			// + RafIOCalc.getLineString(haf4,
			// reReadTuple, haf4.getQtCols());
			// }
			// }
			// }
			// if (!join.kernel.lastJoin) {
			// hafNextFlashJoin.writeTuple(line);
			// }
			// // System.out.println(line);
			//
			// tupleBlock = hafFlashJoin.nextTuple(block);
			// }
			// block = hafFlashJoin.nextBlock();
			// }

			// FIM FetchKernel
			// rrStopTime = System.currentTimeMillis();
			// timeFetchKernel = (rrStopTime - rrStartTime) / 1000;

			// if (!join.kernel.lastJoin) {
			// hafNextFlashJoin.flush();
			// escritasAcumuladas = escritasAcumuladas
			// + hafNextFlashJoin.numberofWrite;
			// // Escrita Tempo
			// // resultlog.writeLog("Time Fetch: " + timeFetchKernel);
			// // resultlog.writeLog("Escritas FetchKernel: "
			// // + hafNextFlashJoin.numberofWrite);
			// hafNextFlashJoin.gatherStats();
			// hafNextFlashJoin.close();
			// } else {
			// // resultlog.writeLog("Time Fetch: " + timeFetchKernel);
			// }
			totaltimeFetchKernel = totaltimeFetchKernel + timeFetchKernel;

			// for (int i = 0; i < join.kernel.tablesReRead.length; i++) {
			// if (i == 0) {
			// releiturasAcumuladas += haf0.numberofReReadbyRowId;
			// // resultlog.writeLog("ReRead FetchKernel "
			// // + join.kernel.tablesReRead[i] + ": "
			// // + haf0.numberofReReadbyRowId);
			// haf0.close();
			// }
			// if (i == 1) {
			// releiturasAcumuladas += haf1.numberofReReadbyRowId;
			// // resultlog.writeLog("ReRead FetchKernel "
			// // + join.kernel.tablesReRead[i] + ": "
			// // + haf1.numberofReReadbyRowId);
			// haf1.close();
			// }
			// if (i == 2) {
			// releiturasAcumuladas += haf2.numberofReReadbyRowId;
			// // resultlog.writeLog("ReRead FetchKernel "
			// // + join.kernel.tablesReRead[i] + ": "
			// // + haf2.numberofReReadbyRowId);
			// haf2.close();
			// }
			// if (i == 3) {
			// releiturasAcumuladas += haf3.numberofReReadbyRowId;
			// // resultlog.writeLog("ReRead FetchKernel "
			// // + join.kernel.tablesReRead[i] + ": "
			// // + haf3.numberofReReadbyRowId);
			// haf3.close();
			// }
			// if (i == 4) {
			// releiturasAcumuladas += haf4.numberofReReadbyRowId;
			// // resultlog.writeLog("ReRead FetchKernel "
			// // + join.kernel.tablesReRead[i] + ": "
			// // + haf4.numberofReReadbyRowId);
			// haf4.close();
			// }
			// }

			if (join.kernel.lastJoin) {
				// resultlog.writeLog("Total Time JoinKernel: "
				// + totaltimeJoinKernel);
				// resultlog.writeLog("Total Time FetchKernel: "
				// + totaltimeFetchKernel);
				// resultlog.writeLog("Number of Tuples: "
				// + numberOfTupleFinalJoin);
				tuple_DB = tuple_DB + totaltimeJoinKernel + "|"
						+ numberOfTupleFinalJoin;
			}
			vazao_Tb1 = 0;
			vazao_Tb2 = 0;
			timeJoinKernel = 0;
			numberOfTupleFinalJoin = 0;
			numberOfTupleFinalJoin = 0;
			// resultlog.writeLog("Escritas Acumuladas: " + escritasAcumuladas);
			// resultlog
			// .writeLog("Releituras Acumuladas: " + releiturasAcumuladas);
			// resultlog.writeLog("===================================");
			// Foi fechado mais acima
			// hafNextFlashJoin.close();
			hafBigger.close();
			hafSmaller.close();
			hafFlashJoin.close();
			hafBiggerOverFlow.close();
			hafSmallerOverFlow.close();

		}

	}

	private void fetchKernel(FlashObj join, String line, HandleFile haf0,
			HandleFile haf1, HandleFile haf2, HandleFile haf3, HandleFile haf4,
			HandleFile haf5, HandleFile hafNextFlashJoin) throws Exception {
		// Tempo de Leitura
		double rrStartTime = System.currentTimeMillis();
		reRead = line.split("\\|");
		// Start reRead
		if (!join.kernel.lastJoin) {
			// A releitura de atributo é de uma só tabela
			reReadTuple = haf0.readRowid(reRead[atribReRead]);
			for (int i = 0; i < join.kernel.atbReRead.length; i++) {
				line = RafIOCalc.getColumn(haf0, reReadTuple,
						haf0.getColumnPos(join.kernel.atbReRead[i]),
						haf0.getQtCols())
						+ "|" + line;
			}
		} else {
			line = "";
			for (int i = 0; i < join.kernel.tablesReRead.length; i++) {
				if (i == 0) {
					reReadTuple = haf0.readRowid(reRead[i]);
					line = line
							+ RafIOCalc.getLineString(haf0, reReadTuple,
									haf0.getQtCols());
				}
				if (i == 1) {
					reReadTuple = haf1.readRowid(reRead[i]);
					line = line
							+ RafIOCalc.getLineString(haf1, reReadTuple,
									haf1.getQtCols());
				}
				if (i == 2) {
					reReadTuple = haf2.readRowid(reRead[i]);
					line = line
							+ RafIOCalc.getLineString(haf2, reReadTuple,
									haf2.getQtCols());
				}
				if (i == 3) {
					reReadTuple = haf3.readRowid(reRead[i]);
					line = line
							+ RafIOCalc.getLineString(haf3, reReadTuple,
									haf3.getQtCols());
				}
				if (i == 4) {
					reReadTuple = haf4.readRowid(reRead[i]);
					line = line
							+ RafIOCalc.getLineString(haf4, reReadTuple,
									haf4.getQtCols());
				}
				if (i == 5) {
					reReadTuple = haf5.readRowid(reRead[i]);
					line = line
							+ RafIOCalc.getLineString(haf5, reReadTuple,
									haf5.getQtCols());
				}
			}
		}
		if (!join.kernel.lastJoin) {
			hafNextFlashJoin.writeTuple(line);
		}
		// System.out.println(line);
		// FIM FetchKernel
		double rrStopTime = System.currentTimeMillis();
		timeFetchKernel += (rrStopTime - rrStartTime) / 1000;

	}

	private void sendToDiskSmallerFinish(HandleFile hafSmallerOverFlow,
			FlashObj join) throws Exception {
		for (Integer hashResultInMapForSmaller : mapOfTableSmaller.keySet()) {
			if (bucketsInMemoryForSmaller
					.containsKey(hashResultInMapForSmaller)) {
				auxBucketsInMemory = bucketsInMemoryForSmaller
						.get(hashResultInMapForSmaller);
				vetOfhashResult = mapOfTableSmaller
						.get(hashResultInMapForSmaller);
				if (vetOfhashResult == null) {
					vetOfhashResult = new ArrayList<>();
				}

				for (ByteBuffer chave : auxBucketsInMemory.keySet()) {
					numbersOfKeysInMemorySmaller--;
					auxBucketsInMemoryInside = auxBucketsInMemory.get(chave);
					// exceeded memory for buckets
					// remove of memory and copy to disk
					for (int j = 0; j < auxBucketsInMemoryInside.size(); j++) {
						linhaToAux = RafIOCalc.getLineString(
								hafSmallerOverFlow,
								auxBucketsInMemoryInside.get(j),
								hafSmallerOverFlow.getQtCols());
						numbersOfTuplesInMemorySmaller--;
						hafSmallerOverFlow.writeTuple(linhaToAux);
						linhaToAux = "";
						numblockwritten = hafSmallerOverFlow.raf
								.getMaxBlockNo();
						// write map of blocks
						if (!vetOfhashResult.contains(numblockwritten)) {
							vetOfhashResult.add(numblockwritten);
						}
						mapOfTableSmaller.put(hashResultInMapForSmaller,
								vetOfhashResult);
					}
				}

				bucketsInMemoryForSmaller.remove(hashResultInMapForSmaller);
				hafSmallerOverFlow.flush();
			}

		}

	}

	private void sendToDiskBiggerFinish(HandleFile hafSmallerOverFlow,
			HandleFile hafBiggerOverFlow, FlashObj join) throws Exception {
		for (Integer hashResultInMapForSmaller : mapOfTableSmaller.keySet()) {
			if (bucketsInMemoryForBigger.containsKey(hashResultInMapForSmaller)) {
				auxBucketsInMemory = bucketsInMemoryForBigger
						.get(hashResultInMapForSmaller);

				vetOfhashResult = mapOfTableBigger
						.get(hashResultInMapForSmaller);

				if (vetOfhashResult == null) {
					vetOfhashResult = new ArrayList<>();
				}

				for (ByteBuffer chave : auxBucketsInMemory.keySet()) {
					numbersOfKeysInMemoryBigger--;
					auxBucketsInMemoryInside = auxBucketsInMemory.get(chave);
					// exceeded memory for buckets
					// remove of memory and copy to disk
					byte[] keysInMemoryInside = new byte[chave.remaining()];
					for (int j = 0; j < auxBucketsInMemoryInside.size(); j++) {
						// int auxKeystoInt = 0;
						// chave.position(0);
						// chave.get(keysInMemoryInside, 0,
						// keysInMemoryInside.length);
						// for (int i = 0; i < keysInMemoryInside.length / 4;
						// i++) {
						// byte temp[] = new byte[4];
						// for (int k = 0; k < 4; k++) {
						// temp[k] = keysInMemoryInside[auxKeystoInt];
						// auxKeystoInt++;
						// }
						// linhaToAux = linhaToAux
						// + Integer.toString(RafIOCalc.getInt(temp))
						// + "|";
						// }
						linhaToAux = RafIOCalc.getLineString(hafBiggerOverFlow,
								auxBucketsInMemoryInside.get(j),
								hafBiggerOverFlow.getQtCols());
						numbersOfTuplesInMemoryBigger--;
						hafBiggerOverFlow.writeTuple(linhaToAux);
						linhaToAux = "";
						numblockwritten = hafBiggerOverFlow.raf.getMaxBlockNo();
						// write map of blocks
						if (!vetOfhashResult.contains(numblockwritten)) {
							vetOfhashResult.add(numblockwritten);
						}
						mapOfTableBigger.put(hashResultInMapForSmaller,
								vetOfhashResult);
					}
				}

				bucketsInMemoryForBigger.remove(hashResultInMapForSmaller);
				hafBiggerOverFlow.flush();
			}

		}

	}

	private void calcMemory(FlashObj join) {
		memoryUsed = 0;
		memoryUsed = memoryUsed + numbersOfKeysInMemorySmaller
				* (join.joinColumnsFirstRelation.length * 4);
		memoryUsed = memoryUsed + numbersOfKeysInMemoryBigger
				* (join.joinColumnsSecondRelation.length * 4);
		memoryUsed = memoryUsed + numbersOfTuplesInMemorySmaller
				* (join.columnsFirstRelation.length * 12);
		memoryUsed = memoryUsed + numbersOfTuplesInMemoryBigger
				* (join.columnsSecondRelation.length * 12);
	}

	private void sendToDiskBigger(byte[] tupleBlockToMemory, HandleFile haf,
			int indCol, int[] indJoinCols, HandleFile hafSmallerOverFlow,
			HandleFile hafBiggerOverFlow, int memorySizeJoinKernel,
			FlashObj join) throws Exception {
		// Nao manda a particao do hashResult
		// Verificar quem esta em disco tb1 e mandar o correspondente tb2
		// Verificar particoes
		for (Integer hashResultInMapForSmaller : mapOfTableSmaller.keySet()) {
			if (bucketsInMemoryForBigger.containsKey(hashResultInMapForSmaller)) {
				auxBucketsInMemory = bucketsInMemoryForBigger
						.get(hashResultInMapForSmaller);
				vetOfhashResult = mapOfTableBigger
						.get(hashResultInMapForSmaller);

				if (vetOfhashResult == null) {
					vetOfhashResult = new ArrayList<>();
				}

				for (ByteBuffer chave : auxBucketsInMemory.keySet()) {
					numbersOfKeysInMemoryBigger--;
					memoryUsed -= 4;
					auxBucketsInMemoryInside = auxBucketsInMemory.get(chave);
					// exceeded memory for buckets
					// remove of memory and copy to disk
					byte[] keysInMemoryInside = new byte[chave.remaining()];
					for (int j = 0; j < auxBucketsInMemoryInside.size(); j++) {
						// int auxKeystoInt = 0;
						// chave.position(0);
						// chave.get(keysInMemoryInside, 0,
						// keysInMemoryInside.length);
						// for (int i = 0; i < keysInMemoryInside.length / 4;
						// i++) {
						// byte temp[] = new byte[4];
						// for (int k = 0; k < 4; k++) {
						// temp[k] = keysInMemoryInside[auxKeystoInt];
						// auxKeystoInt++;
						// }
						// linhaToAux = linhaToAux
						// + Integer.toString(RafIOCalc.getInt(temp))
						// + "|";
						// }
						linhaToAux = RafIOCalc.getLineString(hafBiggerOverFlow,
								auxBucketsInMemoryInside.get(j),
								hafBiggerOverFlow.getQtCols());
						hafBiggerOverFlow.writeTuple(linhaToAux);
						numbersOfTuplesInMemoryBigger--;
						memoryUsed -= auxBucketsInMemoryInside.get(j).length;
						linhaToAux = "";
						numblockwritten = hafBiggerOverFlow.raf.getMaxBlockNo();
						// write map of blocks
						if (!vetOfhashResult.contains(numblockwritten)) {
							vetOfhashResult.add(numblockwritten);
						}
						mapOfTableBigger.put(hashResultInMapForSmaller,
								vetOfhashResult);
					}
				}
				bucketsInMemoryForBigger.remove(hashResultInMapForSmaller);
				// Ok sem este flush para grande memoria
				// hafBiggerOverFlow.flush();
			}

		}

		// calcMemory(join);
		// Verificar se tem espaco em memoria
		if (memorySizeJoinKernel > memoryUsed) {
			hafBiggerOverFlow.flush();
			sendToMemoryBigger(tupleBlock, haf, indCol, indJoinCols, join);
		} else {
			// Enviar para disco a particao da tupla tb2
			keyJoin = RafIOCalc.getKey(tupleBlockToMemory, indCol,
					haf.getQtCols());
			keys = RafIOCalc.getKeys(tupleBlockToMemory, indJoinCols,
					haf.getQtCols());
			bb = ByteBuffer.wrap(keys);
			hashResult = HashFunction.hashCode(keyJoin);
			// rowid = haf.getRowid(indCol);

			vetOfhashResult = mapOfTableBigger.get(hashResult);
			if (vetOfhashResult == null) {
				vetOfhashResult = new ArrayList<>();
			}

			// Pegar em memoria a particao e enviar para disco
			if (bucketsInMemoryForBigger.containsKey(hashResult)) {
				auxBucketsInMemory = bucketsInMemoryForBigger.get(hashResult);
				for (ByteBuffer chave : auxBucketsInMemory.keySet()) {
					numbersOfKeysInMemoryBigger--;
					memoryUsed -= 4;
					auxBucketsInMemoryInside = auxBucketsInMemory.get(chave);
					// exceeded memory for buckets
					// remove of memory and copy to disk
					byte[] keysInMemoryInside = new byte[chave.remaining()];
					for (int j = 0; j < auxBucketsInMemoryInside.size(); j++) {
						// int auxKeystoInt = 0;
						// chave.position(0);
						// chave.get(keysInMemoryInside, 0,
						// keysInMemoryInside.length);
						// for (int i = 0; i < keysInMemoryInside.length / 4;
						// i++) {
						// byte temp[] = new byte[4];
						// for (int k = 0; k < 4; k++) {
						// temp[k] = keysInMemoryInside[auxKeystoInt];
						// auxKeystoInt++;
						// }
						// linhaToAux = linhaToAux
						// + Integer.toString(RafIOCalc.getInt(temp))
						// + "|";
						// }
						linhaToAux = linhaToAux
								+ RafIOCalc.getLineString(hafBiggerOverFlow,
										auxBucketsInMemoryInside.get(j),
										hafBiggerOverFlow.getQtCols());
						hafBiggerOverFlow.writeTuple(linhaToAux);
						numbersOfTuplesInMemoryBigger--;
						memoryUsed -= auxBucketsInMemoryInside.get(j).length;
						linhaToAux = "";
						numblockwritten = hafBiggerOverFlow.raf.getMaxBlockNo();
						// write map of blocks
						if (!vetOfhashResult.contains(numblockwritten)) {
							vetOfhashResult.add(numblockwritten);
						}
						mapOfTableBigger.put(hashResult, vetOfhashResult);
					}
				}
				bucketsInMemoryForBigger.remove(hashResult);
			}
			// Adicionando a linha tb2 em questão
			// for (int i = 0; i < keys.length / 4; i++) {
			// byte temp[] = new byte[4];
			// for (int k = 0; k < 4; k++) {
			// temp[k] = keys[auxKeystoInt];
			// auxKeystoInt++;
			// }
			// linhaToAux = linhaToAux
			// + Integer.toString(RafIOCalc.getInt(temp)) + "|";
			// }
			linhaToAux = RafIOCalc.getLineString(hafBiggerOverFlow,
					tupleBlockToMemory, hafBiggerOverFlow.getQtCols());
			hafBiggerOverFlow.writeTuple(linhaToAux);
			linhaToAux = "";
			numblockwritten = hafBiggerOverFlow.raf.getMaxBlockNo();
			hafBiggerOverFlow.flush();
			if (!vetOfhashResult.contains(numblockwritten)) {
				vetOfhashResult.add(numblockwritten);
			}
			mapOfTableBigger.put(hashResult, vetOfhashResult);

			// Tambem deve ser enviado a disco a particao da tb1
			if (bucketsInMemoryForSmaller.containsKey(hashResult)) {
				auxBucketsInMemory = bucketsInMemoryForSmaller.get(hashResult);

				vetOfhashResult = mapOfTableSmaller.get(hashResult);

				if (vetOfhashResult == null) {
					vetOfhashResult = new ArrayList<>();
				}

				for (ByteBuffer chave : auxBucketsInMemory.keySet()) {
					numbersOfKeysInMemorySmaller--;
					memoryUsed -= 4;
					auxBucketsInMemoryInside = auxBucketsInMemory.get(chave);
					// exceeded memory for buckets
					// remove of memory and copy to disk
					for (int j = 0; j < auxBucketsInMemoryInside.size(); j++) {
						linhaToAux = RafIOCalc.getLineString(
								hafSmallerOverFlow,
								auxBucketsInMemoryInside.get(j),
								hafSmallerOverFlow.getQtCols());
						memoryUsed -= auxBucketsInMemoryInside.get(j).length;
						hafSmallerOverFlow.writeTuple(linhaToAux);
						numbersOfTuplesInMemorySmaller--;
						linhaToAux = "";
						numblockwritten = hafSmallerOverFlow.raf
								.getMaxBlockNo();
						// write map of blocks
						if (!vetOfhashResult.contains(numblockwritten)) {
							vetOfhashResult.add(numblockwritten);
						}
						mapOfTableSmaller.put(hashResult, vetOfhashResult);
					}
				}
				hafSmallerOverFlow.flush();
				// write map of blocks
				bucketsInMemoryForSmaller.remove(hashResult);
			}

		}

	}

	private void sendToMemoryBigger(byte[] tupleBlockToMemory, HandleFile haf,
			int indCol, int[] indJoinCols, FlashObj join) {

		keyJoin = RafIOCalc.getKey(tupleBlockToMemory, indCol, haf.getQtCols());
		keys = RafIOCalc.getKeys(tupleBlockToMemory, indJoinCols,
				haf.getQtCols());
		bb = ByteBuffer.wrap(keys);
		hashResult = HashFunction.hashCode(keyJoin);
		rowid = tupleBlockToMemory;
		if (bucketsInMemoryForBigger.containsKey(hashResult)) {
			auxBucketsInMemory = bucketsInMemoryForBigger.get(hashResult);
			// Cabe em memoria e cabe no bucket
			isThere = auxBucketsInMemory.containsKey(bb);
			if (!isThere) {
				numbersOfKeysInMemoryBigger++;
				auxBucketsInMemoryInside = new ArrayList<byte[]>();
			} else {
				auxBucketsInMemoryInside = auxBucketsInMemory.get(bb);
			}
			numbersOfTuplesInMemoryBigger++;
			memoryUsed += tupleBlockToMemory.length;
			auxBucketsInMemoryInside.add(rowid);
			auxBucketsInMemory.put(bb, auxBucketsInMemoryInside);
			bucketsInMemoryForBigger.put(hashResult, auxBucketsInMemory);

			// Se não verifica se cabe mais um bucket
		} else {
			// Adiciona em memoria
			// 16 é 4 + 12 = rowid+chave
			auxBucketsInMemory = new THashMap<ByteBuffer, ArrayList<byte[]>>();
			auxBucketsInMemoryInside = new ArrayList<byte[]>();
			auxBucketsInMemoryInside.add(rowid);
			numbersOfTuplesInMemoryBigger++;
			numbersOfKeysInMemoryBigger++;
			memoryUsed += 4;
			memoryUsed += tupleBlockToMemory.length;
			auxBucketsInMemory.put(bb, auxBucketsInMemoryInside);
			bucketsInMemoryForBigger.put(hashResult, auxBucketsInMemory);
			// Estourou vai pra disco
		}

	}

	private void sendToDiskSmaller(byte[] tupleBlockToMemory, HandleFile haf,
			int indCol, int[] indJoinCols, HandleFile hafOverFlow, FlashObj join)
			throws Exception {
		keyJoin = RafIOCalc.getKey(tupleBlockToMemory, indCol, haf.getQtCols());
		keys = RafIOCalc.getKeys(tupleBlockToMemory, indJoinCols,
				haf.getQtCols());
		bb = ByteBuffer.wrap(keys);
		hashResult = HashFunction.hashCode(keyJoin);
		// Se é firstJoin trabalha nível de byte
		// Se não é firstJoin trabalho nível de String(rowidString)
		rowid = tupleBlockToMemory;
		// Verificando as que estavam em memória
		if (bucketsInMemoryForSmaller.containsKey(hashResult)) {
			auxBucketsInMemory = bucketsInMemoryForSmaller.get(hashResult);
			vetOfhashResult = mapOfTableSmaller.get(hashResult);
			if (vetOfhashResult == null) {
				vetOfhashResult = new ArrayList<>();
			}

			for (ByteBuffer chave : auxBucketsInMemory.keySet()) {
				numbersOfKeysInMemorySmaller--;
				memoryUsed -= 4;
				auxBucketsInMemoryInside = auxBucketsInMemory.get(chave);
				// exceeded memory for buckets
				// remove of memory and copy to disk
				for (int j = 0; j < auxBucketsInMemoryInside.size(); j++) {
					linhaToAux = RafIOCalc.getLineString(hafOverFlow,
							auxBucketsInMemoryInside.get(j),
							hafOverFlow.getQtCols());
					numbersOfTuplesInMemorySmaller--;
					memoryUsed -= auxBucketsInMemoryInside.get(j).length;
					hafOverFlow.writeTuple(linhaToAux);
					linhaToAux = "";
					numblockwritten = hafOverFlow.raf.getMaxBlockNo();
					// write map of blocks
					if (!vetOfhashResult.contains(numblockwritten)) {
						vetOfhashResult.add(numblockwritten);
					}
					mapOfTableSmaller.put(hashResult, vetOfhashResult);
				}
			}

		}
		// Adicionando a linha em questão
		linhaToAux = RafIOCalc.getLineString(hafOverFlow, rowid,
				hafOverFlow.getQtCols());
		hafOverFlow.writeTuple(linhaToAux);
		linhaToAux = "";
		numblockwritten = hafOverFlow.raf.getMaxBlockNo();
		hafOverFlow.flush();

		vetOfhashResult = mapOfTableSmaller.get(hashResult);

		if (vetOfhashResult == null) {
			vetOfhashResult = new ArrayList<>();
		}

		if (!vetOfhashResult.contains(numblockwritten)) {
			vetOfhashResult.add(numblockwritten);
		}
		mapOfTableSmaller.put(hashResult, vetOfhashResult);

		bucketsInMemoryForSmaller.remove(hashResult);
	}

	private void sendToMemorySmaller(byte[] tupleBlockToMemory, HandleFile haf,
			int indCol, int[] indJoinCols, FlashObj join) {

		keyJoin = RafIOCalc.getKey(tupleBlockToMemory, indCol, haf.getQtCols());
		keys = RafIOCalc.getKeys(tupleBlockToMemory, indJoinCols,
				haf.getQtCols());
		bb = ByteBuffer.wrap(keys);
		hashResult = HashFunction.hashCode(keyJoin);
		rowid = tupleBlockToMemory;

		if (bucketsInMemoryForSmaller.containsKey(hashResult)) {
			auxBucketsInMemory = bucketsInMemoryForSmaller.get(hashResult);
			// Cabe em memoria e cabe no bucket
			isThere = auxBucketsInMemory.containsKey(bb);
			if (!isThere) {
				numbersOfKeysInMemorySmaller++;

				auxBucketsInMemoryInside = new ArrayList<byte[]>();
			} else {
				auxBucketsInMemoryInside = auxBucketsInMemory.get(bb);
			}
			numbersOfTuplesInMemorySmaller++;
			memoryUsed += tupleBlockToMemory.length;
			auxBucketsInMemoryInside.add(rowid);
			auxBucketsInMemory.put(bb, auxBucketsInMemoryInside);
			bucketsInMemoryForSmaller.put(hashResult, auxBucketsInMemory);
		} else {
			auxBucketsInMemory = new THashMap<ByteBuffer, ArrayList<byte[]>>();
			auxBucketsInMemoryInside = new ArrayList<byte[]>();
			auxBucketsInMemoryInside.add(rowid);
			numbersOfTuplesInMemorySmaller++;
			numbersOfKeysInMemorySmaller++;
			memoryUsed += 4;
			memoryUsed += tupleBlockToMemory.length;
			auxBucketsInMemory.put(bb, auxBucketsInMemoryInside);
			bucketsInMemoryForSmaller.put(hashResult, auxBucketsInMemory);
		}

	}
}
