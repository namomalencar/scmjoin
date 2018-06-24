package br.scmjoin;

import gnu.trove.iterator.TIntObjectIterator;

import java.io.BufferedReader;
import java.io.FileReader;

public class LoadFile {

	private static Histogram hist;

	public static void main(String args[]) {

		long allocatedMemory, freeMemory, usedMemory;
		Runtime runtime = Runtime.getRuntime();

		int blockSize = 8192;
		LogFile log;
		HandleFile haf;

		int key = 0;
		long qtdLines = 0;
		byte[] rowid = null, temp = null;
		String rowidS = "";
		String line = "", headerline = "", filename = "";
		BufferedReader br = null;

		int indIni, indFim, val;

		int blo = 1;
		try {

			HandleFile hafTeste = new HandleFile(blockSize);
			hafTeste.open("C:/TPCH10/supplier.b");
			hafTeste.gatherStats();
			System.out.println(hafTeste.numberOfTuples);
			System.out.println(hafTeste.numberOfBlocks);
			System.out.println(RafIOCalc.getHeaderString(hafTeste));
			byte[] block = hafTeste.nextBlock();
			block = hafTeste.readBlock(blo);
			//hafTeste.gatherStats();
			//blo++;
			while (block != null) {
				byte[] tupleBlock = hafTeste.nextTuple(block);
				while (tupleBlock != null) {
					// System.out.println(hafTeste.gatherStats(););
					// rowid = hafTeste.getRowid(1);
					System.out.println(RafIOCalc.getLineString(hafTeste, tupleBlock, hafTeste.getQtCols()));
					// String x = hafTeste.readRowid(rowid);
					// System.out.println(RafIOCalc.getRowid(rowid));
					// System.out.println(RafIOCalc.getLineString(hafTeste,
					// tupleBlock, hafTeste.getQtCols()));
					tupleBlock = hafTeste.nextTuple(block);
				}
				block = hafTeste.nextBlock();
			}

			// Depois da juncao utiliza a primeira funcao hash novamente para
			// bucketizar os rowids,
			// (a entrada da funcao hash é o numero do bloco da maior tabela
			// juntada, garantindo que na releitura duas threads não vao ler o
			// mesmo bloco)
			// dentro de cada hash será feito a ordenação pela maior tabela
			// (usar também no flashjoin).
			// Se ocorrer overflow no fetch - faz um novo hash e guarda somente
			// o mapa
			// Obs: A ordenação e a separação deve ser feita junto com o join.
			// Como é um novo operador, ele deve ter uma memoria só para ele que
			// deve ser definida"manualmente"

			System.exit(0);
			//
			//
			//
			// System.exit(0);
			// filename = "D:/TPCH/TPCH10/lineitem.b";
			// filename =
			// "C:/Users/SCMJOIN/Desktop/Proj.Pesquisa/TPCH_10/nation.b";
			// filename = "c:/RAFIO/TPCH_1/lineitem.b";
			filename = "C:/TPCH10/supplier.b";

			// br = new BufferedReader(new
			// FileReader("C:/Users/SCMJOIN/Desktop/Proj.Pesquisa/TPCH_10/nation.tbl"));
			// br = new BufferedReader(new
			// FileReader("C:/RAFIO/TPCH_1/nation.txt"));
			br = new BufferedReader(new FileReader("C:/TPCH10/supplier.tbl"));
			//
			// headerline = br.readLine();
			// System.out.println(headerline);
			// System.exit(0);

			// header of lineitem
			// headerline =
			// "orderkey[I(18)]|partkey[I(18)]|suppkey[I(18)]|l_linenumber[I(18)]|l_quantity[F(18,2)]|l_extendedprice[F(18,2)]|l_discount[F(18,2)]|l_tax[F(18,2)]|l_returnflag[A(1)]|l_linestatus[A(1)]|l_shipdate[A(30)]|l_commitdate[A(30)]|l_receiptdate[A(30)]|l_shipinstruct[A(25)]|l_shipmode[A(10)]|l_comment[A(44)]|";

			// header of partsupp
			// headerline =
			// "partkey[I(18)]|suppkey[I(18)]|ps_availqty[I(18)]|ps_supplycost[F(18,2)]|ps_comment[A(199)]|";

			// header of part
			// headerline =
			// "partkey[I(18)]|p_name[A(55)]|p_mfgr[A(25)]|p_brand[A(10)]|p_type[A(25)]|p_size[I(18)]|p_container[A(10)]|p_retailprice[F(18,2)]|p_comment[A(23)]|";

			// header of order
			// headerline =
			// "orderkey[I(18)]|custkey[I(18)]|o_orderstatus[A(1)]|o_totalprice[F(18,2)]|o_orderdate[A(30)]|o_orderpriority[A(15)]|o_clerk[A(15)]|o_shippriority[I(18)]|o_comment[A(79)]|";

			// header of customer
			// headerline =
			// "custkey[I(18)]|c_name[A(25)]|c_address[A(40)]|nationkey[I(18)]|c_phone[A(15)]|c_acctbal[F(18,2)]|c_mktsegment[A(10)]|c_comment[A(117)]|";

			// header of nation
			// headerline =
			// "nationkey[I(18)]|n_name[A(25)]|regionkey[I(18)]|n_comment[A(152)]|";

			// header of region
			// headerline =
			// "regionkey[I(18)]|r_name[A(25)]|r_comment[A(40)]|";

			// header of supplier
			headerline = "suppkey[I(18)]|s_name[A(25)]|s_address[A(40)]|nationkey[I(18)]|s_phone[A(15)]|s_acctbal[F(18,2)]|s_comment[A(10)]|";
			//

			// for (int i=1; i <= 6; i++) {
			// qtdLines = 1;
			haf = new HandleFile(blockSize);
			//
			haf.create(filename, headerline);

			// //if (i==1)
			line = br.readLine();
			//
			while (line != null && line.trim() != "") { // && qtdLines <=
														// 10000000) {
				// check partkey value for a smaller binary file
				// indIni = line.indexOf("|") +1;
				// indFim = line.indexOf("|", indIni+1);
				// val = Integer.parseInt(line.substring(indIni, indFim));
				// if (val <= 20000) // check partkey value for a smaller binary
				// file
				haf.writeTuple(line);
				line = br.readLine();
				// qtdLines++;
				// // write only part of file, a specified amount of lines
				// if (qtdLines> 80000)
				// // break;
			}
			// //
			haf.flush();
			haf.gatherStats();
			haf.close();
			haf = null;
			// }

			br.close();

			// System.exit(0);
			// System.out.println(qtdLines);

			haf = new HandleFile(blockSize);
			haf.open(filename);

			System.out.println(haf.numberOfBlocks);
			System.out.println(haf.numberOfTuples);
			System.out.println(haf.mediumSizeOfTuple);

			System.exit(0);
			haf.EBhistogram();

			haf.close();
			haf = new HandleFile(blockSize);
			haf.open(filename);
			// for (TIntObjectIterator it = haf.histogram.iterator();
			// it.hasNext();) {
			// it.advance();
			// System.out.println("Column: " + it.key() + " "
			// + haf.tupleStruct.get(it.key()).columnName);
			// hist = (Histogram) it.value();
			// System.out.println("Biggest freq value: " + hist.valBigFreq);
			// System.out.println("Biggest freq: " + hist.bigFreq);
			// System.out.println("Smallest freq value: " + hist.valSmallFreq);
			// System.out.println("Smallest freq: " + hist.smallFreq);
			// System.out.println("Media freq: " + hist.avgFreq);
			// System.out.println("Dist values: " + hist.distinctValues);
			// }

			haf.close();

			// /*
			// String linha="";
			// byte block[] = new byte[blockSize];
			// byte tupleBlock[] = new byte[blockSize];;
			//
			// // log = new LogFile();
			// // Integer indCol = haf.getColumnPos("custkey");
			// // if (indCol == null)
			// // return;
			// System.out.println("before first block read Memory:" +
			// Measure.getMemory());
			// block = haf.nextBlock();
			// qtdLines=1;
			// System.out.println("haf open - Memory:" + Measure.getMemory());
			// */
			// // while (block != null) {
			// // tupleBlock = haf.nextTuple(block);
			// //// linha = RafIOCalc.getLineString(haf,tupleBlock,
			// haf.getQtCols());
			// //// System.out.println(linha);
			// // while (tupleBlock !=null) {
			// //// key = RafIOCalc.getKey(tupleBlock, indCol, haf.getQtCols());
			// //// rowid = haf.getRowid(indCol);
			// ////
			// //// rowidS =RafIOCalc.getRowid(rowid);
			// //// if (qtdLines == 1 || qtdLines%5000 == 0)
			// //// log.writeLog(linha + "chave:" + key + " rowid:"+ rowidS);
			// // qtdLines++;
			// // tupleBlock = haf.nextTuple(block);
			// //// if (tupleBlock != null)
			// //// linha = RafIOCalc.getLineString(haf,tupleBlock,
			// haf.getQtCols());
			// //// else
			// //// linha = null;
			// // }
			// // //System.out.println("Block:" + haf.getNextBlock());
			// // //block=null;
			// // System.out.println(qtdLines + " - Memory:" +
			// Measure.getMemory());
			// // block = haf.nextBlock();
			// // //System.out.println("bytes used in this block:" +
			// haf.raf.getBlockBytesUsed());
			// // }
			// // System.out.println("finished - Memory:" +
			// Measure.getMemory());
			// //// log.writeLog(linha + "chave:" + key + " rowid:"+ rowidS);
			// //// log.writeLog("Qtd Lines:" + qtdLines);
			// //// log.closeLog();
			// // TimeUnit.SECONDS.sleep(30);
			// // System.out.println("end program - Memory:" +
			// Measure.getMemory());

		} catch (Exception e) {
			System.out.println(e.toString() + "   " + line);
		}

	}
}