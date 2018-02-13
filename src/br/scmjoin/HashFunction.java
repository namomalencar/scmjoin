package br.scmjoin;

import java.math.BigInteger;
import java.util.Random;

public class HashFunction {
	static Random generator = new Random();
	static int a, b, hashTableSize, p;
	public static void initialize(int N) {
		p = 4 * N + 1;
		hashTableSize = N;
		while (a==0) a = 66528; //generator.nextInt(p-1);
		while (b==0) b = 107737; // generator.nextInt(p-1);
	}
	static int hashCode(long key) {
		int i = (int)((key >>> 32) + (int) key);
		return compressHashCode(i, hashTableSize);
	}

	static int hashCode(Double key) {
	  long bits = Double.doubleToLongBits(key);
	  int i = (int) (bits ^ (bits >>> 32));
	  return compressHashCode(i, hashTableSize);
	}
	
	static int hashCode(int key) {
		return compressHashCode(key, hashTableSize);
	}
	
	static int compressHashCode(int i, int N) {	
		BigInteger A = new BigInteger(String.valueOf(a));		
		BigInteger res = A.multiply(new BigInteger(String.valueOf(i)));
		res = res.add(new BigInteger(String.valueOf(b)));
		res = res.mod(new BigInteger(String.valueOf(p)));
		int hashvalue = res.intValue();
		hashvalue = hashvalue % N;
		A = null;
		res = null;
		return hashvalue;
	}
	
	public static int hashFunction(int i, int buckets) {
		int hf = i % buckets;
		return hf + 1;
	}
	
	public static void main (String args[]){
		initialize(10);
		
		int k = 0; 
		HandleFile haf = new HandleFile(8192);
		byte block[] = new byte[8192];
		byte tupleBlock[];
		int indCol=0,keyJoin=0;
		byte[] keyJ;
		int[] indCols = new int[3];
		
		
		LogFile log = new LogFile();
		/*try {
			haf.open("C:/RAFIO/data_part.b");
			indCol = haf.getColumnPos("partkey");
			block = haf.nextBlock(); //first data block
			while (block != null) {
				tupleBlock = haf.nextTuple(block); //tuple block
				while (tupleBlock != null) {
					keyJoin = RafIOCalc.getKey(tupleBlock, indCol, haf.getQtCols());
					k = hashCode(keyJoin);
					if (k < 0)
						System.out.println("key:" + keyJoin + " hash:" + k);
					log.writeLog("KeyJoin:" + String.format("%08d",keyJoin) + " hash:" +  String.format("%04d", k));
					  
					tupleBlock = haf.nextTuple(block); //tuple block
				}					
				block = haf.nextBlock();
			}	
			log.closeLog();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}*/
		
		try {
			haf.open("C:/RAFIO/data_partsupp.b");
			indCols[0] = haf.getColumnPos("suppkey");
			indCols[1] = haf.getColumnPos("partkey");
			indCols[2] = haf.getColumnPos("ps_availqty");
			block = haf.nextBlock(); //first data block
			while (block != null) {
				tupleBlock = haf.nextTuple(block); //tuple block
				while (tupleBlock != null) {
					keyJ = RafIOCalc.getKeys(tupleBlock, indCols, haf.getQtCols());
					System.out.println("key:" + keyJ.toString());
					log.writeLog("KeyJoin:" + keyJ.toString());
					  
					tupleBlock = haf.nextTuple(block); //tuple block
				}					
				block = haf.nextBlock();
			}	
			log.closeLog();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
}

