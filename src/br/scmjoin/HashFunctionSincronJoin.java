package br.scmjoin;

import java.math.BigInteger;
import java.util.Random;

public class HashFunctionSincronJoin {

	static Random generator = new Random();
	int hashTableSize;
	static int a, b, p;

	public HashFunctionSincronJoin(int N) {
		p = 4 * N + 1;
		hashTableSize = N;
		while (a == 0)
			a = generator.nextInt(p - 1);
		while (b == 0)
			b = generator.nextInt(p - 1);
	}

	public HashFunctionSincronJoin() {
		// TODO Auto-generated constructor stub
	}

	public int hashCode(long key) {
		int i = (int) ((key >>> 32) + (int) key);
		return compressHashCode(i, hashTableSize);
	}

	public int hashCode(Double key) {
		long bits = Double.doubleToLongBits(key);
		int i = (int) (bits ^ (bits >>> 32));
		return compressHashCode(i, hashTableSize);
	}

	public int hashCode(int key) {
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

	public  int hashFunction(int i, int buckets) {
		int hf = i % buckets;
		return hf + 1;
	}
}
