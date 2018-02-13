package br.scmjoin;

import java.util.ArrayList;
import java.util.Scanner;


public class ReadTable {
	static int blockSize = 8192;
	static byte block[] = new byte[blockSize];
    static byte tupleBlock[];
	private static int numberOfThreads;
	private static int jump;
	private static String directory = "C:\\TPCH_10\\orders.b";
	static ArrayList<ReadBinaryThread> listOfThreads = new ArrayList<ReadBinaryThread>();
	
	public static void main(String[] args) {
		Scanner in = new Scanner(System.in);
		
		System.out.println("Numero de Threads: ");
		numberOfThreads = in.nextInt();
		System.out.println("Numero de Jump: ");
		jump = in.nextInt();
		
	    for (int i = 0; i < numberOfThreads; i++) {
			listOfThreads.add(new ReadBinaryThread(numberOfThreads, jump, directory));
		}
		if(!listOfThreads.isEmpty()){
			for (int i = 0; i < listOfThreads.size(); i++) {
				listOfThreads.get(i).start();
			}
		}
	}
}
