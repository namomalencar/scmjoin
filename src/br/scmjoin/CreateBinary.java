package br.scmjoin;

import java.io.BufferedReader;
import java.io.FileReader;


public class CreateBinary {
	
	public static void main(String[] args) {
		String line = "";
		BufferedReader br = null;
		int blockSize = 8192;
		//byte block[] = new byte[blockSize];
	  //  byte tupleBlock[];
		HandleFile haf = new HandleFile(blockSize);				
		try {
			br = new BufferedReader(new FileReader("C:/Users/'Hamilton/Desktop/supplier.txt"));
			line = br.readLine();	    	 
			
		    haf.create("C:/Users/'Hamilton/Desktop/supplier.b", line);
		  
		    line = br.readLine();
		    while (line != null && line.trim() != "") {	 		        
		        haf.writeTuple(line);
		        line = br.readLine();
		    }	
		    haf.flush();
		    haf.gatherStats();
		    haf.close();
		    br.close();
		    
		    
			haf.open("C:/Users/'Hamilton/Desktop/supplier.b");
			
		//	System.out.println(haf.numberOfBlocks);
			//block = haf.readBlock(185);
			/*tupleBlock = haf.nextTuple(block);
			System.out.println(RafIOCalc.getColumn(haf, tupleBlock, 0, haf.getQtCols()));*/
				
		    
		} catch (Exception e) {	    	
			System.out.println(e.toString() + "   " + line);
		}
	}

}
