package br.scmjoin;

import java.io.BufferedReader;
import java.io.FileReader;

import br.scmjoin.HandleFile;


public class CreateBinary {
	
	public static void main(String[] args) {
		String line = "";
		BufferedReader br = null;
		int blockSize = 8192;
		//byte block[] = new byte[blockSize];
	  //  byte tupleBlock[];
		HandleFile haf = new HandleFile(blockSize);				
		try {
			br = new BufferedReader(new FileReader("C:\\TPCH_10\\supplier.txt"));
			line = br.readLine();	    	 
			
		    haf.create("C:\\TPCH_10\\supplier.b", line);
		  
		    line = br.readLine();
		    while (line != null && line.trim() != "") {	 		        
		        haf.writeTuple(line);
		        line = br.readLine();
		    }	
		    haf.flush();
		    haf.gatherStats();
		    haf.close();
		    br.close();
		    
		    
			haf.open("C:\\TPCH_10\\supplier.b");
			
		//	System.out.println(haf.numberOfBlocks);
			//block = haf.readBlock(185);
			/*tupleBlock = haf.nextTuple(block);
			System.out.println(RafIOCalc.getColumn(haf, tupleBlock, 0, haf.getQtCols()));*/
				
		    
		} catch (Exception e) {	    	
			System.out.println(e.toString() + "   " + line);
		}
	}

}
