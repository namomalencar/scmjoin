package br.scmjoin;
import java.io.BufferedReader;
import java.io.FileReader;


public class GCCount {


		public static void main (String args[]) {
			
			String line = "";
			String seconds = "";
			double tot = 0;
			BufferedReader br = null;
			
			int indIni, indFim;
			double val;
			String filename = "C:/Mestrado/Teses/Resultados/RESULTSCM/LastVersion_tpch10_8kb/lsj_10_b_tt.txt";
			System.out.println(filename);
					
		     try {
	 	    	
		    	 br = new BufferedReader(new FileReader(filename));
		    	 line = br.readLine();
	 	    		
	 	    	 while (line != null && line.trim() != ""){
	 	    		    if (line.indexOf("GC") >0) {
		 	    			indIni = line.indexOf(",") + 1;
		 	    			indFim = line.indexOf("sec", indIni+1) - 1;
		 	    			val = Double.parseDouble(line.substring(indIni, indFim));
		 	    			tot = tot + val;
	 	    		    }	
	 	    			line = br.readLine();
 	    		}
		    } catch (Exception e) {	  
				e.printStackTrace();
		    } 
	 	    	 
 	    	System.out.println(tot);
	}
}
