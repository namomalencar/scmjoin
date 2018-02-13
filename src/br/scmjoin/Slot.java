package br.scmjoin;

import br.scmjoin.RafIOCalc;

public class Slot {  
	public class Content {
		byte[] rowid;
		byte[] projectedColumns;
		byte[] finalColumns;		
	}
    Content[] listRA;
    Content[] listRB;

    boolean flagMatch;
    long size=0;
    
    public Slot(){
    }
    
    public Slot(byte[] rowid, int[] projCols, String[] finalCols, String relation){
   	 byte[] temp, tempLen;
   	 int arrayLen, ind;
   	 long addSize=0;
   	 this.flagMatch = false;
   	 Content content = new Content();
   	 
   	 content.rowid = rowid;
   	 if (projCols!=null) {
   		 content.projectedColumns = new byte[4*projCols.length];
   		 for (int i=0; i < projCols.length; i++) {
   			 temp = RafIOCalc.getByteArray(projCols[i]);
   			 for (int j=0; j < 4; j++)
   				 content.projectedColumns[j+4*i] = temp[j];
   		 }
   	 } else {
   		 content.projectedColumns = null; 
   	 }
   	 if (finalCols!=null) {
   		 arrayLen=0;
   		 for (int i=0; i < finalCols.length; i++) {
   			 temp = RafIOCalc.getByteArray(finalCols[i]);
   			 arrayLen = arrayLen + temp.length +2;
   		 }
   		 content.finalColumns = new byte[arrayLen];
   		 ind=0;
   		 for (int i=0; i < finalCols.length; i++) {
   			 temp = RafIOCalc.getByteArray(finalCols[i]);
   			 tempLen = RafIOCalc.getByteArray(temp.length);
				 content.finalColumns[ind] = tempLen[0];
			 	 ind++;
			     content.finalColumns[ind] = tempLen[1];
			     ind++;
   			 for (int j=0; j < temp.length; j++) {
   				 content.finalColumns[ind+j] = temp[j];
   			 }
   			 ind = ind + temp.length;
   		 }	    		 
   	 } else {
   		 content.finalColumns = null; 
   	 }	 
   	 if (relation.equals("A")) {
   		 listRA = new Content[1];
   		 listRA[0] = content;
//   		 addSize =  8 + content.rowid.length + 12 + 
//   				  (content.projectedColumns == null ? 0 :content.projectedColumns.length + 12) + 
//   				  (content.finalColumns == null ? 0 :content.finalColumns.length + 12);
   		 addSize =  content.rowid.length + 
 				  (content.projectedColumns == null ? 0 :content.projectedColumns.length) + 
 				  (content.finalColumns == null ? 0 :content.finalColumns.length);
   		 this.size = addSize;
   		 listRB = new Content[1];
   		 listRB[0] = null;
   		 content = null;
   	 } else {
   		 listRA = new Content[1];
   		 listRB = new Content[1];
   		 listRA[0] = null;
   		 listRB[0] = content;
//   		 addSize =  8 + content.rowid.length + 12 + 
//  				  (content.projectedColumns == null ? 0 :content.projectedColumns.length + 12) + 
//  				  (content.finalColumns == null ? 0 :content.finalColumns.length + 12);
		 addSize =  content.rowid.length + 
		  (content.projectedColumns == null ? 0 :content.projectedColumns.length) + 
		  (content.finalColumns == null ? 0 :content.finalColumns.length);
  		 this.size = addSize;
   		 content = null;
   	 }
    }
    
    public void addRowid (byte[] rowid, int[] projCols, String[] finalCols, String relation) {
   	 byte[] temp, tempLen;
   	 int arrayLen, ind;
   	 long addSize;
   	 
   	 Content content = new Content();
   	 content.rowid = rowid;
   	 if (projCols!=null) {
   		 content.projectedColumns = new byte[4*projCols.length];
   		 for (int i=0; i < projCols.length; i++) {
   			 temp = RafIOCalc.getByteArray(projCols[i]);
   			 for (int j=0; j < 4; j++)
   				 content.projectedColumns[j+4*i] = temp[j];
   		 }
   		 
   	 } else {
   		 content.projectedColumns = null; 
   	 }
   	 if (finalCols!=null) {
   		 arrayLen=0;
   		 for (int i=0; i < finalCols.length; i++) {
   			 temp = RafIOCalc.getByteArray(finalCols[i]);
   			 arrayLen = arrayLen + temp.length +2;
   		 }
   		 content.finalColumns = new byte[arrayLen];
   		 ind=0;
   		 for (int i=0; i < finalCols.length; i++) {
   			 temp = RafIOCalc.getByteArray(finalCols[i]);
   			 tempLen = RafIOCalc.getByteArray(temp.length);
			 content.finalColumns[ind] = tempLen[0];
			 ind++;
			 content.finalColumns[ind] = tempLen[1];
			 ind++;
			 
   			 for (int j=0; j < temp.length; j++) {
   				 content.finalColumns[ind+j] = temp[j];
   			 }
   			 ind = ind + temp.length;
   		 }	  
   	 } else {
   		 content.finalColumns = null; 
   	 }	 
   	 if (relation.equals("A")) {
   		 if (listRA[listRA.length-1] == null)
   			listRA[listRA.length-1] = content;
   		 else {
   			 listRA = RafIOCalc.expand(listRA, listRA.length+1); 
   			 listRA[listRA.length-1] = content;
   		 }	 
//   		 addSize =  8 + content.rowid.length + 12 + 
//  				  (content.projectedColumns == null ? 0 :content.projectedColumns.length + 12) + 
//  				  (content.finalColumns == null ? 0 :content.finalColumns.length + 12);
		 addSize =  content.rowid.length + 
		  (content.projectedColumns == null ? 0 :content.projectedColumns.length) + 
		  (content.finalColumns == null ? 0 :content.finalColumns.length);
  		 this.size = this.size + addSize;
   		 content = null;
   	 } else {
   		 if (listRB[listRB.length-1] == null)
   			listRB[listRB.length-1] = content;
   		 else {
   			 listRB = RafIOCalc.expand(listRB, listRB.length+1); 
   			 listRB[listRB.length-1] = content;
   		 }	 
   		 addSize =  content.rowid.length + 
 				  (content.projectedColumns == null ? 0 :content.projectedColumns.length) + 
 				  (content.finalColumns == null ? 0 :content.finalColumns.length);
 		 this.size = this.size + addSize;
   		 content = null;
   	 }    	
   	 if (listRA[listRA.length-1] !=null && listRB[listRB.length-1] !=null)
   		 this.flagMatch = true;
    }
    
    public void addRowid (Content content, String relation) {
      	 
       	 if (relation.equals("A")) { 
       		if (listRA == null) {
       			listRA = new Content[1];
       			listRA[0] = content;
       			listRB = new Content[1];
       			listRB[0] = null;
       		} else {
       			if (listRA[listRA.length-1] == null)
       	   			listRA[listRA.length-1] = content;
       	   		else {
       	   			 listRA = RafIOCalc.expand(listRA, listRA.length+1); 
       	   			 listRA[listRA.length-1] = content;
       	   		}
       		}	
       	 } 
       	 if (relation.equals("B")) {
       		 if (listRB == null) {
       			 listRB = new Content[1];
       			 listRB[0] = content;
       			 listRA = new Content[1];
       	   		 listRA[0] = null;
       		 } else {	 
       			 if (listRB[listRB.length-1] == null)
        			listRB[listRB.length-1] = content;
        		 else {
        			 listRB = RafIOCalc.expand(listRB, listRB.length+1); 
        			 listRB[listRB.length-1] = content;
        		 }
       		 }	 
       	 } 
    }

    public String listArrays () {
   	 String listas="";
   	  if (listRA[listRA.length-1] !=null)
   		  listas = "RA:" + listRA.toString();
   	  if (listRB[listRB.length-1] !=null) 
   		  listas = listas + "RB:" + listRB.toString();
   	  return listas;
    }
    
};

