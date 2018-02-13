package br.scmjoin;


import java.nio.ByteBuffer;
import gnu.trove.map.hash.*;

public class HashJoin {
	
	int qtSlots=0;
	int qtBuckets=0;
	int qtRowids=0;
	int hashTableSize=0;
	long memoryUsed=0;
	long totTuples=0;
	
	public class Bucket {  
		 ByteBuffer[] keys;
	     Slot[] rowIDList;  
    };
		
	TIntObjectHashMap hashTable = null;
	
	public HashJoin(){
		this.hashTable = new TIntObjectHashMap();
	}
	
	synchronized void insert (byte[] key, byte[] rowid, int[] projCols, String[] finalCols, String relation){
		
		byte[] temp = {key[0], key[1], key[2], key[3]};
		try {
			
			this.hashTableSize = HashFunction.hashTableSize;
			
			int hash = HashFunction.hashCode(RafIOCalc.getInt(temp));
			
			Slot slot;		
			Bucket bucket;
			ByteBuffer bb;
			boolean found;
				
			if (this.hashTable.isEmpty() || !hashTable.contains(hash)) {				
				slot = new Slot(rowid, projCols, finalCols, relation);
				bucket = new Bucket();
				bucket.rowIDList = new Slot[1];
				bucket.rowIDList[0] = slot;
				bucket.keys = new ByteBuffer[1];
				bb = ByteBuffer.wrap(key);
				bucket.keys[0] = bb;
		    	if (slot.listRA[slot.listRA.length-1] !=null && slot.listRB[slot.listRB.length-1] !=null) 
					totTuples = totTuples + (slot.listRA.length*slot.listRB.length);    	     

				memoryUsed = memoryUsed + slot.size;
				memoryUsed = memoryUsed + key.length;
				
				hashTable.put(hash, bucket);
				this.qtSlots++;
				this.qtRowids++;
				this.qtBuckets++;
			} else {
				bucket = (Bucket)hashTable.get(hash);
				bb = ByteBuffer.wrap(key);
				found = false;
				for (int i=0; i< bucket.keys.length; i++) {
					if (bucket.keys[i].equals(bb)) {
						if (bucket.rowIDList[i].listRA[bucket.rowIDList[i].listRA.length-1] !=null && bucket.rowIDList[i].listRB[bucket.rowIDList[i].listRB.length-1] !=null) 
			    			totTuples = totTuples - (bucket.rowIDList[i].listRA.length*bucket.rowIDList[i].listRB.length);
						
						bucket.rowIDList[i].addRowid(rowid, projCols, finalCols, relation);
						this.qtRowids++;
						
			    		if (bucket.rowIDList[i].listRA[bucket.rowIDList[i].listRA.length-1] !=null && bucket.rowIDList[i].listRB[bucket.rowIDList[i].listRB.length-1] !=null) 
			    			totTuples = totTuples + (bucket.rowIDList[i].listRA.length*bucket.rowIDList[i].listRB.length);

						memoryUsed = memoryUsed + bucket.rowIDList[i].size;
						found = true;
						break;
					}						
				}
				// just insert a non existent key if the relation is the first, named A
				if (!found && relation.equals("A")){
					bb = ByteBuffer.wrap(key);
					slot = new Slot(rowid, projCols, finalCols, relation);
					this.qtSlots++;
					this.qtRowids++;
					bucket.rowIDList = RafIOCalc.expand(bucket.rowIDList, bucket.rowIDList.length+1);
					bucket.keys = RafIOCalc.expand(bucket.keys, bucket.keys.length+1);
					bucket.rowIDList[bucket.rowIDList.length-1] = slot;
					bucket.keys[bucket.keys.length-1] = bb;
					
					if (slot.listRA[slot.listRA.length-1] !=null && slot.listRB[slot.listRB.length-1] !=null) 
						totTuples = totTuples + (slot.listRA.length*slot.listRB.length);    	     

					memoryUsed = memoryUsed + slot.size;
					memoryUsed = memoryUsed + key.length;
					
					hashTable.put(hash, bucket);						
					
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
