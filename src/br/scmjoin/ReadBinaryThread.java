package br.scmjoin;

public class ReadBinaryThread extends Thread {
	static int nid = 0;
	private int id, firstBlock, next, jump, currentBlock; 
	int blockSize = ReadTable.blockSize; 
	byte[] block = ReadTable.block; 
	byte[] tupleBlock, rowid;
	String directory, x;
	
	public ReadBinaryThread(int nThreads, int jump, String directory) {
		this.id = nid++;
		this.firstBlock = (id * jump) + 1;
		this.jump = jump;
		this.next = nThreads * jump;
		this.directory = directory;
	}

	public void run() {
		long start = System.currentTimeMillis();
		HandleFile haf = null, haflg = null;
		try {
			haf = new HandleFile(blockSize);
			haf.open(directory);
			//haf.gatherStats();
			System.out.println("Blocos: " +haf.numberOfBlocks);
			System.out.println("Tuplas: " +haf.numberOfTuples);
			//haflg = new HandleFile(blockSize);
			//haflg.create("C:\\TPCH10\\teste\\"+id+".b", RafIOCalc.getHeaderString(haf));
			block = haf.readBlock(firstBlock);
			currentBlock = firstBlock;
			while (block != null) {
				for (int j = 0; j < jump; j++) {
					if (block == null) break;
					tupleBlock = haf.nextTuple(block);
					while (tupleBlock != null) {
						rowid = haf.getRowid(0, currentBlock);
						x = RafIOCalc.getLineString(haf, tupleBlock, haf.getQtCols());
						//haflg.writeTuple(x);
						tupleBlock = haf.nextTuple(block);
					}
					currentBlock++;
					block = haf.readBlock(currentBlock);
				}
				firstBlock += next;
				currentBlock = firstBlock;
				System.out.println("Thread: "+id+ "  Bloco: "+currentBlock);
				block = haf.readBlock(firstBlock);
			}
			//haflg.flush();
			//haflg.gatherStats();
			//haflg.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}finally{
			System.out.println("Thread "+id+" Encerrada, N de Blocos Lidos: "+haf.getqtBlocksReadRowid()+", Tempo de Execução : "+(System.currentTimeMillis()-start));
		}

	}

}
