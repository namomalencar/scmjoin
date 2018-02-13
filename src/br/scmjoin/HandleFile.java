package br.scmjoin;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * The HandleFile provides methods to treat tuples and columns of a file from
 * blocks returned by methods of RafIO.
 * 
 **/

public class HandleFile {
	private int effectiveBlockSize; // bytes used for tuples in writeTuple
									// method
	private int blockInd;
	private int nextBlock;
	private int currentBlock;
	private int qtCols;

	private int currentReadRowidBlockNo;
	private byte[] currentReadRowidBlock;
	ArrayList<Integer> readRowidBlocks;

	private static int fileNum = 0;
	private int nextTuple = 0;
	private int currentTupleId = 0;
	private int currentTupleLen = 0;
	private byte[] block, tempBlock, blockWithHeader;
	private byte[] blockRead;

	int numberOfBlocks = 0;
	int numberOfTuples = 0;
	int mediumSizeOfTuple = 0;
	int sizeOfallTuples = 0;
	int headerSize;
	int numberofWrite = 0;
	int numberofReReadbyRowId = 0;
	String header = "";

	public RafIO raf;

	public class TupleStructure {
		public String columnName;
		public String columnType;
		public String columnLength;
	}

	public ArrayList<TupleStructure> tupleStruct;

	public HandleFile(int blocksize) {
		this.blockInd = 0;
		this.effectiveBlockSize = 0;
		this.nextBlock = 0;
		this.currentBlock = 0;
		this.currentReadRowidBlockNo = -1;
		this.readRowidBlocks = null;
	}

	/**
	 * Opens an file. * Invokes a RafIO class to open a binary file *
	 * 
	 * @param filename
	 * @return RafIO
	 * @throws Exception
	 */
	public RafIO open(String fileName) throws Exception {
		// fileNum++;
		this.raf = new RafIO(fileNum, fileName);
		this.block = new byte[RafIO.getBlockSize()]; // block used for fill
														// tuples on block
		this.blockWithHeader = new byte[RafIO.getBlockSize()]; // block used for
																// fill tuples
																// on block
		this.nextBlock = 0;
		this.currentBlock = 0;
		this.currentReadRowidBlockNo = -1;
		this.readRowidBlocks = null;
		this.tupleStruct = this.setHeaderStructure(this.nextBlock());
		return raf;
	} // open

	/**
	 * Creates a file with header. * Invokes class RafIO to create a binary file
	 * for a text file *
	 * 
	 * @param filename
	 * @param header
	 *            the header of the text file
	 * @param blockSize
	 * @return RafIO
	 * @throws Exception
	 */
	public RafIO create(String filename, String header) throws Exception {
		fileNum++;
		byte[] headerBlock = RafIOCalc.getHeaderByteArray(header,
				RafIO.getBlockSize());
		this.raf = new RafIO(fileNum, filename, headerBlock);
		this.tupleStruct = this.setHeaderStructure(this.raf.readBlock(0));
		this.block = new byte[RafIO.getBlockSize()];
		this.blockWithHeader = new byte[RafIO.getBlockSize()];
		return this.raf;
	} // create

	/**
	 * Write Tuple * Fill block until blockSize and when is full write block
	 * (invokes RafIO.writeBlock) * * The layout of byte array of tuples is 1) 8
	 * bytes for containerNo, blockNo, blockType and bytesUsed for each tuple
	 * within the block 2) 2 bytes for size of tuple for each column within the
	 * tuple 3) 2 bytes for size of column 4) bytes for the column's value *
	 * 
	 * @param line
	 * @throws Exception
	 */
	public void writeTuple(String line) throws Exception {

		int byteArrayLineLen = 0;

		if (this.effectiveBlockSize == 0) {
			for (int i = 0; i < RafIO.getBlockSize(); i++) {
				this.block[i] = 0;
			}
			this.blockInd = 0;
			this.effectiveBlockSize = 0;
		}

		this.tempBlock = RafIOCalc.getLineByteArray(this, line.trim(),
				RafIO.getBlockSize());
		byteArrayLineLen = RafIOCalc.lastEffectiveBytesBlock;
		if ((this.effectiveBlockSize + RafIOCalc.lastEffectiveBytesBlock + 2) < RafIO
				.getBlockSize() - RafIO.getBlockHeaderSize()) {
			// first position of tuple in the block: amount of bytes of tuple in
			// bytes
			this.block[blockInd] = RafIOCalc.getByte(
					RafIOCalc.getByteArray(byteArrayLineLen), 0);
			this.blockInd++;
			this.block[blockInd] = RafIOCalc.getByte(
					RafIOCalc.getByteArray(byteArrayLineLen), 1);
			this.blockInd++;
			// fill the block with content of tuple

			for (int i = 0; i < RafIOCalc.lastEffectiveBytesBlock; i++) {
				this.block[blockInd] = tempBlock[i];
				this.blockInd++;
			}
		} else {
			numberofWrite++;
			this.raf.writeBlock(this.effectiveBlockSize, this.block);

			// clear the block and completeBlock for tuples of next block
			for (int i = 0; i < RafIO.getBlockSize(); i++) {
				this.block[i] = 0;
			}
			this.blockInd = 0;
			this.effectiveBlockSize = 0;
			// first position of tuple in the block: amount of bytes of tuple in
			// bytes
			this.block[blockInd] = RafIOCalc.getByte(
					RafIOCalc.getByteArray(byteArrayLineLen), 0);
			this.blockInd++;
			this.block[blockInd] = RafIOCalc.getByte(
					RafIOCalc.getByteArray(byteArrayLineLen), 1);
			this.blockInd++;
			for (int i = 0; i < RafIOCalc.lastEffectiveBytesBlock; i++) {
				this.block[blockInd] = tempBlock[i];
				this.blockInd++;
			}
		}
		this.effectiveBlockSize = this.effectiveBlockSize
				+ RafIOCalc.lastEffectiveBytesBlock + 2;
	} // writeTuple

	/**
	 * Write Tuple in Block * Fill block with the actual content of block
	 * blockNo and append the new line and write on the specified block If the
	 * new line made block to exceed the blockSize then a new block is written
	 * with line * * The layout of byte array of tuples is 1) 8 bytes for
	 * containerNo, blockNo, blockType and bytesUsed for each tuple within the
	 * block 2) 2 bytes for size of tuple for each column within the tuple 3) 2
	 * bytes for size of column 4) bytes for the column's value *
	 * 
	 * @param line
	 * @param blockNo
	 * @return blockNo
	 * @throws Exception
	 */
	public int writeTupleinBlock(String line, int blockNo) throws Exception {
		int byteArrayLineLen = 0;

		this.blockWithHeader = this.raf.readBlock(blockNo);

		this.effectiveBlockSize = this.raf.getBlockBytesUsed();
		this.blockInd = this.effectiveBlockSize + this.raf.getBlockHeaderSize();

		this.tempBlock = RafIOCalc.getLineByteArray(this, line.trim(),
				RafIO.getBlockSize());
		byteArrayLineLen = RafIOCalc.lastEffectiveBytesBlock;

		if ((this.effectiveBlockSize + RafIOCalc.lastEffectiveBytesBlock + 100) < RafIO
				.getBlockSize() - RafIO.getBlockHeaderSize()) {
			// first position of tuple in the block: amount of bytes of tuple in
			// bytes
			this.blockWithHeader[blockInd] = RafIOCalc.getByte(
					RafIOCalc.getByteArray(byteArrayLineLen), 0);
			this.blockInd++;
			this.blockWithHeader[blockInd] = RafIOCalc.getByte(
					RafIOCalc.getByteArray(byteArrayLineLen), 1);
			this.blockInd++;
			// fill the block with content of tuple
			for (int i = 0; i < RafIOCalc.lastEffectiveBytesBlock; i++) {
				this.blockWithHeader[blockInd] = tempBlock[i];
				this.blockInd++;
			}
			this.effectiveBlockSize = this.effectiveBlockSize
					+ RafIOCalc.lastEffectiveBytesBlock + 2;
			numberofWrite++;
			this.raf.writeBlock(this.effectiveBlockSize, this.blockWithHeader,
					blockNo);
			// allocate another block for next tuple
			this.blockInd = 0;
			this.effectiveBlockSize = 0;
		} else {
			// clear the block and completeBlock for tuples of next block
			for (int i = 0; i < RafIO.getBlockSize(); i++) {
				this.block[i] = 0;
			}
			this.blockInd = 0;
			this.effectiveBlockSize = 0;
			// first position of tuple in the block: amount of bytes of tuple in
			// bytes
			this.block[blockInd] = RafIOCalc.getByte(
					RafIOCalc.getByteArray(byteArrayLineLen), 0);
			this.blockInd++;
			this.block[blockInd] = RafIOCalc.getByte(
					RafIOCalc.getByteArray(byteArrayLineLen), 1);
			this.blockInd++;
			for (int i = 0; i < RafIOCalc.lastEffectiveBytesBlock; i++) {
				this.block[blockInd] = tempBlock[i];
				this.blockInd++;
			}
			this.effectiveBlockSize = this.effectiveBlockSize
					+ RafIOCalc.lastEffectiveBytesBlock + 2;
			numberofWrite++;
			blockNo = this.raf.writeBlock(this.effectiveBlockSize, this.block);
			// allocate another block for next tuple
			this.blockInd = 0;
			this.effectiveBlockSize = 0;
		}
		return blockNo;
	} // writeTupleinBlock

	/**
	 * Flush * Write last block *
	 * 
	 * @throws Exception
	 */
	public void flush() throws Exception {
		numberofWrite++;
		if (this.effectiveBlockSize == 0) {
			numberofWrite--;
		} else {
			this.raf.writeBlock(this.effectiveBlockSize, this.block);
			// clear the block for tuples of next block
			for (int i = 0; i < RafIO.getBlockSize(); i++) {
				this.block[i] = 0;
			}

			// initialize for next write
			this.blockInd = 0;
			this.effectiveBlockSize = 0;
		} // flush
	}

	/**
	 * Close * close file *
	 * 
	 * @throws Exception
	 */
	public void close() throws Exception {
		this.raf.close();
		this.blockInd = 0;
		this.effectiveBlockSize = 0;
	} // close

	/*
	 * Nextblock * read next block of raf *
	 * 
	 * @throws Exception
	 */
	public byte[] nextBlock() throws Exception {
		blockRead = this.raf.readBlock(this.nextBlock);

		this.currentTupleId = 0;
		if (blockRead == null)
			return null;

		this.currentBlock = this.nextBlock;
		this.nextBlock++;
		return blockRead;
	} // nextBlock

	/*
	 * Read Block * read block of raf with blockNo specified *
	 * 
	 * @throws Exception
	 */
	public byte[] readBlock(int blockNo) throws Exception {

		byte[] block = this.raf.readBlock(blockNo);
		this.currentBlock = blockNo;

		this.currentTupleId = 0;
		if (block == null)
			return null;

		return block;
	} // nextBlock

	/*
	 * Structure of tuple from header * fill tupleStruct *
	 * 
	 * @param byteArray array of bytes of the file's header
	 * 
	 * @return ArrayList<TupleStructure>
	 * 
	 * @throws Exception
	 */
	public ArrayList<TupleStructure> setHeaderStructure(byte[] byteArray)
			throws Exception {
		this.tupleStruct = new ArrayList<TupleStructure>();
		RafIOCalc.setHeaderStructure(this, byteArray);
		this.qtCols = this.tupleStruct.size();
		return this.tupleStruct;

	} // setHeaderStructure

	/*
	 * Next tuple * return next tuple on the block read as a array of bytes *
	 * 
	 * @param byteArray array of bytes of the file's tuple
	 * 
	 * @return byte[]
	 * 
	 * @throws Exception
	 */
	public byte[] nextTuple(byte[] byteArray) throws Exception {
		int tupleId = 0;
		if (this.currentTupleId == 0) {
			this.tempBlock = new byte[4];
			tupleId = RafIO.getBlockHeaderSize();
			this.currentTupleId = tupleId;
		} else {
			tupleId = this.currentTupleId + this.currentTupleLen + 2;
			this.currentTupleId = tupleId;
		}
		if (tupleId + 2 >= this.raf.getBlockSize())
			return null;
		this.tempBlock = new byte[4];
		this.tempBlock[0] = byteArray[tupleId];
		this.tempBlock[1] = byteArray[tupleId + 1];
		this.currentTupleLen = RafIOCalc.getInt(this.tempBlock);
		this.tempBlock = null;
		if (this.currentTupleLen == 0)
			return null;
		this.tempBlock = new byte[this.currentTupleLen + 2];
		for (int i = 0; i < this.currentTupleLen + 2; i++) {
			this.tempBlock[i] = byteArray[tupleId + i];
		}
		return this.tempBlock;
	} // nextTuple

	/*
	 * Read tuple by Id * return a tuple with the tupleId specified *
	 * 
	 * @param byteArray array of bytes of the file's tuple
	 * 
	 * @param tupleId
	 * 
	 * @return byte[]
	 * 
	 * @throws Exception
	 */
	public byte[] readTupleById(byte[] byteArray, int tupleId) throws Exception {

		if (tupleId >= this.raf.getBlockSize())
			return null;
		this.tempBlock = new byte[4];
		this.tempBlock[0] = byteArray[tupleId];
		this.tempBlock[1] = byteArray[tupleId + 1];
		this.currentTupleLen = RafIOCalc.getInt(this.tempBlock);
		this.tempBlock = null;
		if (this.currentTupleLen == 0)
			return null;
		this.tempBlock = new byte[this.currentTupleLen + 2];
		for (int i = 0; i < this.currentTupleLen + 2; i++) {
			this.tempBlock[i] = byteArray[tupleId + i];
		}
		return this.tempBlock;
	} // readTupleById

	/**
	 * Returns the index of current tuple (last read tuple with nextTuple)
	 * 
	 * @return int
	 */
	public int getCurrentTupleId() {
		return this.currentTupleId;
	} // getCurrentTupleId

	/**
	 * Returns the length of current tuple (last read tuple with nextTuple)
	 * 
	 * @return int
	 */
	public int getCurrentTupleLen() {
		return this.currentTupleLen;
	} // getCurrentTupleLen

	/**
	 * Returns the length of block
	 * 
	 * @return int
	 */
	public int getBlockSize() {
		return RafIO.getBlockSize();
	} // getBlockSize

	/**
	 * Returns the quantity of columns
	 * 
	 * @return int
	 */
	public int getQtCols() {
		return this.qtCols;
	} // getQtCols

	/**
	 * Returns the next block that will be read by nextBlock() method
	 * 
	 * @return int
	 */
	public int getNextBlock() {
		return this.nextBlock;
	} // getNextBlock

	/**
	 * Returns the blocks read by readRowid method
	 * 
	 * @return int
	 */
	public int getqtBlocksReadRowid() {
		return this.readRowidBlocks.size();
	} // getqtBlocksReadRowid

	/**
	 * Returns the position of column within the tuple
	 * 
	 * @param columnName
	 * @return Integer
	 */
	public Integer getColumnPos(String columnName) {
		Integer indCol = null;
		String structColName = null;
		for (int i = 0; i < this.tupleStruct.size(); i++) {
			structColName = ((TupleStructure) this.tupleStruct.get(i)).columnName
					.trim();
			if (structColName.equals(columnName)) {
				indCol = i;
				break;
			}
		}
		if (indCol == null)
			System.out.println("Column " + columnName
					+ " doesn't exist in the file.");
		return indCol;
	} // getColumnPos

	/**
	 * Returns true if tuple complies with conditions and false otherwise
	 * 
	 * @param condition
	 * @param tupleBlock
	 * @return boolean
	 */
	public boolean checkCondition(String condition, byte[] tupleBlock)
			throws Exception {
		Integer indCol = null;
		int indOp = 0, indC = 0, indConector = 0;
		String operator = null, conector = null, columnName = null, columnValue = null, condValue = null;
		int intcondValue, intcolumnValue;

		try {
			if (condition == null || condition.trim().equals(""))
				return true;
			indOp = condition.indexOf(">=", 0);
			if (indOp != -1)
				operator = ">=";
			else {
				indOp = condition.indexOf("<=", 0);
				if (indOp != -1)
					operator = "<=";
				else {
					indOp = condition.indexOf("<>", 0);
					if (indOp != -1)
						operator = "<>";
					else {
						indOp = condition.indexOf(">", 0);
						if (indOp != -1)
							operator = ">";
						else {
							indOp = condition.indexOf("<", 0);
							if (indOp != -1)
								operator = "<";
							else {
								indOp = condition.indexOf("=", 0);
								if (indOp != -1)
									operator = "=";
								else {
									indOp = condition.indexOf("LIKE", 0);
									if (indOp != -1)
										operator = "LIKE";
								}
							}

						}
					}
				}
			}

			while (indOp != -1) {
				columnName = condition.substring(indC, indOp).trim();
				conector = null;
				indConector = condition.indexOf(" and ", indC);
				if (indConector != -1)
					conector = "and";
				else {
					indConector = condition.indexOf(" or ", indC);
					if (indConector != -1)
						conector = "or";
				}

				if (indConector != -1) {
					if (operator.equals("LIKE"))
						condValue = condition.substring(indOp + 4, indConector)
								.trim();
					else {
						if (operator.equals("<=") || operator.equals(">=")
								|| operator.equals("<>"))
							condValue = condition.substring(indOp + 2,
									indConector).trim();
						else
							condValue = condition.substring(indOp + 1,
									indConector).trim();
					}
					if (conector.equals("and"))
						indC = indConector + 5;
					else
						indC = indConector + 4;
				} else {
					if (operator.equals("LIKE"))
						condValue = condition.substring(indOp + 4).trim();
					else {
						if (operator.equals("<=") || operator.equals(">=")
								|| operator.equals("<>"))
							condValue = condition.substring(indOp + 2).trim();
						else
							condValue = condition.substring(indOp + 1).trim();
					}
				}
				indCol = this.getColumnPos(columnName);
				if (indCol != null) {
					columnValue = RafIOCalc.getColumn(this, tupleBlock, indCol,
							this.getQtCols());
					if (this.tupleStruct.get(indCol).columnType.equals("I")) {
						intcondValue = Integer.parseInt(condValue);
						intcolumnValue = Integer.parseInt(columnValue);
						if (operator.equals("=")
								&& intcolumnValue == intcondValue
								&& (conector == null || conector.equals("or")))
							return true;
						if (operator.equals("=")
								&& !(intcolumnValue == intcondValue)
								&& (conector == null || conector.equals("and")))
							return false;

						if (operator.equals("<>")
								&& intcolumnValue != intcondValue
								&& (conector == null || conector.equals("or")))
							return true;
						if (operator.equals("<>")
								&& !(intcolumnValue != intcondValue)
								&& (conector == null || conector.equals("and")))
							return false;

						if (operator.equals(">=")
								&& intcolumnValue >= intcondValue
								&& (conector == null || conector.equals("or")))
							return true;
						if (operator.equals(">=")
								&& !(intcolumnValue >= intcondValue)
								&& (conector == null || conector.equals("and")))
							return false;

						if (operator.equals("<=")
								&& intcolumnValue <= intcondValue
								&& (conector == null || conector.equals("or")))
							return true;
						if (operator.equals("<=")
								&& !(intcolumnValue <= intcondValue)
								&& (conector == null || conector.equals("and")))
							return false;

						if (operator.equals(">")
								&& intcolumnValue > intcondValue
								&& (conector == null || conector.equals("or")))
							return true;
						if (operator.equals(">")
								&& !(intcolumnValue > intcondValue)
								&& (conector == null || conector.equals("and")))
							return false;

						if (operator.equals("<")
								&& intcolumnValue < intcondValue
								&& (conector == null || conector.equals("or")))
							return true;
						if (operator.equals("<")
								&& !(intcolumnValue < intcondValue)
								&& (conector == null || conector.equals("and")))
							return false;

					} else {
						if (operator.equals("=")
								&& columnValue.equals(condValue)
								&& (conector == null || conector.equals("or")))
							return true;
						if (operator.equals("=")
								&& !columnValue.equals(condValue)
								&& (conector == null || conector.equals("and")))
							return false;

						if (operator.equals("<>")
								&& !(columnValue.equals(condValue))
								&& (conector == null || conector.equals("or")))
							return true;
						if (operator.equals("<>")
								&& (columnValue.equals(condValue))
								&& (conector == null || conector.equals("and")))
							return false;

						if (operator.equals(">=")
								&& columnValue.compareTo(condValue) >= 0
								&& (conector == null || conector.equals("or")))
							return true;
						if (operator.equals(">=")
								&& !(columnValue.compareTo(condValue) >= 0)
								&& (conector == null || conector.equals("and")))
							return false;

						if (operator.equals("<=")
								&& columnValue.compareTo(condValue) <= 0
								&& (conector == null || conector.equals("or")))
							return true;
						if (operator.equals("<=")
								&& !(columnValue.compareTo(condValue) <= 0)
								&& (conector == null || conector.equals("and")))
							return false;

						if (operator.equals(">")
								&& columnValue.compareTo(condValue) > 0
								&& (conector == null || conector.equals("or")))
							return true;
						if (operator.equals(">")
								&& !(columnValue.compareTo(condValue) > 0)
								&& (conector == null || conector.equals("and")))
							return false;

						if (operator.equals("<")
								&& columnValue.compareTo(condValue) < 0
								&& (conector == null || conector.equals("or")))
							return true;
						if (operator.equals("<")
								&& !(columnValue.compareTo(condValue) < 0)
								&& (conector == null || conector.equals("and")))
							return false;

						if (operator.equals("LIKE")
								&& columnValue.contains(condValue)
								&& (conector == null || conector.equals("or")))
							return true;
						if (operator.equals("LIKE")
								&& !columnValue.contains(condValue)
								&& (conector == null || conector.equals("and")))
							return false;

					}

				} else
					throw new Exception("Column " + columnName
							+ " doesn't exist in the file.");

				indOp = condition.indexOf(">=", indC);
				if (indOp != -1)
					operator = ">=";
				else {
					indOp = condition.indexOf("<=", indC);
					if (indOp != -1)
						operator = "<=";
					else {
						indOp = condition.indexOf("<>", indC);
						if (indOp != -1)
							operator = "<>";
						else {
							indOp = condition.indexOf("<", indC);
							if (indOp != -1)
								operator = "<";
							else {
								indOp = condition.indexOf(">", indC);
								if (indOp != -1)
									operator = ">";
								else {
									indOp = condition.indexOf("=", indC);
									if (indOp != -1)
										operator = "=";
									else {
										indOp = condition.indexOf("LIKE", indC);
										if (indOp != -1)
											operator = "LIKE";
									}
								}
							}
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("checkCondition: " + condition + "  "
					+ columnName + " " + condValue + " " + columnValue + " "
					+ operator + " " + conector);
		}
		return true;

	} // checkCondition

	/**
	 * Returns the rowid of current tuple (least read tuple with nextTuple)
	 * 
	 * @return byte[] CCBBBBTTTTLL (CC-ContainerNo, BBBB-BlockNo, TTTT-TupleId,
	 *         LL-ColumnId)
	 */
	public byte[] getRowid(int indCol) {
		// String rowid ="";
		// int i = 0;
		byte[] rowidB = new byte[12];
		byte[] temp = RafIOCalc.getByteArray(raf.getContainerNo());
		rowidB[0] = temp[0];
		rowidB[1] = temp[1];
		temp = RafIOCalc.getByteArray(this.currentBlock);
		rowidB[2] = temp[0];
		rowidB[3] = temp[1];
		rowidB[4] = temp[2];
		rowidB[5] = temp[3];
		temp = RafIOCalc.getByteArray(this.currentTupleId);
		rowidB[6] = temp[0];
		rowidB[7] = temp[1];
		rowidB[8] = temp[2];
		rowidB[9] = temp[3];
		temp = RafIOCalc.getByteArray(indCol);
		rowidB[10] = temp[0];
		rowidB[11] = temp[1];
//		if (readRowidBlocks == null) {
//			readRowidBlocks = new ArrayList<Integer>();
//			readRowidBlocks.add(blockNo);
//		} else if (!readRowidBlocks.contains(blockNo))
//			readRowidBlocks.add(blockNo);
		// rowid = String.format("%03d", raf.getContainerNo()) +
		// String.format("%08d",this.currentBlock) +
		// String.format("%08d",this.currentTupleId) + String.format("%03d",
		// indCol);
		temp = null;
		return rowidB;
	} // getRowid

	/**
	 * Read a tuple by the rowid
	 * 
	 * @param String
	 *            rowid
	 * @return byte[] * @throws Exception
	 */
	public byte[] readRowid(String rowid) throws Exception {
		int blockNo;
		int tupleId = 0, tupleLen = 0;
		byte[] block;

		try {
			blockNo = Integer.valueOf(rowid.substring(3, 11));
			if (blockNo != currentReadRowidBlockNo) {
				numberofReReadbyRowId++;
				block = this.raf.readBlock(blockNo);
				currentReadRowidBlock = block;
				currentReadRowidBlockNo = blockNo;
				if (readRowidBlocks == null) {
					readRowidBlocks = new ArrayList<Integer>();
					readRowidBlocks.add(blockNo);
				} else if (!readRowidBlocks.contains(blockNo))
					readRowidBlocks.add(blockNo);
			} else
				block = currentReadRowidBlock;

			tupleId = Integer.valueOf(rowid.substring(11, 19));

			this.tempBlock = new byte[4];
			this.tempBlock[0] = block[tupleId];
			this.tempBlock[1] = block[tupleId + 1];
			tupleLen = RafIOCalc.getInt(this.tempBlock);
			this.tempBlock = new byte[tupleLen + 2];
			for (int i = 0; i < tupleLen + 2; i++) {
				this.tempBlock[i] = block[tupleId + i];
			}
			block = null;
		} catch (Exception e) {
			System.out.println("Rowid com erro: " + rowid);
			System.out.println("tupleId:" + tupleId + "  tupleLen:" + tupleLen);
			e.printStackTrace();
		}
		return this.tempBlock;

	} // readRowid

	/**
	 * Gather statics of file and write it on header (block 0). * Number of
	 * blocks - 4 bytes Number of tuples - 4 bytes Average size of tuple - 4
	 * bytes
	 * 
	 * @throws Exception
	 */
	public void gatherStats() throws Exception {
		String line = "";
		byte block[] = new byte[RafIO.getBlockSize()];
		byte tupleBlock[];
		numberOfBlocks = 0;
		numberOfTuples = 0;
		sizeOfallTuples = 0;
		mediumSizeOfTuple = 0;

		try {
			this.nextBlock = 1;
			block = this.nextBlock();
			while (block != null) {
				numberOfBlocks++;
				tupleBlock = this.nextTuple(block);
				line = RafIOCalc.getLineString(this, tupleBlock, this.qtCols);

				while (line != null) {
					numberOfTuples++;
					sizeOfallTuples = sizeOfallTuples + line.getBytes().length;

					tupleBlock = this.nextTuple(block);
					if (tupleBlock != null) {
						line = RafIOCalc.getLineString(this, tupleBlock,
								this.qtCols);
					} else {
						line = null;
					}
				}
				block = this.nextBlock();
			}
			mediumSizeOfTuple = sizeOfallTuples / numberOfTuples;

			block = this.raf.readBlock(0);
			byte[] stats = new byte[12];

			byte[] numBlocks = RafIOCalc.getByteArray(numberOfBlocks);
			byte[] numTuples = RafIOCalc.getByteArray(numberOfTuples);
			byte[] medTuple = RafIOCalc.getByteArray(mediumSizeOfTuple);
			for (int i = 0; i < numBlocks.length; i++) {
				stats[i] = numBlocks[i];
			}
			for (int i = 0; i < numTuples.length; i++) {
				stats[i + 4] = numTuples[i];
			}
			for (int i = 0; i < medTuple.length; i++) {
				stats[i + 8] = medTuple[i];
			}

			for (int i = 0; i < stats.length; i++) {
				block[i + this.raf.getTotalHeaderSize()] = stats[i];
			}
			numberofWrite++;
			this.raf.writeBlock(0, block, 0);
		} catch (Exception e) {
			System.out.println("gatherStats: " + line);
			e.printStackTrace();
		}

	}

	public void EBhistogram() throws Exception {
		String line = "";
		int cols[] = new int[this.tupleStruct.size()];
		byte block[] = new byte[RafIO.getBlockSize()];
		byte tupleBlock[];
		HashMap<String, Integer> mapOfhistogram;
		int indcol = 0;
		int distinctValues = 0;
		String valWithBiggerFreq = "";
		int biggerFreq = 0;
		String valWithSmallerFreq = "";
		int smallerFreq = 0;
		int avgFreq = 0;
		int intcolValue = 0;
		byte[] colId;
		byte[] qtBytesValWithBiggerFrequence;
		byte[] valWithBiggerFrequence;
		byte[] biggerFrequence;
		byte[] qtBytesValWithSmallerFrequence;
		byte[] valWithSmallerFrequence;
		byte[] smallerFrequence;
		byte[] avgFrequence;
		int position = 0, qtCols = 0, x = 0;

		String columnValue = "";

		int blockSize = this.raf.getBlockSize();
		byte[] eBhistogram = new byte[blockSize - this.headerSize];
		byte[] blockZero = new byte[this.raf.getBlockSize()];
		x = this.headerSize;
		try {
			// histogram for all columns of table
			int ind = 0;
			String colName = "";
			for (int i = 0; i < this.tupleStruct.size(); i++) {
				colName = this.tupleStruct.get(i).columnName;
				if (colName.equals("orderkey") || colName.equals("partkey")
						|| colName.equals("suppkey")
						|| colName.equals("l_quantity")
						|| colName.equals("l_returnflag")
						|| colName.equals("l_linestatus")
						|| colName.equals("l_shipdate")
						|| colName.equals("l_commitdate")
						|| colName.equals("l_receiptdate")
						|| colName.equals("l_shipmode")
						|| colName.equals("ps_availqty")
						|| colName.equals("p_brand")
						|| colName.equals("p_type") || colName.equals("p_size")
						|| colName.equals("custkey")
						|| colName.equals("o_orderstatus")
						|| colName.equals("o_orderdate")
						|| colName.equals("o_orderpriority")
						|| colName.equals("nationkey")
						|| colName.equals("c_mktsegment")
						|| colName.equals("regionkey")
						|| colName.equals("COD_AUTHOR")
						|| colName.equals("COD_BOOK")
						|| colName.equals("COD_GENRE")) {
					cols[ind] = this.getColumnPos(colName);
					ind++;
					qtCols++;
				}
			}
			for (int z = 0; z < cols.length; z++) {
				if (z != 0 && cols[z] == 0)
					continue;
				indcol = cols[z];
				mapOfhistogram = null;
				mapOfhistogram = new HashMap<String, Integer>();
				this.nextBlock = 1;
				block = this.nextBlock();
				while (block != null) {
					tupleBlock = this.nextTuple(block);
					while (tupleBlock != null) {
						columnValue = RafIOCalc.getColumn(this, tupleBlock,
								indcol, this.getQtCols());
						// key = RafIOCalc.getKey(tupleBlock, indcol,
						// this.qtCols);
						if (mapOfhistogram.containsKey(columnValue)) {
							mapOfhistogram.put(columnValue,
									mapOfhistogram.get(columnValue) + 1);
						} else {
							mapOfhistogram.put(columnValue, 1);
						}
						tupleBlock = this.nextTuple(block);
					}
					block = this.nextBlock();
				}
				distinctValues = mapOfhistogram.size();
				biggerFreq = 0;
				smallerFreq = 0;
				avgFreq = 0;
				valWithBiggerFreq = "";
				valWithSmallerFreq = "";
				colId = null;
				qtBytesValWithBiggerFrequence = null;
				valWithBiggerFrequence = null;
				biggerFrequence = null;
				qtBytesValWithSmallerFrequence = null;
				valWithSmallerFrequence = null;
				smallerFrequence = null;
				avgFrequence = null;

				for (String colval : mapOfhistogram.keySet()) {
					avgFreq = mapOfhistogram.get(colval) + avgFreq;
					if (smallerFreq == 0 && biggerFreq == 0) {
						valWithSmallerFreq = colval;
						smallerFreq = mapOfhistogram.get(colval);
						valWithBiggerFreq = colval;
						biggerFreq = mapOfhistogram.get(colval);
					}

					if (mapOfhistogram.get(colval) > biggerFreq) {
						valWithBiggerFreq = colval;
						biggerFreq = mapOfhistogram.get(colval);
					}
					if (mapOfhistogram.get(colval) < smallerFreq) {
						valWithSmallerFreq = colval;
						smallerFreq = mapOfhistogram.get(colval);
					}
				}
				if (mapOfhistogram.size() > 2)
					avgFreq = ((avgFreq - (smallerFreq + biggerFreq)) / (mapOfhistogram
							.size() - 2));
				else
					avgFreq = avgFreq / 2;

				colId = RafIOCalc.getByteArray(indcol);
				if (this.tupleStruct.get(indcol).columnType.equals("I")) {
					intcolValue = Integer.parseInt(valWithBiggerFreq);
					valWithBiggerFrequence = RafIOCalc
							.getByteArray(intcolValue);
				} else {
					valWithBiggerFrequence = RafIOCalc
							.getByteArray(valWithBiggerFreq);
				}
				biggerFrequence = RafIOCalc.getByteArray(biggerFreq);
				if (this.tupleStruct.get(indcol).columnType.equals("I")) {
					if (valWithSmallerFreq.equals(""))
						valWithSmallerFreq = "0";
					intcolValue = Integer.parseInt(valWithSmallerFreq);
					valWithSmallerFrequence = RafIOCalc
							.getByteArray(intcolValue);
				} else {
					valWithSmallerFrequence = RafIOCalc
							.getByteArray(valWithSmallerFreq);
				}
				smallerFrequence = RafIOCalc.getByteArray(smallerFreq);
				avgFrequence = RafIOCalc.getByteArray(avgFreq);

				for (int i = 0; i < 4; i++) {
					eBhistogram[position] = colId[i];
					position++;
				}
				qtBytesValWithBiggerFrequence = RafIOCalc
						.getByteArray(valWithBiggerFrequence.length);
				for (int i = 0; i < 4; i++) {
					eBhistogram[position] = qtBytesValWithBiggerFrequence[i];
					position++;
				}
				for (int i = 0; i < valWithBiggerFrequence.length; i++) {
					eBhistogram[position] = valWithBiggerFrequence[i];
					position++;
				}
				for (int i = 0; i < 4; i++) {
					eBhistogram[position] = biggerFrequence[i];
					position++;
				}
				qtBytesValWithSmallerFrequence = RafIOCalc
						.getByteArray(valWithSmallerFrequence.length);
				for (int i = 0; i < 4; i++) {
					eBhistogram[position] = qtBytesValWithSmallerFrequence[i];
					position++;
				}
				for (int i = 0; i < valWithSmallerFrequence.length; i++) {
					eBhistogram[position] = valWithSmallerFrequence[i];
					position++;
				}
				for (int i = 0; i < 4; i++) {
					eBhistogram[position] = smallerFrequence[i];
					position++;
				}
				for (int i = 0; i < avgFrequence.length; i++) {
					eBhistogram[position] = avgFrequence[i];
					position++;
				}
				byte[] distinctValuesB = RafIOCalc.getByteArray(distinctValues);
				for (int i = 0; i < 4; i++) {
					eBhistogram[position] = distinctValuesB[i];
					position++;
				}
			}

			blockZero = this.raf.readBlock(0);
			byte[] numCols = RafIOCalc.getByteArray(qtCols);
			for (int j = 0; j < 4; j++) {
				blockZero[x] = numCols[j];
				x++;
			}
			for (int i = 0; i < position; i++) {
				blockZero[x] = eBhistogram[i];
				x++;
			}
			numberofWrite++;
			this.raf.writeBlock(0, blockZero, 0);
		} catch (Exception e) {
			System.out.println("EBhistogram:" + line);
			e.printStackTrace();
		}

	}

}