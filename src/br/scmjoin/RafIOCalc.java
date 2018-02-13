package br.scmjoin;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;

import br.scmjoin.HandleFile.TupleStructure;


/**
 * The RafIOCalc provides methods for get bytes from int and String and vice versa.
 * Methods to translate a byte array to tuple.
 * 
 **/
public class RafIOCalc {
	public static String colSeparator = "|";
	public static String typeBeforeSeparator = "[";
	public static String typeAfterSeparator = "]";
	public static String lengthBeforeSeparator = "(";
	public static String lengthAfterSeparator = ")";
	public static int lastEffectiveBytesBlock; //bytes in last execution of getLineByteArray method
	
	/**
     * Returns the int for a array of bytes.
     * @param byteArray  array of bytes
     * @return int
     */
	public static int getInt(byte[] byteArray) {
	    final ByteBuffer bb = ByteBuffer.wrap(byteArray);
	    bb.order(ByteOrder.LITTLE_ENDIAN);
	    return bb.getInt();
	} //getInt
	
	/**
     * Returns the String for a array of bytes.
     * @param byteArray  array of bytes
     * @return String
     */
	public static String getString(byte[] byteArray) {
		ByteBuffer b = ByteBuffer.wrap(byteArray);
		return new String(b.array(), Charset.defaultCharset());		
	} //getString

	/**
     * Returns the array of bytes for the int i.
     * @param i int
     * @return byte[]
     */
	public  static byte[] getByteArray(int i) {
	    final ByteBuffer bb = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
	    bb.order(ByteOrder.LITTLE_ENDIAN);
	    bb.putInt(i);
	    return bb.array();
	} //getByteArray
	
	/**
     * Returns the array of bytes for the String s.
     * @param s string
     * @return byte[]
     */
	public static byte[] getByteArray(String s) {	
		 char[] charArray = s.toCharArray();
		 final CharBuffer cbuf = CharBuffer.wrap(charArray);
		 final ByteBuffer bbuf = Charset.defaultCharset().encode(cbuf);
		 return bbuf.array();
	} // getByteArray
	
	/**
     * Returns the byte in the array on an especific position
     * @param byteArray
     * @param position
     * @return byte
     */
	public  static byte getByte(byte[] byteArray, int position) {
		return byteArray[position];		
	} // getByte
	
	 /**
     * Build a byte array from the string header
     * The header layout is col1name[col1type(col1length)];col2name[col2type(col2length)]; and so on
     * *
     * the byte array layout is 
     * <length of the string col1name in bytes><col1name in bytes><length of the string col1type in bytes><col1type in bytes><length of col1length in bytes><col1length in bytes>
     * *
     * @param header the string of header
     * @param blocksize
     * @return byte[]
     */	
	public static byte[] getHeaderByteArray(String header, int blockSize){
		int ind, p, pColSep;
		String colName, colType, colLen;
		byte[] byteArray = new byte[blockSize], temp1, temp2, headerArray;
		pColSep = header.indexOf(colSeparator,0);
		p = 0;
		ind = 0;
		while (pColSep != -1) {			
			colName = header.substring(p, header.indexOf(typeBeforeSeparator,p));
			colType = header.substring(header.indexOf(typeBeforeSeparator,p)+1, header.indexOf(lengthBeforeSeparator,p));
			colLen = header.substring(header.indexOf(lengthBeforeSeparator,p)+1,header.indexOf(lengthAfterSeparator,p));
			//column name in bytes
			temp1 = RafIOCalc.getByteArray(colName); 
			temp2 = RafIOCalc.getByteArray(temp1.length);
			byteArray[ind] = RafIOCalc.getByte(temp2, 0);
			ind++;
			for (int i = 0; i < temp1.length; i++){
				byteArray[ind] = RafIOCalc.getByte(temp1,i);
				ind++;
			}			
			//column type in bytes
			temp1 = RafIOCalc.getByteArray(colType); 
			temp2 = RafIOCalc.getByteArray(temp1.length);
			byteArray[ind] = RafIOCalc.getByte(temp2, 0);
			ind++;
			for (int i = 0; i < temp1.length; i++){
				byteArray[ind] = RafIOCalc.getByte(temp1,i);
				ind++;
			}
			//column length in bytes
			temp1 = RafIOCalc.getByteArray(colLen); 
			temp2 = RafIOCalc.getByteArray(temp1.length);
			byteArray[ind] = RafIOCalc.getByte(temp2, 0);
			ind++;
			for (int i = 0; i < temp1.length; i++){
				byteArray[ind] = RafIOCalc.getByte(temp1,i);
				ind++;
			}
			p = pColSep +1;
			pColSep = header.indexOf(colSeparator,pColSep+1);			
		}
		headerArray = new byte[ind];
		for (int i=0; i < ind; i++)
			headerArray[i] = byteArray[i];
		byteArray = null;
		return headerArray;		
	} // getHeaderByteArray
	
	/**
     * Return the string for the byte array of header
     * The byte array layout is 
     * <length of the string col1name in bytes><col1name in bytes><length of the string col1type in bytes><col1type in bytes><length of col1length in bytes><col1length in bytes>
     * *     
     * * The header string layout is col1name[col1type(col1length)];col2name[col2type(col2length)]; and so on
     * * 
     * @param headerByteArray
     * @return String
     */
	public static String getHeaderString(byte[] headerByteArray){
		int containerNo, blockSize, pageTypeUnused, maxBlockNo, qtdBytes, ind, cont;
		String colName, colType, colLen, retorno;
		byte[] tempStr;
		
		byte[] temp = {headerByteArray[0],0,0,0};		
		containerNo = RafIOCalc.getInt(temp);
		retorno = containerNo + colSeparator;
		
		byte[] temp1 = {headerByteArray[1], headerByteArray[2], headerByteArray[3],0};
		blockSize = RafIOCalc.getInt(temp1);
		retorno = retorno + blockSize + colSeparator;
		
		byte[] temp2  = {headerByteArray[4],0,0,0};
		pageTypeUnused = RafIOCalc.getInt(temp2);
		retorno = retorno + pageTypeUnused + colSeparator;
		
		byte[] temp3  = {headerByteArray[5],headerByteArray[6],headerByteArray[7],headerByteArray[8]};
		maxBlockNo = RafIOCalc.getInt(temp3);
		retorno = retorno + maxBlockNo + colSeparator;
		
		byte[] tempqtd = new byte[4];
		tempqtd[0] = headerByteArray[11];
		qtdBytes = RafIOCalc.getInt(tempqtd);
		
		ind = 12;
		cont = 1;
		while (qtdBytes != 0) {
			tempStr = null;
			if (qtdBytes < 4) {
				tempStr = new byte[4];
				for (int i=0; i < 4; i++) tempStr[i]=0;
			}
			else {
				tempStr = new byte[qtdBytes];
			}
			for (int i =0; i < qtdBytes; i++){
				tempStr[i] = headerByteArray[ind];
				ind++;
			}
			if (cont == 1) {
				colName = RafIOCalc.getString(tempStr);
				retorno = retorno + colName.trim() + colSeparator;
			}	
			if (cont == 2) {
				colType = RafIOCalc.getString(tempStr);
				retorno = retorno + colType.trim() + colSeparator;
			}
			if (cont == 3){
				colLen = RafIOCalc.getString(tempStr);
				retorno = retorno + colLen.trim() + colSeparator;
				cont = 0;
			}
			cont++;
			tempqtd[0] = headerByteArray[ind]; 
			ind++;
			qtdBytes = RafIOCalc.getInt(tempqtd);		
		}
		return retorno;		
	} // getHeaderString
	
	/**
     * Set the header structure (layout) on ArrayList tupleStruct of HandleFile from the byte array of header.
     *  TupleStructure{
	 *		String columnName;
	 *		String columnType;
	 *		String columnLength;
	 *	}
	 * ArrayList<TupleStructure> tupleStruct;
     *     
     * * 
     * @param haf pointer for HandleFile instance
     * @param headerByteArray
     */
	public static void setHeaderStructure(HandleFile haf, byte[] headerByteArray) {
		
		TupleStructure tps=null;
		int containerNo, blockSize, pageTypeUnused, qtdBytes, headerSize, maxBlockNo, ind, cont;
		String colName, colType, colLen, retorno;
		byte[] tempStr;
		
		byte[] temp = {headerByteArray[0],0,0,0};		
		containerNo = RafIOCalc.getInt(temp);
		retorno = containerNo + colSeparator;
		
		byte[] temp1 = {headerByteArray[1], headerByteArray[2], headerByteArray[3],0};
		blockSize = RafIOCalc.getInt(temp1);
		retorno = retorno + blockSize + colSeparator;
		
		byte[] temp2  = {headerByteArray[4],0,0,0};
		pageTypeUnused = RafIOCalc.getInt(temp2);
		retorno = retorno + pageTypeUnused + colSeparator;
		
		byte[] temp3  = {headerByteArray[5],headerByteArray[6],headerByteArray[7],headerByteArray[8]};
		maxBlockNo = RafIOCalc.getInt(temp3);
		retorno = retorno + maxBlockNo + colSeparator;
		
		byte[] temp4  = {headerByteArray[9],headerByteArray[10],0,0};
		headerSize = RafIOCalc.getInt(temp4);
		retorno = retorno + headerSize + colSeparator;
		
		byte[] tempqtd = new byte[4];
		tempqtd[0] = headerByteArray[11];  
		qtdBytes = RafIOCalc.getInt(tempqtd);
		
		ind = 12;
		cont = 1;
		while (ind < headerSize + 12) {
			tempStr = null;
			if (qtdBytes < 4) {
				tempStr = new byte[4];
				for (int i=0; i < 4; i++) tempStr[i]=0;
			}
			else {
				tempStr = new byte[qtdBytes];
			}
			for (int i =0; i < qtdBytes; i++){
				tempStr[i] = headerByteArray[ind];
				ind++;
			}			
					
			if (cont == 1) {
				tps = haf.new TupleStructure();
				colName = RafIOCalc.getString(tempStr);
				tps.columnName = colName.trim();
			}	
			if (cont == 2) {
				colType = RafIOCalc.getString(tempStr);
				tps.columnType = colType.trim();
			}
			if (cont == 3){
				colLen = RafIOCalc.getString(tempStr);
				tps.columnLength = colLen.trim();
				cont = 0;
				haf.tupleStruct.add(tps);
			}
			cont++;
			tempqtd[0] = headerByteArray[ind]; 
			ind++;
			qtdBytes = RafIOCalc.getInt(tempqtd);		
		}	
		ind--;
		// statistics
		for (int i=0; i < 4; i++) 
			tempqtd[i] = headerByteArray[ind + i];			
		haf.numberOfBlocks = RafIOCalc.getInt(tempqtd);
		ind = ind + 4;
		
		for (int i=0; i < 4; i++) 
			tempqtd[i] = headerByteArray[ind + i];			
		haf.numberOfTuples = RafIOCalc.getInt(tempqtd);
		ind = ind + 4;
		
		for (int i=0; i < 4; i++) 
			tempqtd[i] = headerByteArray[ind + i];			
		haf.mediumSizeOfTuple = RafIOCalc.getInt(tempqtd);
		ind = ind + 4;
		//header size including statistics
		haf.headerSize = ind;

		//histogram of cols
 		byte[] numCols = new byte[4];
 		for (int i = 0; i < 4; i++) 
 			numCols[i] = headerByteArray[ind + i];
 		ind = ind + 4;
 		int numColsInt = RafIOCalc.getInt(numCols);
 		if (numColsInt==0)
 			return;
 		int qtBytes=0;
		byte[] colId = new byte[4];
		byte[] qtBytesValWithBiggerFrequence = new byte[4];
		byte[] valWithBiggerFrequence;
		byte[] biggerFrequence = new byte[4];
		byte[] qtBytesValWithSmallerFrequence = new byte[4];
		byte[] valWithSmallerFrequence;
		byte[] smallerFrequence = new byte[4];
		byte[] avgFrequence = new byte[4];
		byte[] distinctValues = new byte[4];
		int intColValue=0;
		
		Histogram hist;

 		for (int z = 0; z < numColsInt; z++) {
 			for (int i = 0; i < 4; i++) 
 				colId[i] = headerByteArray[ind+i];
 			ind = ind + 4;
 			for (int i = 0; i < 4; i++)
 				qtBytesValWithBiggerFrequence[i] = headerByteArray[ind+i];
 			ind = ind + 4;
 			qtBytes = RafIOCalc.getInt(qtBytesValWithBiggerFrequence);
 			valWithBiggerFrequence = new byte[qtBytes];
 			for (int i = 0; i < qtBytes; i++)
 				valWithBiggerFrequence[i] = headerByteArray[ind+i];
 			ind = ind + qtBytes;
 			for (int i = 0; i < 4; i++)
 				biggerFrequence[i] = headerByteArray[ind+i];
 			ind = ind + 4;
 			for (int i = 0; i < 4; i++)
 				qtBytesValWithSmallerFrequence[i] = headerByteArray[ind+i];
 			ind = ind + 4;
 			qtBytes = RafIOCalc.getInt(qtBytesValWithSmallerFrequence);
 			valWithSmallerFrequence = new byte[qtBytes];
 			for (int i = 0; i < qtBytes; i++)
 				valWithSmallerFrequence[i] = headerByteArray[ind+i];
 			ind = ind + qtBytes;
 			for (int i = 0; i < 4; i++)
 				smallerFrequence[i] = headerByteArray[ind+i];
 			ind = ind + 4;
 			for (int i = 0; i < 4; i++)
 				avgFrequence[i] = headerByteArray[ind+i];
 			ind = ind + 4;
 			for (int i = 0; i < 4; i++)
 				distinctValues[i] = headerByteArray[ind+i];
 			ind = ind + 4;			
 			
 			hist = new Histogram();
			if (haf.tupleStruct.get(RafIOCalc.getInt(colId)).columnType.equals("I")){
				intColValue = RafIOCalc.getInt(valWithBiggerFrequence);
				hist.valBigFreq = String.valueOf(intColValue);
			} else {
				hist.valBigFreq = RafIOCalc.getString(valWithBiggerFrequence);
			}
			hist.bigFreq = RafIOCalc.getInt(biggerFrequence);
			if (haf.tupleStruct.get(RafIOCalc.getInt(colId)).columnType.equals("I")){
				intColValue = RafIOCalc.getInt(valWithSmallerFrequence);
				hist.valSmallFreq = String.valueOf(intColValue);
			} else {
				hist.valSmallFreq = RafIOCalc.getString(valWithSmallerFrequence);
			}
			hist.smallFreq = RafIOCalc.getInt(smallerFrequence);
			hist.avgFreq = RafIOCalc.getInt(avgFrequence);
			hist.distinctValues = RafIOCalc.getInt(distinctValues);
// 			haf.histogram.put(RafIOCalc.getInt(colId), hist);
 		}	
			
	} // setHeaderStructure
	
	public static Slot.Content[] expand(Slot.Content[] array, int size) {
	    Slot.Content[] temp = new Slot.Content[size];
	    System.arraycopy(array, 0, temp, 0, array.length);
	    for(int j = array.length; j < size; j++)
	        temp[j] = null;
	    return temp;
	}
	
	public static ByteBuffer[] expand(ByteBuffer[] array, int size) {
		ByteBuffer[] temp = new ByteBuffer[size];
	    System.arraycopy(array, 0, temp, 0, array.length);
	    for(int j = array.length; j < size; j++)
	        temp[j] = null;
	    return temp;
	}
	
	public static Slot[] expand(Slot[] array, int size) {
		Slot[] temp = new Slot[size];
	    System.arraycopy(array, 0, temp, 0, array.length);
	    for(int j = array.length; j < size; j++)
	        temp[j] = null;
	    return temp;
	}
	
	public static byte[] expand(byte[] array, int size) {
		byte[] temp = new byte[size];
	    System.arraycopy(array, 0, temp, 0, array.length);
	    for(int j = array.length; j < size; j++)
	        temp[j] = 0;
	    return temp;
	}
	
	/**
     * Return the string for the TupleStructure of HandleFile
     * *     
     * * The header string layout is col1name[col1type(col1length)];col2name[col2type(col2length)]; and so on
     * * 
     * @param HandleFile
     * @return String
     */
	public static String getHeaderString(HandleFile haf){
		String headerString="";
		for (int i=0; i < haf.tupleStruct.size(); i++){
			headerString = headerString + ((TupleStructure)haf.tupleStruct.get(i)).columnName + "[" + ((TupleStructure)haf.tupleStruct.get(i)).columnType + "(" + ((TupleStructure)haf.tupleStruct.get(i)).columnLength + ")]" + colSeparator;	
		}
		return headerString;
	}
	/**
     * Build a byte array from the string line
     * *
     * The line layout is 
     * column1value;column2value; and so on finilized with ;
     * *
     * The byte array layout is 
     * <amount of bytes of col1value><col1value in bytes><amount of bytes of col2value><col2value in bytes> and so on
     * *
     * @param line the string of line
     * @param blocksize
     * @return byte[]
     */
	public static byte[] getLineByteArray(HandleFile haf, String line, int blockSize){
		int ind=0, p=0, pColSep, indCol=0;
		String lineContent;
		int intContent;
		String colType;
		byte[] byteArray = new byte[blockSize], temp1, temp2;
		pColSep = line.indexOf(colSeparator,0);
		
		while (pColSep != -1) {			
			lineContent = line.substring(p, pColSep);
			//line content in bytes
			colType = haf.tupleStruct.get(indCol).columnType;
			if (colType.trim().equals("I")) { 
				intContent = Integer.parseInt(lineContent);
				temp1 = RafIOCalc.getByteArray(intContent);
			} else  {
				temp1 = RafIOCalc.getByteArray(lineContent); 
			}				
			
			temp2 = RafIOCalc.getByteArray(temp1.length);
			byteArray[ind] = RafIOCalc.getByte(temp2, 0);
			ind++;
			byteArray[ind] = RafIOCalc.getByte(temp2, 1);
			ind++;
			for (int i = 0; i < temp1.length; i++){
				byteArray[ind] = RafIOCalc.getByte(temp1,i);
				ind++;
			}			
			p = pColSep +1;
			pColSep = line.indexOf(colSeparator,pColSep+1);	
			indCol++;
		}
		lastEffectiveBytesBlock = ind;
		return byteArray;		
	} //getLineByteArray

	/**
     * Returns the String for a byte array of one line (or tuple)
     * *
     * *
     * The line layout is 
     * column1value;column2value; and so on finilized with ;
     * *
     * @param lineByteArray byte array of tuples
     * @param qtdCols amount of columns on tuple
     * @return String
     */
	public static String getLineString(HandleFile haf, byte[] lineByteArray, int qtdCols){
		int qtdBytes=0, ind=0, indCol=0, bytesLine=0, qtdBytesLine=0;
		String lineContent="", retorno="", colType="";
		byte[] tempStr, tempqtd = new byte[4];
		int intContent;
		
		try {
			ind = 0;
			tempqtd[0] = lineByteArray[ind]; 
			ind++;
			tempqtd[1] = lineByteArray[ind]; 
			tempqtd[2] = 0;
			tempqtd[3] = 0;
			qtdBytesLine = RafIOCalc.getInt(tempqtd);
			ind++;	
			
			if (qtdBytesLine==0) return null;
			
			
			tempqtd[0] = lineByteArray[ind]; 
			ind++;
			tempqtd[1] = lineByteArray[ind];
			tempqtd[2] = 0;
			tempqtd[3] = 0;
			qtdBytes = RafIOCalc.getInt(tempqtd);
			ind++;
			
			bytesLine = qtdBytes;
			
			while (qtdBytes != 0) {
				tempStr = null;
				if (qtdBytes < 4) {
					tempStr = new byte[4];
					for (int i=0; i < 4; i++) tempStr[i]=0;
				}
				else {
					tempStr = new byte[qtdBytes];
				}
				for (int i =0; i < qtdBytes; i++){
					tempStr[i] = lineByteArray[ind];
					ind++;
				}
				colType = haf.tupleStruct.get(indCol).columnType;
				if (colType.trim().equals("I")) { 
					intContent = RafIOCalc.getInt(tempStr);
					lineContent = String.valueOf(intContent);				
				} else {
					lineContent = RafIOCalc.getString(tempStr);
				}	
				
				retorno = retorno.trim() + lineContent.trim() + colSeparator;
				if (bytesLine + qtdCols*2 + 2 >= qtdBytesLine){
					bytesLine =0;
					retorno = retorno.trim();
					qtdBytes = 0;
					break;
				}	
				tempqtd[0] = lineByteArray[ind]; 
				ind++;
				tempqtd[1] = lineByteArray[ind]; 
				ind++;
				qtdBytes = RafIOCalc.getInt(tempqtd);
				bytesLine = bytesLine  + qtdBytes;
				indCol++;
			}
		} catch (Exception e) {
			System.out.println("qtdbytes col:" + qtdBytes + "  BytesLine:" + qtdBytesLine + "  Retorno:" + retorno + "  QtdCols:" + qtdCols + "  bytesLine:" + bytesLine + " lineByteArray len:" + lineByteArray.length + " linecontent:" + lineContent);
			e.printStackTrace();
			return null;
		}
		return retorno;		
	} //getLineString

	/**
     * Returns the value of key (column which is join attribute)
     * *
     * @param tupleByteArray byte array of tuple
     * @param indCol index of the column
     * @param qtdCols amount of columns on tuple
     * @return Integer
     */
	public static Integer getKey(byte[] tupleByteArray, int indCol, int qtdCols){
		int qtdBytes, ind=0, bytesLine, qtdBytesLine, indColAux;
		byte[] tempStr, tempqtd = new byte[4];
		int key;
		
		ind = 0;
		tempqtd[0] = tupleByteArray[ind]; 
		ind++;
		tempqtd[1] = tupleByteArray[ind];
		tempqtd[2] = 0;
		tempqtd[3] = 0;
		qtdBytesLine = RafIOCalc.getInt(tempqtd);
		ind++;	
		
		if (qtdBytesLine==0) return null;
		
		
		tempqtd[0] = tupleByteArray[ind]; 
		ind++;
		tempqtd[1] = tupleByteArray[ind];
		tempqtd[2] = 0;
		tempqtd[3] = 0;
		qtdBytes = RafIOCalc.getInt(tempqtd);
		ind++;
		
		bytesLine = qtdBytes;
		
		indColAux = 0;
		while (qtdBytes != 0) {
			tempStr = null;
			if (qtdBytes < 4) {
				tempStr = new byte[4];
				for (int i=0; i < 4; i++) tempStr[i]=0;
			}
			else {
				tempStr = new byte[qtdBytes];
			}
			for (int i =0; i < qtdBytes; i++){
				tempStr[i] = tupleByteArray[ind];
				ind++;
			}
			if (indColAux == indCol) {
				key = RafIOCalc.getInt(tempStr);
				return key;
			}
				
			
			if (bytesLine + qtdCols == qtdBytesLine){
				bytesLine =0;				
				qtdBytes = 0;
				break;
			}	
			tempqtd[0] = tupleByteArray[ind]; 
			ind++;
			tempqtd[1] = tupleByteArray[ind]; 
			ind++;
			qtdBytes = RafIOCalc.getInt(tempqtd);
			bytesLine = bytesLine  + qtdBytes;
			indColAux++;
		}
		return null;
	}//getKey
	
	/**
     * Returns the value of column 
     * *
     * @param tupleByteArray byte array of tuple
     * @param indCol index of the column
     * @param qtdCols amount of columns on tuple
     * @return String
     */
	public static String getColumn(HandleFile haf, byte[] tupleByteArray, int indCol, int qtdCols){
		int qtdBytes, ind=0, bytesLine, qtdBytesLine, indColAux;
		byte[] tempStr, tempqtd = new byte[4];
		String column, colType="";
		int intContent;
		
		ind = 0;
		tempqtd[0] = tupleByteArray[ind]; 
		ind++;
		tempqtd[1] = tupleByteArray[ind];
		tempqtd[2] = 0;
		tempqtd[3] = 0;
		qtdBytesLine = RafIOCalc.getInt(tempqtd);
		ind++;	
		
		if (qtdBytesLine==0) return null;
		
		
		tempqtd[0] = tupleByteArray[ind]; 
		ind++;
		tempqtd[1] = tupleByteArray[ind];
		tempqtd[2] = 0;
		tempqtd[3] = 0;
		qtdBytes = RafIOCalc.getInt(tempqtd);
		ind++;
		
		bytesLine = qtdBytes;
		
		indColAux = 0;
		while (qtdBytes != 0) {
			tempStr = null;
			if (qtdBytes < 4) {
				tempStr = new byte[4];
				for (int i=0; i < 4; i++) tempStr[i]=0;
			}
			else {
				tempStr = new byte[qtdBytes];
			}
			for (int i =0; i < qtdBytes; i++){
				tempStr[i] = tupleByteArray[ind];
				ind++;
			}
			if (indColAux == indCol) {
				colType = haf.tupleStruct.get(indCol).columnType;
				if (colType.trim().equals("I")) { 
					intContent = RafIOCalc.getInt(tempStr);
					column = String.valueOf(intContent);				
				} else {
					column = RafIOCalc.getString(tempStr);
				}	
				return column.trim();
			}				
			
			if (bytesLine + qtdCols == qtdBytesLine){
				bytesLine =0;				
				qtdBytes = 0;
				break;
			}	
			tempqtd[0] = tupleByteArray[ind]; 
			ind++;
			tempqtd[1] = tupleByteArray[ind]; 
			ind++;			
			qtdBytes = RafIOCalc.getInt(tempqtd);
			bytesLine = bytesLine  + qtdBytes;
			indColAux++;
		}
		return null;
	}//getColumn
	
	
	/**
     * Returns the value of keys concatenated (columns which is join attribute)
     * *
     * @param tupleByteArray byte array of tuple
     * @param indCol[] index of the column
     * @param qtdCols amount of columns on tuple
     * @return Integer
     */
	public static byte[] getKeys(byte[] tupleByteArray, int[] indCol, int qtdCols){
		int qtdBytes, qtdBytesIni=0, ind=0, indini=0, bytesLine, qtdBytesLine, indColAux;
		byte[] tempStr, tempqtd = new byte[4];
		byte[] key= new byte[4*indCol.length];
		int indKey=0;
		
		ind = 0;
		tempqtd[0] = tupleByteArray[ind];
		ind++;
		tempqtd[1] = tupleByteArray[ind];
		tempqtd[2] = 0;
		tempqtd[3] = 0;
		qtdBytesLine = RafIOCalc.getInt(tempqtd);
		ind++;	
		
		if (qtdBytesLine==0) return null;
		
		
		tempqtd[0] = tupleByteArray[ind]; 
		ind++;
		tempqtd[1] = tupleByteArray[ind];
		tempqtd[2] = 0;
		tempqtd[3] = 0;
		qtdBytes = RafIOCalc.getInt(tempqtd);
		ind++;
		
		indini = ind;
		qtdBytesIni = qtdBytes;
		
		for (int j=0; j < indCol.length; j++) {
			
			bytesLine = qtdBytesIni;
			qtdBytes = qtdBytesIni;
			ind = indini;
		
			indColAux = 0;
			while (qtdBytes != 0) {
				tempStr = null;
				if (qtdBytes < 4) {
					tempStr = new byte[4];
					for (int i=0; i < 4; i++) tempStr[i]=0;
				}
				else {
					tempStr = new byte[qtdBytes];
				}
				for (int i =0; i < qtdBytes; i++){
					tempStr[i] = tupleByteArray[ind];
					ind++;
				}

				if (indColAux == indCol[j]) {
					for (int i=0; i < 4; i++) { 
						key[indKey] = tempStr[i];
						indKey++;
					}				
					break;
				}			
			
				if (bytesLine + qtdCols == qtdBytesLine){
					bytesLine =0;				
					qtdBytes = 0;
					break;
				}	
				tempqtd[0] = tupleByteArray[ind]; 
				ind++;
				tempqtd[1] = tupleByteArray[ind];
				ind++;
				qtdBytes = RafIOCalc.getInt(tempqtd);
				bytesLine = bytesLine  + qtdBytes;
				indColAux++;
			}
		}	
		return key;
	}//getKeys
	
	/**
     * Returns the string of rowid
     * *
     * @param rowid byte array of rowid fileds
     * @return String
     */
	public static String getRowid(byte[] rowid){
		byte[] temp = new byte[4];
		String rowidS;
		
		temp[0] = rowid[0];
		temp[1] = rowid[1];
		temp[2] = 0;
		temp[3] = 0;
		rowidS = String.format("%03d",RafIOCalc.getInt(temp));
		temp[0] = rowid[2];
		temp[1] = rowid[3];
		temp[2] = rowid[4];
		temp[3] = rowid[5];
		rowidS = rowidS.trim() + String.format("%08d",RafIOCalc.getInt(temp));
		temp[0] = rowid[6];
		temp[1] = rowid[7];
		temp[2] = rowid[8];
		temp[3] = rowid[9];
		rowidS = rowidS.trim() + String.format("%08d",RafIOCalc.getInt(temp));
		temp[0] = rowid[10];
		temp[1] = rowid[11];
		temp[2] = 0;
		temp[3] = 0;
		rowidS = rowidS.trim() + String.format("%03d",RafIOCalc.getInt(temp));
		temp = null;
		rowid = null;
		return rowidS;
	}//getKey
	
	/**
     * Compare the keys that composes the array of bytes (4 for each key)
     * Returns the 1 if first is greater than second
     *            -1 if first is less than second array
     *             0 if they are equals
     * *
     * @param firstKeys byte array
     * @param secondKeys byte array
     * @return int
     */
	public static int compareCompKeys(byte[] firstKeys, byte[] secondKeys) {
		int qtKeys=0, f4=0;
		byte[] tempf = new byte[4], temps= new byte[4];
		qtKeys = firstKeys.length/4;
		for (int i=0; i < qtKeys; i++) {
			for (int j=0; j<4; j++) {
				tempf[j] = firstKeys[j+f4];
				temps[j] = secondKeys[j+f4];
			}
			if (RafIOCalc.getInt(tempf)>RafIOCalc.getInt(temps))
				return 1;
			if (RafIOCalc.getInt(tempf)<RafIOCalc.getInt(temps))
				return -1;
			f4=f4+4;
		}
		return 0;			
	} //compareComKeys
	
	public static byte[] mapContainsKey(byte[] key, Map<byte[], Slot> hashTable) {
	    for (byte[] keysJoin :  hashTable.keySet()) {
	        if (compareCompKeys(key, keysJoin)==0) {
	            return keysJoin;
	        }
	    }
	    return null;
	}
	
	
	public static boolean isEqual(ArrayList<String> arraylist, String[] array) {
		boolean contains;
	    for (int i=0; i < array.length; i++) {
	        if (!arraylist.contains(array[i])) 
	            return false;	        
	    }
	    for (int i=0; i < arraylist.size(); i++) {
	    	contains = false;
	        for (int j=0; j < array.length; j++) {
	        	if (array[i].equals(arraylist.get(i)))
	        		contains = true;
	        }
	        if (!contains)
	        	return false;
	    }
	    return true;
	}
	
	public static byte[] arraylistContainsKey(byte[] key, ArrayList<byte[]> arraylist) {		
	    for (byte[] keysJoin : arraylist) {	    	
	        if (compareCompKeys(key, keysJoin)==0) {
	            return keysJoin;
	        }
	    }
	    return null;
	}
	
	public synchronized static byte[] getLineByteArrayN2Join(HandleFile haf, String line, int blockSize){
		int ind=0, p=0, pColSep, indCol=0;
		String lineContent;
		int intContent;
		String colType;
		byte[] byteArray = new byte[blockSize], temp1, temp2;
		pColSep = line.indexOf(colSeparator,0);
		byteArray[ind] = 54;
		ind++;
		byteArray[ind] = 0;
		ind++;
		
		while (pColSep != -1) {			
			lineContent = line.substring(p, pColSep);
			//line content in bytes
			colType = haf.tupleStruct.get(indCol).columnType;
			if (colType.trim().equals("I")) { 
				intContent = Integer.parseInt(lineContent);
				temp1 = RafIOCalc.getByteArray(intContent);
			} else  {
				temp1 = RafIOCalc.getByteArray(lineContent); 
			}				
			
			temp2 = RafIOCalc.getByteArray(temp1.length);
			byteArray[ind] = RafIOCalc.getByte(temp2, 0);
			ind++;
			byteArray[ind] = RafIOCalc.getByte(temp2, 1);
			ind++;
			for (int i = 0; i < temp1.length; i++){
				byteArray[ind] = RafIOCalc.getByte(temp1,i);
				ind++;
			}			
			p = pColSep +1;
			pColSep = line.indexOf(colSeparator,pColSep+1);	
			indCol++;
		}
		lastEffectiveBytesBlock = ind;
		byte[] byteReturn = new byte [ind];
		for(int i = 0;i<byteReturn.length;i++){
			byteReturn[i] = byteArray[i];
		}
		return byteReturn;		
	} //getLineByteArray

} 