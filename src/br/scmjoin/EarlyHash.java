package br.scmjoin;

public class EarlyHash {

	String[] joinColumns = null;
	String[] earlyColumns = null;
	boolean firstJoinTable = false;
	boolean firstLastTable = false;
	String[] selectRelation = null;
	int memorySizeJoinKernel ;
	String headerJoin = null;
	String table = null;
	int numberPhase = 0;
	int numberTable = 0;
	int auxMemoryPreviusTuple = 0;
	int auxMemoryPreviusKey = 0;
	int auxMemoryCurrentTuple = 0;
	int auxMemoryCurrentKey = 0;

	HandleFile haf11 = new HandleFile(8192);
	HandleFile haf12 = new HandleFile(8192);
	HandleFile haf13 = new HandleFile(8192);
	HandleFile haf14 = new HandleFile(8192);
	HandleFile haf15 = new HandleFile(8192);
	HandleFile haf16 = new HandleFile(8192);
	HandleFile haf17 = new HandleFile(8192);
	HandleFile haf18 = new HandleFile(8192);
	HandleFile haf19 = new HandleFile(8192);
	HandleFile haf110 = new HandleFile(8192);
	HandleFile haf111 = new HandleFile(8192);
	HandleFile haf112 = new HandleFile(8192);
	HandleFile haf113 = new HandleFile(8192);
	HandleFile haf114 = new HandleFile(8192);
	HandleFile haf115 = new HandleFile(8192);
	HandleFile haf116 = new HandleFile(8192);

	HandleFile haf21 = new HandleFile(8192);
	HandleFile haf22 = new HandleFile(8192);
	HandleFile haf23 = new HandleFile(8192);
	HandleFile haf24 = new HandleFile(8192);
	HandleFile haf25 = new HandleFile(8192);
	HandleFile haf26 = new HandleFile(8192);
	HandleFile haf27 = new HandleFile(8192);
	HandleFile haf28 = new HandleFile(8192);
	HandleFile haf29 = new HandleFile(8192);
	HandleFile haf210 = new HandleFile(8192);
	HandleFile haf211 = new HandleFile(8192);
	HandleFile haf212 = new HandleFile(8192);
	HandleFile haf213 = new HandleFile(8192);
	HandleFile haf214 = new HandleFile(8192);
	HandleFile haf215 = new HandleFile(8192);
	HandleFile haf216 = new HandleFile(8192);

	public EarlyHash() {

	}
	
	

	public HandleFile getHaf19() {
		return haf19;
	}



	public void setHaf19(HandleFile haf19) {
		this.haf19 = haf19;
	}



	public HandleFile getHaf110() {
		return haf110;
	}



	public void setHaf110(HandleFile haf110) {
		this.haf110 = haf110;
	}



	public HandleFile getHaf111() {
		return haf111;
	}



	public void setHaf111(HandleFile haf111) {
		this.haf111 = haf111;
	}



	public HandleFile getHaf112() {
		return haf112;
	}



	public void setHaf112(HandleFile haf112) {
		this.haf112 = haf112;
	}



	public HandleFile getHaf113() {
		return haf113;
	}



	public void setHaf113(HandleFile haf113) {
		this.haf113 = haf113;
	}



	public HandleFile getHaf114() {
		return haf114;
	}



	public void setHaf114(HandleFile haf114) {
		this.haf114 = haf114;
	}



	public HandleFile getHaf115() {
		return haf115;
	}



	public void setHaf115(HandleFile haf115) {
		this.haf115 = haf115;
	}



	public HandleFile getHaf116() {
		return haf116;
	}



	public void setHaf116(HandleFile haf116) {
		this.haf116 = haf116;
	}



	public HandleFile getHaf29() {
		return haf29;
	}



	public void setHaf29(HandleFile haf29) {
		this.haf29 = haf29;
	}



	public HandleFile getHaf210() {
		return haf210;
	}



	public void setHaf210(HandleFile haf210) {
		this.haf210 = haf210;
	}



	public HandleFile getHaf211() {
		return haf211;
	}



	public void setHaf211(HandleFile haf211) {
		this.haf211 = haf211;
	}



	public HandleFile getHaf212() {
		return haf212;
	}



	public void setHaf212(HandleFile haf212) {
		this.haf212 = haf212;
	}



	public HandleFile getHaf213() {
		return haf213;
	}



	public void setHaf213(HandleFile haf213) {
		this.haf213 = haf213;
	}



	public HandleFile getHaf214() {
		return haf214;
	}



	public void setHaf214(HandleFile haf214) {
		this.haf214 = haf214;
	}



	public HandleFile getHaf215() {
		return haf215;
	}



	public void setHaf215(HandleFile haf215) {
		this.haf215 = haf215;
	}



	public HandleFile getHaf216() {
		return haf216;
	}



	public void setHaf216(HandleFile haf216) {
		this.haf216 = haf216;
	}



	public HandleFile getHaf11() {
		return haf11;
	}

	public void setHaf11(HandleFile haf11) {
		this.haf11 = haf11;
	}

	public HandleFile getHaf12() {
		return haf12;
	}

	public void setHaf12(HandleFile haf12) {
		this.haf12 = haf12;
	}

	public HandleFile getHaf13() {
		return haf13;
	}

	public void setHaf13(HandleFile haf13) {
		this.haf13 = haf13;
	}

	public HandleFile getHaf14() {
		return haf14;
	}

	public void setHaf14(HandleFile haf14) {
		this.haf14 = haf14;
	}

	public HandleFile getHaf15() {
		return haf15;
	}

	public void setHaf15(HandleFile haf15) {
		this.haf15 = haf15;
	}

	public HandleFile getHaf16() {
		return haf16;
	}

	public void setHaf16(HandleFile haf16) {
		this.haf16 = haf16;
	}

	public HandleFile getHaf17() {
		return haf17;
	}

	public void setHaf17(HandleFile haf17) {
		this.haf17 = haf17;
	}

	public HandleFile getHaf18() {
		return haf18;
	}

	public void setHaf18(HandleFile haf18) {
		this.haf18 = haf18;
	}

	public HandleFile getHaf21() {
		return haf21;
	}

	public void setHaf21(HandleFile haf21) {
		this.haf21 = haf21;
	}

	public HandleFile getHaf22() {
		return haf22;
	}

	public void setHaf22(HandleFile haf22) {
		this.haf22 = haf22;
	}

	public HandleFile getHaf23() {
		return haf23;
	}

	public void setHaf23(HandleFile haf23) {
		this.haf23 = haf23;
	}

	public HandleFile getHaf24() {
		return haf24;
	}

	public void setHaf24(HandleFile haf24) {
		this.haf24 = haf24;
	}

	public HandleFile getHaf25() {
		return haf25;
	}

	public void setHaf25(HandleFile haf25) {
		this.haf25 = haf25;
	}

	public HandleFile getHaf26() {
		return haf26;
	}

	public void setHaf26(HandleFile haf26) {
		this.haf26 = haf26;
	}

	public HandleFile getHaf27() {
		return haf27;
	}

	public void setHaf27(HandleFile haf27) {
		this.haf27 = haf27;
	}

	public HandleFile getHaf28() {
		return haf28;
	}

	public void setHaf28(HandleFile haf28) {
		this.haf28 = haf28;
	}

	public String[] getJoinColumns() {
		return joinColumns;
	}

	public void setJoinColumns(String[] joinColumns) {
		this.joinColumns = joinColumns;
	}

	public String[] getEarlyColumns() {
		return earlyColumns;
	}

	public void setEarlyColumns(String[] earlyColumns) {
		this.earlyColumns = earlyColumns;
	}

	public boolean isFirstJoinTable() {
		return firstJoinTable;
	}

	public void setFirstJoinTable(boolean firstJoinTable) {
		this.firstJoinTable = firstJoinTable;
	}

	public boolean isFirstLastTable() {
		return firstLastTable;
	}

	public void setFirstLastTable(boolean firstLastTable) {
		this.firstLastTable = firstLastTable;
	}

	public String[] getSelectRelation() {
		return selectRelation;
	}

	public void setSelectRelation(String[] selectRelation) {
		this.selectRelation = selectRelation;
	}

	public int getMemorySizeJoinKernel() {
		return memorySizeJoinKernel;
	}

	public void setMemorySizeJoinKernel(int memorySizeJoinKernel) {
		this.memorySizeJoinKernel = memorySizeJoinKernel;
	}

	public String getHeaderJoin() {
		return headerJoin;
	}

	public void setHeaderJoin(String headerJoin) {
		this.headerJoin = headerJoin;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public int getNumberPhase() {
		return numberPhase;
	}

	public void setNumberPhase(int numberPhase) {
		this.numberPhase = numberPhase;
	}

	public int getNumberTable() {
		return numberTable;
	}

	public void setNumberTable(int numberTable) {
		this.numberTable = numberTable;
	}

	public int getAuxMemoryPreviusTuple() {
		return auxMemoryPreviusTuple;
	}

	public void setAuxMemoryPreviusTuple(int auxMemoryPreviusTuple) {
		this.auxMemoryPreviusTuple = auxMemoryPreviusTuple;
	}

	public int getAuxMemoryPreviusKey() {
		return auxMemoryPreviusKey;
	}

	public void setAuxMemoryPreviusKey(int auxMemoryPreviusKey) {
		this.auxMemoryPreviusKey = auxMemoryPreviusKey;
	}

	public int getAuxMemoryCurrentTuple() {
		return auxMemoryCurrentTuple;
	}

	public void setAuxMemoryCurrentTuple(int auxMemoryCurrentTuple) {
		this.auxMemoryCurrentTuple = auxMemoryCurrentTuple;
	}

	public int getAuxMemoryCurrentKey() {
		return auxMemoryCurrentKey;
	}

	public void setAuxMemoryCurrentKey(int auxMemoryCurrentKey) {
		this.auxMemoryCurrentKey = auxMemoryCurrentKey;
	}

}