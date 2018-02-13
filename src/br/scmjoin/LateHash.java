package br.scmjoin;

public class LateHash {
	String[] joinColumns = null;
	String[] earlyColumnsTb1 = null;
	String[] earlyColumnsTb2 = null;
	String[] atbNextJoin = null;
	boolean lastJoin = false;
	long memorySizeJoinKernel;
	String headerJoin = null;
	String[] tableforLastRelation = null;
	boolean firstJoinTb1 = false;
	boolean firstJoinTb2 = false;
	HandleFile haf2 = new HandleFile(8192);
	HandleFile haf1 = new HandleFile(8192);
	boolean twoway = false;
	String[] tb1 = null;
	String[] tb2 = null;
	String[] reReadDiskTb1 = null;
	String[] reReadDiskTb2 = null;
	int auxMemoryKey = 0;
	int auxMemoryRowid = 0;
	
	String tableToFetch = "";
	int colToFetch;
	
	

	String joinTb11 = "";
	String joinTb12 = "";
	String joinTb13 = "";
	String joinTb14 = "";
	String joinTb15 = "";
	String joinTb16 = "";
	String joinTb17 = "";
	String joinTb18 = "";
	
	String joinTb21 = "";
	String joinTb22 = "";
	String joinTb23 = "";
	String joinTb24 = "";
	String joinTb25 = "";
	String joinTb26 = "";
	String joinTb27 = "";
	String joinTb28 = "";

	HandleFile nextHaf11 = new HandleFile(8192);
	HandleFile nextHaf12 = new HandleFile(8192);
	HandleFile nextHaf13 = new HandleFile(8192);
	HandleFile nextHaf14 = new HandleFile(8192);
	HandleFile nextHaf15 = new HandleFile(8192);
	HandleFile nextHaf16 = new HandleFile(8192);
	HandleFile nextHaf17 = new HandleFile(8192);
	HandleFile nextHaf18 = new HandleFile(8192);
	HandleFile nextHaf19 = new HandleFile(8192);
	HandleFile nextHaf110 = new HandleFile(8192);
	HandleFile nextHaf111 = new HandleFile(8192);
	HandleFile nextHaf112 = new HandleFile(8192);
	HandleFile nextHaf113 = new HandleFile(8192);
	HandleFile nextHaf114 = new HandleFile(8192);
	HandleFile nextHaf115 = new HandleFile(8192);
	HandleFile nextHaf116 = new HandleFile(8192);

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
	
	public int getColToFetch() {
		return colToFetch;
	}

	public void setColToFetch(int colToFetch) {
		this.colToFetch = colToFetch;
	}

	public String getTableToFetch() {
		return tableToFetch;
	}

	public void setTableToFetch(String tableToFetch) {
		this.tableToFetch = tableToFetch;
	}

	public String getJoinTb15() {
		return joinTb15;
	}

	public void setJoinTb15(String joinTb15) {
		this.joinTb15 = joinTb15;
	}

	public String getJoinTb16() {
		return joinTb16;
	}

	public void setJoinTb16(String joinTb16) {
		this.joinTb16 = joinTb16;
	}

	public String getJoinTb17() {
		return joinTb17;
	}

	public void setJoinTb17(String joinTb17) {
		this.joinTb17 = joinTb17;
	}

	public String getJoinTb18() {
		return joinTb18;
	}

	public void setJoinTb18(String joinTb18) {
		this.joinTb18 = joinTb18;
	}

	public String getJoinTb25() {
		return joinTb25;
	}

	public void setJoinTb25(String joinTb25) {
		this.joinTb25 = joinTb25;
	}

	public String getJoinTb26() {
		return joinTb26;
	}

	public void setJoinTb26(String joinTb26) {
		this.joinTb26 = joinTb26;
	}

	public String getJoinTb27() {
		return joinTb27;
	}

	public void setJoinTb27(String joinTb27) {
		this.joinTb27 = joinTb27;
	}

	public String getJoinTb28() {
		return joinTb28;
	}

	public void setJoinTb28(String joinTb28) {
		this.joinTb28 = joinTb28;
	}

	public String getJoinTb13() {
		return joinTb13;
	}

	public void setJoinTb13(String joinTb13) {
		this.joinTb13 = joinTb13;
	}

	public String getJoinTb14() {
		return joinTb14;
	}

	public void setJoinTb14(String joinTb14) {
		this.joinTb14 = joinTb14;
	}

	public String getJoinTb23() {
		return joinTb23;
	}

	public void setJoinTb23(String joinTb23) {
		this.joinTb23 = joinTb23;
	}

	public String getJoinTb24() {
		return joinTb24;
	}

	public void setJoinTb24(String joinTb24) {
		this.joinTb24 = joinTb24;
	}

	public String getJoinTb11() {
		return joinTb11;
	}

	public void setJoinTb11(String joinTb11) {
		this.joinTb11 = joinTb11;
	}

	public String getJoinTb12() {
		return joinTb12;
	}

	public void setJoinTb12(String joinTb12) {
		this.joinTb12 = joinTb12;
	}

	public String getJoinTb21() {
		return joinTb21;
	}

	public void setJoinTb21(String joinTb21) {
		this.joinTb21 = joinTb21;
	}

	public String getJoinTb22() {
		return joinTb22;
	}

	public void setJoinTb22(String joinTb22) {
		this.joinTb22 = joinTb22;
	}

	public HandleFile getNextHaf19() {
		return nextHaf19;
	}

	public void setNextHaf19(HandleFile nextHaf19) {
		this.nextHaf19 = nextHaf19;
	}

	public HandleFile getNextHaf110() {
		return nextHaf110;
	}

	public void setNextHaf110(HandleFile nextHaf110) {
		this.nextHaf110 = nextHaf110;
	}

	public HandleFile getNextHaf111() {
		return nextHaf111;
	}

	public void setNextHaf111(HandleFile nextHaf111) {
		this.nextHaf111 = nextHaf111;
	}

	public HandleFile getNextHaf112() {
		return nextHaf112;
	}

	public void setNextHaf112(HandleFile nextHaf112) {
		this.nextHaf112 = nextHaf112;
	}

	public HandleFile getNextHaf113() {
		return nextHaf113;
	}

	public void setNextHaf113(HandleFile nextHaf113) {
		this.nextHaf113 = nextHaf113;
	}

	public HandleFile getNextHaf114() {
		return nextHaf114;
	}

	public void setNextHaf114(HandleFile nextHaf114) {
		this.nextHaf114 = nextHaf114;
	}

	public HandleFile getNextHaf115() {
		return nextHaf115;
	}

	public void setNextHaf115(HandleFile nextHaf115) {
		this.nextHaf115 = nextHaf115;
	}

	public HandleFile getNextHaf116() {
		return nextHaf116;
	}

	public void setNextHaf116(HandleFile nextHaf116) {
		this.nextHaf116 = nextHaf116;
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

	public String[] getReReadDiskTb1() {
		return reReadDiskTb1;
	}

	public void setReReadDiskTb1(String[] reReadDiskTb1) {
		this.reReadDiskTb1 = reReadDiskTb1;
	}

	public String[] getReReadDiskTb2() {
		return reReadDiskTb2;
	}

	public void setReReadDiskTb2(String[] reReadDiskTb2) {
		this.reReadDiskTb2 = reReadDiskTb2;
	}

	public int getAuxMemoryKey() {
		return auxMemoryKey;
	}

	public void setAuxMemoryKey(int auxMemoryKey) {
		this.auxMemoryKey = auxMemoryKey;
	}

	public int getAuxMemoryRowid() {
		return auxMemoryRowid;
	}

	public void setAuxMemoryRowid(int auxMemoryRowid) {
		this.auxMemoryRowid = auxMemoryRowid;
	}

	public String[] getAtbNextJoin() {
		return atbNextJoin;
	}

	public void setAtbNextJoin(String[] atbNextJoin) {
		this.atbNextJoin = atbNextJoin;
	}

	public HandleFile getNextHaf11() {
		return nextHaf11;
	}

	public void setNextHaf11(HandleFile nextHaf11) {
		this.nextHaf11 = nextHaf11;
	}

	public HandleFile getNextHaf12() {
		return nextHaf12;
	}

	public void setNextHaf12(HandleFile nextHaf12) {
		this.nextHaf12 = nextHaf12;
	}

	public HandleFile getNextHaf13() {
		return nextHaf13;
	}

	public void setNextHaf13(HandleFile nextHaf13) {
		this.nextHaf13 = nextHaf13;
	}

	public HandleFile getNextHaf14() {
		return nextHaf14;
	}

	public void setNextHaf14(HandleFile nextHaf14) {
		this.nextHaf14 = nextHaf14;
	}

	public HandleFile getNextHaf15() {
		return nextHaf15;
	}

	public void setNextHaf15(HandleFile nextHaf15) {
		this.nextHaf15 = nextHaf15;
	}

	public HandleFile getNextHaf16() {
		return nextHaf16;
	}

	public void setNextHaf16(HandleFile nextHaf16) {
		this.nextHaf16 = nextHaf16;
	}

	public HandleFile getNextHaf17() {
		return nextHaf17;
	}

	public void setNextHaf17(HandleFile nextHaf17) {
		this.nextHaf17 = nextHaf17;
	}

	public HandleFile getNextHaf18() {
		return nextHaf18;
	}

	public void setNextHaf18(HandleFile nextHaf18) {
		this.nextHaf18 = nextHaf18;
	}

	public String[] getTb1() {
		return tb1;
	}

	public void setTb1(String[] tb1) {
		this.tb1 = tb1;
	}

	public String[] getTb2() {
		return tb2;
	}

	public void setTb2(String[] tb2) {
		this.tb2 = tb2;
	}

	public boolean isTwoway() {
		return twoway;
	}

	public void setTwoway(boolean twoway) {
		this.twoway = twoway;
	}

	public HandleFile getHaf2() {
		return haf2;
	}

	public void setHaf2(HandleFile haf2) {
		this.haf2 = haf2;
	}

	public HandleFile getHaf1() {
		return haf1;
	}

	public void setHaf1(HandleFile haf1) {
		this.haf1 = haf1;
	}

	public LateHash() {

	}

	public boolean isFirstJoinTb1() {
		return firstJoinTb1;
	}

	public void setFirstJoinTb1(boolean firstJoinTb1) {
		this.firstJoinTb1 = firstJoinTb1;
	}

	public boolean isFirstJoinTb2() {
		return firstJoinTb2;
	}

	public void setFirstJoinTb2(boolean firstJoinTb2) {
		this.firstJoinTb2 = firstJoinTb2;
	}

	public String[] getTableforLastRelation() {
		return tableforLastRelation;
	}

	public void setTableforLastRelation(String[] tableforLastRelation) {
		this.tableforLastRelation = tableforLastRelation;
	}

	public String[] getEarlyColumnsTb1() {
		return earlyColumnsTb1;
	}

	public void setEarlyColumnsTb1(String[] earlyColumnsTb1) {
		this.earlyColumnsTb1 = earlyColumnsTb1;
	}

	public String[] getEarlyColumnsTb2() {
		return earlyColumnsTb2;
	}

	public void setEarlyColumnsTb2(String[] earlyColumnsTb2) {
		this.earlyColumnsTb2 = earlyColumnsTb2;
	}

	public String[] getJoinColumns() {
		return joinColumns;
	}

	public void setJoinColumns(String[] joinColumns) {
		this.joinColumns = joinColumns;
	}

	public boolean isLastJoin() {
		return lastJoin;
	}

	public void setLastJoin(boolean lastJoin) {
		this.lastJoin = lastJoin;
	}

	public long getMemorySizeJoinKernel() {
		return memorySizeJoinKernel;
	}

	public void setMemorySizeJoinKernel(long memorySizeJoinKernel) {
		this.memorySizeJoinKernel = memorySizeJoinKernel;
	}

	public String getHeaderJoin() {
		return headerJoin;
	}

	public void setHeaderJoin(String headerJoin) {
		this.headerJoin = headerJoin;
	}

}
