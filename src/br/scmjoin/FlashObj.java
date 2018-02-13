package br.scmjoin;

public class FlashObj {

	String[] joinColumnsFirstRelation = null;
	String[] joinColumnsSecondRelation = null;
	
	String[] columnsFirstRelation = null;
	String[] columnsSecondRelation = null;
	
	boolean firstJoinTable1 = false;
	boolean firstJoinTable2 = false;
	String[] selectFirstRelation = null;
	String[] selectSecondRelation = null;
	String[] auxHeaderOverFlow = null;
	int memorySizeJoinKernel = 0;
	int memorySizeFetchKernel = 0;
	String headerJoin = null;
	String table1 = null;
	String table2 = null;
	String intermediateTableJoin = null;
	
	String table1_BD = "";
	String table2_BD = "";
	
	int posReRead;
	
	
	public int getMemorySizeFetchKernel() {
		return memorySizeFetchKernel;
	}

	public void setMemorySizeFetchKernel(int memorySizeFetchKernel) {
		this.memorySizeFetchKernel = memorySizeFetchKernel;
	}

	public int getPosReRead() {
		return posReRead;
	}

	public void setPosReRead(int posReRead) {
		this.posReRead = posReRead;
	}

	public String getTable1_BD() {
		return table1_BD;
	}

	public void setTable1_BD(String table1_BD) {
		this.table1_BD = table1_BD;
	}

	public String getTable2_BD() {
		return table2_BD;
	}

	public void setTable2_BD(String table2_BD) {
		this.table2_BD = table2_BD;
	}

	FlashKernel kernel = new FlashKernel();
	
	public String[] getColumnsFirstRelation() {
		return columnsFirstRelation;
	}

	public void setColumnsFirstRelation(String[] columnsFirstRelation) {
		this.columnsFirstRelation = columnsFirstRelation;
	}

	public String[] getColumnsSecondRelation() {
		return columnsSecondRelation;
	}

	public void setColumnsSecondRelation(String[] columnsSecondRelation) {
		this.columnsSecondRelation = columnsSecondRelation;
	}

	public String[] getAuxHeaderOverFlow() {
		return auxHeaderOverFlow;
	}

	public void setAuxHeaderOverFlow(String[] auxHeaderOverFlow) {
		this.auxHeaderOverFlow = auxHeaderOverFlow;
	}

	public FlashKernel getKernel() {
		return kernel;
	}

	public void setKernel(FlashKernel kernel) {
		this.kernel = kernel;
	}


	public String getIntermediateTableJoin() {
		return intermediateTableJoin;
	}

	public void setIntermediateTableJoin(String intermediateTableJoin) {
		this.intermediateTableJoin = intermediateTableJoin;
	}

	public FlashObj() {

	}

	public String getTable1() {
		return table1;
	}

	public void setTable1(String table1) {
		this.table1 = table1;
	}

	public String getTable2() {
		return table2;
	}

	public void setTable2(String table2) {
		this.table2 = table2;
	}

	public String[] getJoinColumnsFirstRelation() {
		return joinColumnsFirstRelation;
	}

	public void setJoinColumnsFirstRelation(String[] joinColumnsFirstRelation) {
		this.joinColumnsFirstRelation = joinColumnsFirstRelation;
	}

	public String[] getJoinColumnsSecondRelation() {
		return joinColumnsSecondRelation;
	}

	public void setJoinColumnsSecondRelation(String[] joinColumnsSecondRelation) {
		this.joinColumnsSecondRelation = joinColumnsSecondRelation;
	}

	public boolean isFirstJoinTable1() {
		return firstJoinTable1;
	}

	public void setFirstJoinTable1(boolean firstJoinTable1) {
		this.firstJoinTable1 = firstJoinTable1;
	}

	public boolean isFirstJoinTable2() {
		return firstJoinTable2;
	}

	public void setFirstJoinTable2(boolean firstJoinTable2) {
		this.firstJoinTable2 = firstJoinTable2;
	}


	public String[] getSelectFirstRelation() {
		return selectFirstRelation;
	}

	public void setSelectFirstRelation(String[] selectFirstRelation) {
		this.selectFirstRelation = selectFirstRelation;
	}

	public String[] getSelectSecondRelation() {
		return selectSecondRelation;
	}

	public void setSelectSecondRelation(String[] selectSecondRelation) {
		this.selectSecondRelation = selectSecondRelation;
	}

	public long getMemorySizeJoinKernel() {
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

}

class FlashKernel {
	String[] tablesReRead = null;
	String[] ColReRead = null;
	String tableFromJoinKernel = null;
	String tableToNextJoin = null;
	boolean lastJoin = false;
	String headerToNextJoin = null;
	String[] atbReRead = null;

	public String[] getAtbReRead() {
		return atbReRead;
	}

	public void setAtbReRead(String[] atbReRead) {
		this.atbReRead = atbReRead;
	}

	public String getHeaderToNextJoin() {
		return headerToNextJoin;
	}

	public void setHeaderToNextJoin(String headerToNextJoin) {
		this.headerToNextJoin = headerToNextJoin;
	}

	public String getTableToNextJoin() {
		return tableToNextJoin;
	}

	public void setTableToNextJoin(String tableToNextJoin) {
		this.tableToNextJoin = tableToNextJoin;
	}

	public String[] getTablesReRead() {
		return tablesReRead;
	}

	public void setTablesReRead(String[] tablesReRead) {
		this.tablesReRead = tablesReRead;
	}

	public String[] getColReRead() {
		return ColReRead;
	}

	public void setColReRead(String[] colReRead) {
		ColReRead = colReRead;
	}

	public String getTableFromJoinKernel() {
		return tableFromJoinKernel;
	}

	public void setTableFromJoinKernel(String tableFromJoinKernel) {
		this.tableFromJoinKernel = tableFromJoinKernel;
	}

	public boolean isLastJoin() {
		return lastJoin;
	}

	public void setLastJoin(boolean lastJoin) {
		this.lastJoin = lastJoin;
	}

}
