package br.scmjoin;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.text.SimpleDateFormat;

public class Measure {
	private static Runtime runtime = Runtime.getRuntime();
	private static long startTime, stopTime;
	private static double elapsedTime,rrelapsedTime=0;
	private static long allocatedMemory, freeMemory, usedMemory;
	private static long tuplesFirstRelation, tuplesSecondRelation, cartesian;
	private static BigDecimal selectivity;
	
	public static void setStart() {
		startTime = System.currentTimeMillis();
		tuplesFirstRelation = 0;
		tuplesSecondRelation = 0;
	}
	
	public static void setQtTuples(long qtTuples) {
		if (tuplesFirstRelation==0) {
			tuplesFirstRelation=qtTuples;
		} else {
			tuplesSecondRelation=qtTuples;
			cartesian = tuplesFirstRelation * tuplesSecondRelation;
		}		
	}
	
	public static long getTuplesFirstRelation(){
		return tuplesFirstRelation;		
	}
	
	public static long getTuplesSecondRelation(){
		return tuplesSecondRelation;		
	}

	public static long getCartesian(){
		return cartesian;		
	}

	public static void setSelectivity(long qtTuplesResult) {
		BigDecimal c = new BigDecimal(cartesian);	
		selectivity = new BigDecimal(qtTuplesResult);
		selectivity = selectivity.divide(c, 10, RoundingMode.HALF_UP);
	}
	
	public static String getSelectivity() {
		return String.format("%1$,.10f",selectivity);
	}
	
	public static long getMemory(){
 	    allocatedMemory = runtime.totalMemory();
 	    freeMemory = runtime.freeMemory();
 	    usedMemory = allocatedMemory - freeMemory;
 	    return usedMemory;
	}
	
	public static String getElapsedTime() {
		String tTime;
        
		stopTime = System.currentTimeMillis();
        elapsedTime = stopTime - startTime;             
        elapsedTime = elapsedTime/1000;
        tTime = String.format("%1$,.10f",elapsedTime);              
        return tTime;
	}
	
	public static String getTime() {
		SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ssss.SSSZ");

		stopTime = System.currentTimeMillis();
        Date resultdate = new Date(stopTime);
        return sdf.format(resultdate);
	}
	
	public static double getElapsedTimeDouble() {
		stopTime = System.currentTimeMillis();
        elapsedTime = stopTime - startTime;             
        elapsedTime = elapsedTime/1000;
        
        return elapsedTime;
	}

}
