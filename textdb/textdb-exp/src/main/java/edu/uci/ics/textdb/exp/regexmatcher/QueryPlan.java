package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.List;

import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;

public abstract class QueryPlan {
	private static final int NUM_REQ_TUPLES_FOR_WARM_UP = 10;
	private int processedTupleCounter = 0;
	
	public List<Span> process(Tuple inputTuple){
		processedTupleCounter++;
//		System.out.println("Tuple #" + processedTupleCounter);
		if(processedTupleCounter == NUM_REQ_TUPLES_FOR_WARM_UP + 1){
			// The first time isWarmedUp() returns true.
			finishWarmUpPhase();
		}
		return processTuple(inputTuple);
	}
	
	// Processes the next input tuple. It may/may not collect statistic and modify its
	// data structures before computing the output list of spans.
	protected abstract List<Span> processTuple(Tuple inputTuple);
	
	// This method is called right after the warm up is finished and gives this
	// chance to derived classes to implement their own logic for this event.
	protected abstract void finishWarmUpPhase();
	
	// Returns true if the plan still needs more statistics to be 
	// reliable on its expectedCost() output.
	public abstract boolean needsMoreStatistics();

	// Returns the expected cost of running this plan on an
	// arbitrary input tuple.
	public abstract double getExpectedCost();
	
	// Derived classes can use this method to know how reliable their collected statistics
	// are in the most basic way.
	protected boolean isWarmUpFinished(){
		return processedTupleCounter > NUM_REQ_TUPLES_FOR_WARM_UP;
	}
	
	protected int getProcessedTupleCounter(){
		return processedTupleCounter;
	}
	
	public abstract String report();
	
}
