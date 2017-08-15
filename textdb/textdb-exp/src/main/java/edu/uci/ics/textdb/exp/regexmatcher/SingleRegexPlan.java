package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;

public class SingleRegexPlan extends QueryPlan{
	private SubRegex regex = null;
	public SingleRegexPlan(SubRegex regex){
		this.regex = regex;
	}
	@Override
	protected List<Span> processTuple(Tuple inputTuple) {
		if(! needsMoreStatistics()){
			long startMatchingTime = System.nanoTime();
			List<Span> subRegexSpans = 
					RegexMatcherUtils.computeMatchingResultsWithCompletePattern(inputTuple, 
							regex.regex, regex.regexPatern, regex.isReverseExecutionFaster(), 
							regex.getReverseSubRegex().regex, regex.getReverseSubRegex().regexPatern);
			long endMatchingTime = System.nanoTime();
			long matchingTime = endMatchingTime - startMatchingTime;
			int totalTupleTextSize = 1;
			for(String attributeName : regex.regex.getAttributeNames()){
				totalTupleTextSize += inputTuple.getField(attributeName).getValue().toString().length();
			}
			double matchCost = (matchingTime * 1.0) / Math.max(totalTupleTextSize, 1);
			// Save the span list for this subregex to be used in the second phase of the algorithm.
			regex.resetMatchingSpanList(subRegexSpans);
			// Update the statistics
			if(subRegexSpans == null || subRegexSpans.isEmpty()){
				// no matches
				// update statistics with the cost of failure
				if(regex.isReverseExecutionFaster()){
					regex.stats.addStatsSubRegexFailure(0, totalTupleTextSize);
					regex.getReverseSubRegex().stats.addStatsSubRegexFailure(matchCost, totalTupleTextSize);
				}else{
					regex.stats.addStatsSubRegexFailure(matchCost, totalTupleTextSize);
				}
			}else{
				// some matches exist
				// update statistics upon success (df, tf, and success cost)
				List<Integer> matchSizesPerAttributes = new ArrayList();
				regex.regex.getAttributeNames().stream().
					forEach(attr -> matchSizesPerAttributes.add(
							subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
													collect(Collectors.toList()).size()
					));
				if(regex.isReverseExecutionFaster()){
					regex.stats.addStatsSubRegexSuccess(0, totalTupleTextSize);
					regex.getReverseSubRegex().stats.addStatsSubRegexSuccess(matchCost, totalTupleTextSize);
					regex.stats.addTfAveragePerAttribute(matchSizesPerAttributes);
					regex.getReverseSubRegex().stats.addTfAveragePerAttribute(matchSizesPerAttributes);
				}else{
					regex.stats.addStatsSubRegexSuccess(matchCost, totalTupleTextSize);
				}
			}
			return subRegexSpans;
		}
		
		// Still warm up
		{
			long startMatchingTime = System.nanoTime();
			List<Span> subRegexSpans = 
					RegexMatcherUtils.computeMatchingResultsWithCompletePattern(inputTuple, 
							regex.regex, regex.regexPatern, false, 
							regex.getReverseSubRegex().regex, regex.getReverseSubRegex().regexPatern);
			long endMatchingTime = System.nanoTime();
			long matchingTime = endMatchingTime - startMatchingTime;
			int totalTupleTextSize = 1;
			for(String attributeName : regex.regex.getAttributeNames()){
				totalTupleTextSize += inputTuple.getField(attributeName).getValue().toString().length();
			}
			double matchCost = (matchingTime * 1.0) / Math.max(totalTupleTextSize, 1);
			// Save the span list for this subregex to be used in the second phase of the algorithm.
			regex.resetMatchingSpanList(subRegexSpans);
			// Update the statistics
			if(subRegexSpans == null || subRegexSpans.isEmpty()){
				// no matches
				// update statistics with the cost of failure
				regex.stats.addStatsSubRegexFailure(matchCost, totalTupleTextSize);
			}else{
				// some matches exist
				// update statistics upon success (df, tf, and success cost)
				List<Integer> matchSizesPerAttributes = new ArrayList<>();
				regex.regex.getAttributeNames().stream().
					forEach(attr -> matchSizesPerAttributes.add(
							subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
													collect(Collectors.toList()).size()
					));
				regex.stats.addStatsSubRegexSuccess(matchCost, totalTupleTextSize);
				regex.stats.addTfAveragePerAttribute(matchSizesPerAttributes);
			}
		}
		// Reverse
		{
			long startMatchingTime = System.nanoTime();
			List<Span> subRegexSpans = 
					RegexMatcherUtils.computeMatchingResultsWithCompletePattern(inputTuple, 
							regex.regex, regex.regexPatern, true, 
							regex.getReverseSubRegex().regex, regex.getReverseSubRegex().regexPatern);
			long endMatchingTime = System.nanoTime();
			long matchingTime = endMatchingTime - startMatchingTime;
			int totalTupleTextSize = 1;
			for(String attributeName : regex.regex.getAttributeNames()){
				totalTupleTextSize += inputTuple.getField(attributeName).getValue().toString().length();
			}
			double matchCost = (matchingTime * 1.0) / Math.max(totalTupleTextSize, 1);
			// Save the span list for this subregex to be used in the second phase of the algorithm.
			regex.resetMatchingSpanList(subRegexSpans);
			// Update the statistics
			if(subRegexSpans == null || subRegexSpans.isEmpty()){
				// no matches
				// update statistics with the cost of failure
				regex.stats.addStatsSubRegexFailure(0, totalTupleTextSize);
				regex.getReverseSubRegex().stats.addStatsSubRegexFailure(matchCost, totalTupleTextSize);
			}else{
				// some matches exist
				// update statistics upon success (df, tf, and success cost)
				List<Integer> matchSizesPerAttributes = new ArrayList<>();
				regex.regex.getAttributeNames().stream().
					forEach(attr -> matchSizesPerAttributes.add(
							subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
													collect(Collectors.toList()).size()
					));
				regex.stats.addStatsSubRegexSuccess(0, totalTupleTextSize);
				regex.getReverseSubRegex().stats.addStatsSubRegexSuccess(matchCost, totalTupleTextSize);
				regex.getReverseSubRegex().stats.addTfAveragePerAttribute(matchSizesPerAttributes);
			}
			return subRegexSpans;
		}
		
	}

	@Override
	protected void finishWarmUpPhase() {
	}

	@Override
	public boolean needsMoreStatistics() {
		return ! isWarmUpFinished();
	}

	@Override
	public double getExpectedCost() {
		return regex.getExpectedCost() * regex.stats.matchingSrcAvgLen;
	}

}
