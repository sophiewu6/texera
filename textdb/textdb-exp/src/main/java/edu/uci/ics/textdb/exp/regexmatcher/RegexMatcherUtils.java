package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import jregex.Matcher;
import jregex.Pattern;

public class RegexMatcherUtils {
    public static SubPlan findSubRegexNearestComputedLeftNeighbor(Set<SubPlan> subPlans, 
    		SubPlan currentSubPlan){
    	// find the right most computed sub-regex to the left of current sub-regex
    	SubPlan leftBound = null;
    	for(SubPlan computedSubPlan : subPlans){
    		if(computedSubPlan.getSubSeq().getEnd() <= currentSubPlan.getSubSeq().getStart()){
    			if(leftBound == null || leftBound.getSubSeq().getEnd() < computedSubPlan.getSubSeq().getEnd()){
    				leftBound = computedSubPlan;
    			}
    		}
    	}
    	return leftBound;
    }
    
    public static SubPlan findSubRegexNearestComputedRightNeighbor(Set<SubPlan> subPlans, SubPlan currentSubPlan){
    	// find the right most computed sub-regex to the left of current sub-regex
    	SubPlan rightBound = null;
    	for(SubPlan computedSubRegex : subPlans){
    		if(computedSubRegex.getSubSeq().getStart() >= currentSubPlan.getSubSeq().getEnd()){
    			if(rightBound == null || rightBound.getSubSeq().getStart() > computedSubRegex.getSubSeq().getStart()){
    				rightBound = computedSubRegex;
    			}
    		}
    	}
    	return rightBound;
    }
    
    public static List<Span> computeSubRegexMatchesWithComputedNeighbors(Tuple inputTuple, 
    		SubRegex subRegex, 
    		SubPlan leftBound, SubPlan rightBound, 
    		boolean forceExecutionDirection, boolean forceReverse){
    	
    	List<Span> matchingResults = new ArrayList<>();
    	
		boolean runReverse = (forceExecutionDirection && forceReverse) || (
				! forceExecutionDirection && subRegex.isReverseExecutionFaster());
    	
    	if(leftBound == null && rightBound == null){ // No computed subRegex is around the subRegex
    		return computeAllMatchingResults(inputTuple, subRegex, false, false);
    	}else if(leftBound == null){ // && rightBound != null // one computed subRegex on right
    		List<Span> rightBoundSpans = rightBound.getSubSeq().getLatestMatchingSpanList();
	        for (String attributeName : subRegex.regex.getAttributeNames()) {
	            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
	            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
	            // types other than TEXT and STRING: throw Exception for now
	            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
	                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
	            }
	            // Filter the bound spans for the current attributes
	            List<Span> rightSpans = rightBoundSpans.
	            		stream().filter(s -> s.getAttributeName().equals(attributeName)).
	            		collect(Collectors.toList());
	            if(subRegex.getEnd() == rightBound.getSubSeq().getStart()){ // Direct right neighbor is computed.
    	            String reverseFieldValue = new StringBuilder(fieldValue).reverse().toString();
    	            for(Span rightSpan: rightSpans){
    	            	List<Span> reverseMatches = 
    	            			computeMatchingResultsStartingAt(attributeName, reverseFieldValue, 
    	            			fieldValue.length() - rightSpan.getStart(), subRegex.getReverseSubRegex(), subRegex.getStart() != 0);
    	            	for(Span s: reverseMatches){
    	            		matchingResults.add(new Span(attributeName, 
    	            				fieldValue.length() - s.getEnd(), 
    	            				fieldValue.length() - s.getStart(), 
    	            				subRegex.regex.getRegex(), 
    	            				fieldValue.substring(fieldValue.length() - s.getEnd(), fieldValue.length() - s.getStart())));
    	            	}
    	            }
	            }else{
	            	// start reverse matching (from right to left) from rightBound maximum of span starts to the left
	    			SpanListSummary rightBoundSummary = SpanListSummary.summerize(rightSpans);
    	            String reverseFieldValue = new StringBuilder(fieldValue).reverse().toString();
	            	List<Span> reverseMatches = 
	            			computeAllMatchingResults(attributeName, 
	            					reverseFieldValue.substring(fieldValue.length() - rightBoundSummary.startMax), 
	            			subRegex.getReverseSubRegex(), subRegex.getStart() != 0);
	            	for(Span s: reverseMatches){
	            		matchingResults.add(new Span(attributeName, 
	            				rightBoundSummary.startMax - s.getEnd(), 
	            				rightBoundSummary.startMax - s.getStart(), 
	            				subRegex.regex.getRegex(), 
	            				fieldValue.substring(rightBoundSummary.startMax - s.getEnd(), 
	            						rightBoundSummary.startMax - s.getStart())));
	            	}
	            }
	        }
    			
    	}else if(rightBound == null){ // && leftBound != null // one computed subRegex on left
    		List<Span> leftBoundSpans = leftBound.getSubSeq().getLatestMatchingSpanList();
	        for (String attributeName : subRegex.regex.getAttributeNames()) {
	            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
	            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
	            // types other than TEXT and STRING: throw Exception for now
	            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
	                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
	            }
	            // Filter the bound spans for the current attributes
	            List<Span> leftSpans = leftBoundSpans.
	            		stream().filter(s -> s.getAttributeName().equals(attributeName)).
	            		collect(Collectors.toList());
	    		if(subRegex.getStart() == leftBound.getSubSeq().getEnd()){ // Direct left neighbor is computed.
    	            for(Span leftSpan: leftSpans){
    	            	List<Span> spans = computeMatchingResultsStartingAt(attributeName, fieldValue, 
    	            			leftSpan.getEnd(), subRegex, ! subRegex.isLastSubSequence());
    	            	matchingResults.addAll(spans);
    	            }
	    		}else{ // left bound isn't direct.
	    			// start matching from leftBound minimum of span ends to the right
	    			SpanListSummary leftBoundSummary = SpanListSummary.summerize(leftBoundSpans);
    	            List<Span> spans = computeAllMatchingResults(attributeName, 
    	            		fieldValue.substring(leftBoundSummary.endMin), subRegex, 
    	            		! subRegex.isLastSubSequence());
    	            for(Span s: spans){
    	            	matchingResults.add(new Span(s.getAttributeName(), s.getStart() + leftBoundSummary.endMin, 
    	            			s.getEnd() + leftBoundSummary.endMin, s.getKey(), s.getValue()));
    	            }
	    		}
	        }
    	}else{ // rightBound != null && leftBound != null// two computed subRegexes on both sides
    		// start matching from the minimum end of the left bound spans to maximum start of the right bound spans
    		List<Span> leftBoundSpans = leftBound.getSubSeq().getLatestMatchingSpanList();
    		List<Span> rightBoundSpans = rightBound.getSubSeq().getLatestMatchingSpanList();
    		for (String attributeName : subRegex.regex.getAttributeNames()) {
    			AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
    			String fieldValue = inputTuple.getField(attributeName).getValue().toString();
    			// types other than TEXT and STRING: throw Exception for now
    			if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
    				throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
    			}
	            // Filter the bound spans for the current attributes
    			List<Span> leftSpans = leftBoundSpans.stream().filter(s -> s.getAttributeName().equals(attributeName)).collect(Collectors.toList());
    			SpanListSummary leftBoundSummary = SpanListSummary.summerize(leftSpans);
    			List<Span> rightSpans = rightBoundSpans.stream().filter(s -> s.getAttributeName().equals(attributeName)).collect(Collectors.toList());
    			SpanListSummary rightBoundSummary = SpanListSummary.summerize(rightSpans);
    			
    			if(leftBound.getSubSeq().getEnd() == subRegex.getStart() && subRegex.getEnd() == rightBound.getSubSeq().getStart()){ // left and right are both direct neighbors
    				String reverseFieldValue = null;

    				if(runReverse){
    					reverseFieldValue = new StringBuffer(fieldValue).reverse().toString();
    				}
    				for(Span leftSpan: leftSpans){
    					int start = leftSpan.getEnd();
    					for(Span rightSpan: rightSpans){
    						int end = rightSpan.getStart();
    						if(end < start){
    							continue;
    						}
							if(runReverse){
    							// verify in reverse direction..
    							if(doesMatchStartToEnd(attributeName, reverseFieldValue, 
    									reverseFieldValue.length() - end, reverseFieldValue.length() - start, 
    									subRegex.getReverseSubRegex())){
    								matchingResults.add(new Span(attributeName, start, end, subRegex.regex.getRegex(), 
    										fieldValue.substring(start, end)));
    							}
							}else{
    							// verify in normal left to right direction
    							if(doesMatchStartToEnd(attributeName, fieldValue, start, end, subRegex)){
    								matchingResults.add(new Span(attributeName, start, end, subRegex.regex.getRegex(), 
    										fieldValue.substring(start, end)));
    							}
							}
    					}
    				}
    			}else if(leftBound.getSubSeq().getEnd() == subRegex.getStart()){ // only left is direct neighbor
    				for(Span leftSpan: leftSpans){
    					int start = leftSpan.getEnd();
    					if(start >= rightBoundSummary.startMax){
    						continue;
    					}
    					List<Span> spans = computeMatchingResultsStartingAt(attributeName, 
    							fieldValue.substring(0, rightBoundSummary.startMax), 
    							start, subRegex, true);
    					matchingResults.addAll(spans);				
    				}
    			}else if(subRegex.getEnd() == rightBound.getSubSeq().getStart()){ // only right is direct neighbor
    				String reverseFieldValue = new StringBuffer(fieldValue).reverse().toString();
    				for(Span rightSpan: rightSpans){
    					int end = rightSpan.getStart();
    					if(end <= leftBoundSummary.endMin){
    						continue;
    					}
    					int reverseStart = fieldValue.length() - end;
    					int reverseEndBound = fieldValue.length() - leftBoundSummary.endMin;
    					List<Span> spans = computeMatchingResultsStartingAt(attributeName, reverseFieldValue.substring(0, reverseEndBound), 
    							reverseStart, subRegex.getReverseSubRegex(), true);
    					for(Span reverseSpan: spans){
    						matchingResults.add(new Span(attributeName, 
    								fieldValue.length() - reverseSpan.getEnd(), 
    								fieldValue.length() - reverseSpan.getStart(), 
    								subRegex.regex.getRegex(), 
    								fieldValue.substring(fieldValue.length() - reverseSpan.getEnd(), fieldValue.length() - reverseSpan.getStart())));
    					}
    				}
    				
    			}else { // no left nor right are direct neighbors
    				String fieldValueSubStr = fieldValue.substring(leftBoundSummary.endMin, rightBoundSummary.startMax);
					if(runReverse){
						// run in reverse order
    					String reverseFieldSubStr = new StringBuffer(fieldValueSubStr).reverse().toString();
    					List<Span> spans = computeAllMatchingResults(attributeName, reverseFieldSubStr, 
    							subRegex.getReverseSubRegex(), true);
    					for(Span s: spans){
    						matchingResults.add(new Span(s.getAttributeName(), 
    								rightBoundSummary.startMax - s.getEnd(), rightBoundSummary.startMax - s.getStart(), 
    								subRegex.regex.toString(),
    								fieldValue.substring(rightBoundSummary.startMax - s.getEnd(), 
    										rightBoundSummary.startMax - s.getStart())));
    					}
					}else{
						List<Span> spans = computeAllMatchingResults(attributeName, fieldValueSubStr, subRegex, true);
						for(Span s: spans){
							matchingResults.add(new Span(s.getAttributeName(), 
									s.getStart() + leftBoundSummary.endMin , s.getEnd() + leftBoundSummary.endMin , 
									s.getKey(), s.getValue()));
						}
					}
    			}
    		}
    	}
    	
    	return matchingResults;
    }
    
    public static List<Span> computeMatchingResultsStartingAt(String attributeName, String src, int start, SubRegex subRegex, boolean findAll){
    	if(start < 0 || start >= src.length()){
    		return new ArrayList<>();
    	}
    	Matcher matcher = subRegex.startWithRegexPattern.matcher(src.substring(start));
    	List<Span> matchingResults = new ArrayList<>();
    	boolean hasMore = findAll? matcher.proceed(): matcher.find();
    	while(hasMore){
    		matchingResults.add(new Span(attributeName, 
    				matcher.start() + start , 
    				matcher.end() + start, 
    				subRegex.regex.getRegex(), 
    				src.substring(matcher.start() + start , matcher.end() + start)));
    		hasMore = findAll? matcher.proceed(): matcher.find();
    	}
    	return matchingResults;
    }
    
    /*
     * Also returns overlapping and nested mathces. More expensive than computeMatchingResultsWithCompletePattern 
     * because it calls proceed() from the RegexMatcher rather than find().
     */
    public static List<Span> computeAllMatchingResults(Tuple inputTuple, SubRegex subRegex, 
    		boolean forcedExecutionDirection, boolean forcedReverse){
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : subRegex.regex.getAttributeNames()) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // either forced to be reverse, or is known to be better to be reverse
			boolean runReverse = (forcedExecutionDirection && forcedReverse) || 
					((!forcedExecutionDirection) && subRegex.isReverseExecutionFaster());
			if(runReverse){
				String reverseFieldValue = new StringBuilder(fieldValue).reverse().toString();
				Matcher javaMatcher = subRegex.getReverseSubRegex().regexPatern.matcher(reverseFieldValue);
				while (javaMatcher.proceed()) {
					int start = reverseFieldValue.length() - javaMatcher.end();
					int end = reverseFieldValue.length() - javaMatcher.start();
					matchingResults.add(
							new Span(attributeName, start, end, subRegex.regex.getRegex(), fieldValue.substring(start, end)));
				}
			}else{
				Matcher javaMatcher = subRegex.regexPatern.matcher(fieldValue);
				while (javaMatcher.proceed()) {
					int start = javaMatcher.start();
					int end = javaMatcher.end();
					matchingResults.add(
							new Span(attributeName, start, end, subRegex.regex.getRegex(), fieldValue.substring(start, end)));
				}
			}
        }

        return matchingResults;
    }
    
    public static List<Span> computeAllMatchingResults(String attributeName, String src, 
    		SubRegex subRegex, boolean findAll){
    	Matcher matcher = subRegex.regexPatern.matcher(src);
    	List<Span> matchingResults = new ArrayList<>();
    	boolean hasMore = findAll? matcher.proceed(): matcher.find();
    	while(hasMore){
    		matchingResults.add(new Span(attributeName, 
    				matcher.start(), 
    				matcher.end(), 
    				subRegex.regex.getRegex(), 
    				src.substring(matcher.start(), matcher.end())));
    		hasMore = findAll? matcher.proceed(): matcher.find();
    	}
    	return matchingResults;
    }
    
    public static boolean doesMatchStartToEnd(String attributeName, String src, int start, int end, SubRegex subRegex){
    	if(start < 0 || start >= src.length() || end < 0 || end >= src.length()){
    		return false;
    	}
    	
//    	System.out.println(src.substring(start, end));
//    	System.out.println(subRegex.toString());
    	Matcher matcher = subRegex.startToEndRegexPattern.matcher(src.substring(start, end));
    	return matcher.find();
    }
    
    public static int computeNumberOfNeededVerifications(Tuple inputTuple, SubRegex subRegex, SubPlan leftBound, SubPlan rightBound){
    	if(leftBound == null && rightBound == null){ // No computed subRegex is around the subRegex
        	// Default number of running the regex when there is no helping bound around it.
        	int totalSourceSize = 0;
        	for (String attributeName : subRegex.regex.getAttributeNames()) {
                AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
                String fieldValue = inputTuple.getField(attributeName).getValue().toString();
                // types other than TEXT and STRING: throw Exception for now
                if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                    throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
                }
                totalSourceSize += fieldValue.length();
        	}
    		return totalSourceSize;
    	}else if(leftBound == null){ // && rightBound != null // one computed subRegex on right
    		List<Span> rightBoundSpans = rightBound.getSubSeq().getLatestMatchingSpanList();
    		if(subRegex.getEnd() == rightBound.getSubSeq().getStart()){ // Direct right neighbor is computed.
    			return rightBoundSpans.size();
    		}else{ // right bound isn't direct.
    			// start reverse matching (from right to left) from rightBound maximum of span starts to the left
    			int totalMatchingSize = 0;
    	        for (String attributeName : subRegex.regex.getAttributeNames()) {
    	            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
    	            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
    	            // types other than TEXT and STRING: throw Exception for now
    	            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
    	                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
    	            }
    	            // span list must be pruned for only those spans that belong to the same attribute.
    	            SpanListSummary rightBoundSummary = 
    	            		SpanListSummary.summerize(
    	            				rightBoundSpans.stream().filter(
    	            						s -> s.getAttributeName().equals(attributeName)).collect(Collectors.toList()));
    	            totalMatchingSize += (fieldValue.length() - rightBoundSummary.startMax);
    	        }
    	        return totalMatchingSize;
    		}
    	}else if(rightBound == null){ // && leftBound != null // one computed subRegex on left
    		List<Span> leftBoundSpans = leftBound.getSubSeq().getLatestMatchingSpanList();
    		if(subRegex.getStart() == leftBound.getSubSeq().getEnd()){ // Direct left neighbor is computed.
    			return leftBoundSpans.size();
    		}else{ // left bound isn't direct.
    			// start matching from leftBound minimum of span ends to the right
    			int totalMatchingSize = 0;
    	        for (String attributeName : subRegex.regex.getAttributeNames()) {
    	            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
    	            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
    	            // types other than TEXT and STRING: throw Exception for now
    	            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
    	                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
    	            }
    	            SpanListSummary leftBoundSummary = SpanListSummary.summerize(
    	            		leftBoundSpans.stream().filter(
            						s -> s.getAttributeName().equals(attributeName)).collect(Collectors.toList()));
    	            totalMatchingSize += (fieldValue.length() - leftBoundSummary.endMin);
    	        }
    	        return totalMatchingSize;
    		}
    	}else{ // rightBound != null && leftBound != null// two computed subRegexes on both sides
    		// start matching from the minimum end of the left bound spans to maximum start of the right bound spans
    		List<Span> leftBoundSpans = leftBound.getSubSeq().getLatestMatchingSpanList();
    		List<Span> rightBoundSpans = rightBound.getSubSeq().getLatestMatchingSpanList();
    		int totalNumOfVerifications = 0;
    		for (String attributeName : subRegex.regex.getAttributeNames()) {
    			AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
    			String fieldValue = inputTuple.getField(attributeName).getValue().toString();
    			// types other than TEXT and STRING: throw Exception for now
    			if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
    				throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
    			}
    			List<Span> leftSpans = leftBoundSpans.stream().filter(s -> s.getAttributeName().equals(attributeName)).collect(Collectors.toList());
    			SpanListSummary leftBoundSummary = SpanListSummary.summerize(leftSpans);
    			List<Span> rightSpans = rightBoundSpans.stream().filter(s -> s.getAttributeName().equals(attributeName)).collect(Collectors.toList());
    			SpanListSummary rightBoundSummary = SpanListSummary.summerize(rightSpans);
    			if(leftBound.getSubSeq().getEnd() == subRegex.getStart() && subRegex.getEnd() == rightBound.getSubSeq().getStart()){ // left and right are both direct neighbors
    				for(Span leftSpan: leftSpans){
    					int start = leftSpan.getEnd();
    					for(Span rightSpan: rightSpans){
    						int end = rightSpan.getStart();
    						if(end < start){
    							continue;
    						}
    						totalNumOfVerifications ++;
    					}
    				}
    				
    			}else if(leftBound.getSubSeq().getEnd() == subRegex.getStart()){ // only left is direct neighbor
    				for(Span leftSpan: leftSpans){
    					int start = leftSpan.getEnd();
    					if(start >= rightBoundSummary.startMax){
    						continue;
    					}
    					totalNumOfVerifications ++;   					
    				}
    			}else if(subRegex.getEnd() == rightBound.getSubSeq().getStart()){ // only right is direct neighbor
    				for(Span rightSpan: rightSpans){
    					int end = rightSpan.getStart();
    					if(end <= leftBoundSummary.endMin){
    						continue;
    					}
    					totalNumOfVerifications ++;
    				}
    				
    			}else { // no left nor right are direct neighbors
    				String fieldValueSubStr = fieldValue.substring(leftBoundSummary.endMin, rightBoundSummary.startMax);
    				totalNumOfVerifications += fieldValueSubStr.length();
    			}
    		}
	        return totalNumOfVerifications;
    	}
    }
    
    public static List<Span> computeMatchingResultsWithCompletePattern(Tuple inputTuple, RegexPredicate predicate, Pattern pattern, boolean reverse, RegexPredicate revPredicate, Pattern revPattern) {
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : predicate.getAttributeNames()) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

//            System.out.println(fieldValue);
//            System.out.println("--------------------------");
//            System.out.println(predicate.getRegex());
            if(reverse){
            	String reverseFieldValue = new StringBuilder(fieldValue).reverse().toString();
            	Matcher javaMatcher = revPattern.matcher(reverseFieldValue);
            	while (javaMatcher.find()) {
            		int start = fieldValue.length() - javaMatcher.end();
            		int end = fieldValue.length() - javaMatcher.start();
            		matchingResults.add(
            				new Span(attributeName, start, end, predicate.getRegex(), fieldValue.substring(start, end)));
            	}            	
            }else{
            	Matcher javaMatcher = pattern.matcher(fieldValue);
            	while (javaMatcher.find()) {
            		int start = javaMatcher.start();
            		int end = javaMatcher.end();
            		matchingResults.add(
            				new Span(attributeName, start, end, predicate.getRegex(), fieldValue.substring(start, end)));
            	}
            }
        }

        return matchingResults;
    }
}
