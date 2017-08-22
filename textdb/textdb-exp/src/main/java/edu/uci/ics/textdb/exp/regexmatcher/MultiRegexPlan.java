package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.regexmatcher.SubRegex.ComplexityLevel;

/*
 * Processes the input tuples using an efficient permutation of 
 * a given concatenation of a given list of regexes.
 */
public class MultiRegexPlan extends QueryPlan{

	private List<SubPlan> subsForSelection = new ArrayList<>();
	private List<SubPlan> subsForVerification = new ArrayList<>();
	private List<SubPlan> subsForLabels = new ArrayList<>();
	
	private List<List<List<Span>>> warmUpFullSelectionResults = new ArrayList<>();
	
	private QueryGraphNode nullNodeToStart = new QueryGraphNode(null);
	private List<QueryGraphNode> subPlanQueryGraphNodes = new ArrayList<>();
	
	private Map<String, Integer> usedPlans = new HashMap<>(); //TODO: just for testing, remove.
	private void recordUsedPlan(String plan){
		if(! usedPlans.containsKey(plan)){
			usedPlans.put(plan, 0);
		}
		usedPlans.put(plan, usedPlans.get(plan) + 1);
	}
	
	private double sumCost = 0;
	
	public MultiRegexPlan(List<SubSequence> subSequences){
		for(SubSequence sub : subSequences){
			if(sub.isSubRegex()){
				SubRegex s = (SubRegex)sub;
				if(s.complexity == ComplexityLevel.High){
					subsForVerification.add(new SubPlan(s));
				}else{
					subsForSelection.add(new SubPlan(s));
				}
			}else{
				subsForLabels.add(new SubPlan(sub));
			}
		}
		if(subsForSelection.isEmpty() && subsForLabels.isEmpty()){ // TODO remove or replace with exception
			System.out.println("Unexpected. MultiRegexPlan constructor input is unacceptable (no labels and nothing for selection).");
		}
	}
	
	@Override
	protected List<Span> processTuple(Tuple inputTuple) {
		// Reset the per-tuple structures and prepare for execution
		subsForSelection.stream().forEach(s -> s.resetForNextTuple());
		subsForVerification.stream().forEach(s -> s.resetForNextTuple());
		subsForLabels.stream().forEach(s -> s.resetForNextTuple());
		for(SubPlan s : subsForLabels){
			executeSubPlan(inputTuple, s);
		}
		// Phase 0 (warm up):
		// If it's still warm up run all the sub-regexes and record statistics about their 
		// cost and selectivity.
		if(! isWarmUpFinished()){
			boolean tuplePassed = processWarmUpPhase(inputTuple);
			if(tuplePassed){
				return generateFullyCoveringMatchingSpans();
			}else{
				// tuple fails
				return null;
			}
		}
//		if(getProcessedTupleCounter() % 10000 == 0){
//			for(String plan : usedPlans.keySet()){
//				System.out.println(plan + " used " + usedPlans.get(plan) + " time.");
//			}
//		}
		long startTime = System.nanoTime();
		List<Span> result =  processPostWarmUpPhase(inputTuple);
		long endTime = System.nanoTime();
		sumCost += (endTime - startTime);
		return result;
	}
	
	private List<Span> processPostWarmUpPhase(Tuple inputTuple){
		String usedPlan = ""; // TODO: remove
		// 1. Try to fail the tuple by using the graph
		List<QueryGraphNode> passingNodes = new ArrayList<>();
		QueryGraphNode.startGreedyIterator(nullNodeToStart);
		QueryGraphNode nextToExecute = QueryGraphNode.getGreedyIteratorNextNode();
		while(nextToExecute != null){
			if(! nextToExecute.getSubPlan().isExecuted){
				usedPlan += nextToExecute.getSubPlan().getSubSeq().toStringShort(); //TODO remove
				List<Span> spans = executeSubPlan(inputTuple, nextToExecute.getSubPlan());
				if(spans != null && spans.isEmpty()){
					nextToExecute.getSubPlan().getSubSeq().resetMatchingSpanList(null);
				}
				if(spans == null || spans.isEmpty()){
					nullNodeToStart.increaseEdgeTo(nextToExecute);
					for(QueryGraphNode passingNode : passingNodes){
						passingNode.increaseEdgeTo(nextToExecute);
					}
					recordUsedPlan(usedPlan); // TODO: remove
					return null;
				}
				passingNodes.add(nextToExecute);
				QueryGraphNode.expandCurrentIteratorFrom(nextToExecute);
			}
			//
			nextToExecute = QueryGraphNode.getGreedyIteratorNextNode();
		}
//		System.out.println("Top node didn't help.");
		// 2. Not a normal case, try to find a failure in the rest of sub-plans for selection
		QueryGraphNode leastExpensiveFailure = null;
		double leastExpensiveFailureCost = Double.MAX_VALUE;
		for(int subPlanIndex = 0 ; subPlanIndex < subsForSelection.size(); subPlanIndex++){
			nextToExecute = subPlanQueryGraphNodes.get(subPlanIndex);
			if(nextToExecute.getSubPlan().isExecuted){
				continue;
			}
			usedPlan += nextToExecute.getSubPlan().getSubSeq().toStringShort();//TODO remove
			List<Span> spans = executeSubPlan(inputTuple, nextToExecute.getSubPlan());
			if(spans != null && spans.isEmpty()){
				nextToExecute.getSubPlan().getSubSeq().resetMatchingSpanList(null);
			}
			if(spans == null || spans.isEmpty()){
				if(leastExpensiveFailureCost > nextToExecute.getSubPlan().getExpectedCost()){
					leastExpensiveFailure = nextToExecute;
					leastExpensiveFailureCost = nextToExecute.getSubPlan().getExpectedCost();
				}
			}else{
				passingNodes.add(nextToExecute);
			}
		}
		if(leastExpensiveFailure != null){
			nullNodeToStart.increaseEdgeTo(leastExpensiveFailure);
			for(QueryGraphNode passingNode : passingNodes){
				passingNode.increaseEdgeTo(leastExpensiveFailure);
			}
//			//TODO remove print
//			printQueryGraphNodes();
			recordUsedPlan(usedPlan); // TODO: remove
			return null;
		}
//		System.out.println("All selection sub-plans passed.");
		// 3. if we are here, no selection sub-regex has failed:
		// prune the spans produced my selection sub-plans
		pruneSelectionMatchingSpans();
		// try verification sub-regexes.
//		System.out.println("Running verification sub-plans.");
		while(true){
			SubPlan nextToVerify = null;
			double nextToVerifyExpectedCost = Double.MAX_VALUE;
			for(SubPlan subPlan : subsForVerification){
				if(subPlan.isExecuted){
					continue;
				}
				if(subPlan.getExpectedCost() * (1.0 / (1.001 - subPlan.getSubSeq().stats.getSelectivity())) < nextToVerifyExpectedCost){
					nextToVerify = subPlan;
					nextToVerifyExpectedCost = subPlan.getExpectedCost() * (1.0 / (1.001 - subPlan.getSubSeq().stats.getSelectivity()));
				}
			}
			if(nextToVerify == null){
				break;
			}
			usedPlan += nextToVerify.getSubSeq().toStringShort();//TODO remove
			List<Span> spans = executeSubPlan(inputTuple, nextToVerify);
			if(spans != null && spans.isEmpty()){
				nextToVerify.getSubSeq().resetMatchingSpanList(null);
			}
			if(spans == null || spans.isEmpty()){
				recordUsedPlan(usedPlan); // TODO: remove
				return null;
			}
		}
//		System.out.println("Probably success. Generating fully covering spans.");
		// 4. If we are here, all sub-plans have passed.
		recordUsedPlan(usedPlan); // TODO: remove
		return generateFullyCoveringMatchingSpans();
	}

	@Override
	public boolean needsMoreStatistics() {
		if(! isWarmUpFinished()){
			return true;
		}
//		for(SubPlan s : subsForSelection){
//			if(s.sub.stats.getConfidenceValue() >= 0.3){
//				return true;
//			}
//		}
		return false;
	}

	@Override
	public double getExpectedCost() {
		return sumCost / getProcessedTupleCounter();
	}

	@Override
	protected void finishWarmUpPhase() {
		// 1. Create the graph nodes for all the sub-plans
		for(SubPlan s : subsForSelection){
			subPlanQueryGraphNodes.add(new QueryGraphNode(s));
		}
		for(SubPlan s : subsForVerification){
			subPlanQueryGraphNodes.add(new QueryGraphNode(s));
		}
		// 2. add information to the graph for warm up results
		for(int tupleIndex = 0 ; tupleIndex < warmUpFullSelectionResults.size(); tupleIndex ++){
			List<List<Span>> results = warmUpFullSelectionResults.get(tupleIndex);
			QueryGraphNode leastExpensiveFailure = null;
			double leastExpensiveFailureCost = Double.MAX_VALUE;
			double totalSuccessCost = 0;
			List<QueryGraphNode> passedSubPlanNodes = new ArrayList<>();
			for(int subIndex = 0 ; subIndex < subsForSelection.size(); subIndex++){
				QueryGraphNode node = subPlanQueryGraphNodes.get(subIndex);
				totalSuccessCost += node.getSubPlan().getExpectedCost() * node.getSubPlan().getSubSeq().stats.matchingSrcAvgLen;
				if(subIndex > results.size()){
					System.out.println("Unexpected state. Warmup result doesnt have enough elements."); //TODO remove when debugging is done.
				}
				List<Span> matchingList = results.get(subIndex);
				if(matchingList == null){
					if(node.getSubPlan().getExpectedCost() < leastExpensiveFailureCost){
						leastExpensiveFailureCost = node.getSubPlan().getExpectedCost();
						leastExpensiveFailure = node;
					}
				}else{
					passedSubPlanNodes.add(node);
				}
			}
			if(leastExpensiveFailure != null){ // It's not a success case
				nullNodeToStart.increaseEdgeTo(leastExpensiveFailure);
				for(QueryGraphNode passedNode : passedSubPlanNodes){
					passedNode.increaseEdgeTo(leastExpensiveFailure);
				}
				sumCost += leastExpensiveFailure.getSubPlan().getExpectedCost() * leastExpensiveFailure.getSubPlan().getSubSeq().stats.matchingSrcAvgLen;
			}else{
				sumCost += totalSuccessCost;
			}
		}
//		// TODO remove print
//		printQueryGraphNodes();
	}
	
	// Returns true if non of the sub-plans have failed us
	protected boolean processWarmUpPhase(Tuple inputTuple) {
		// First run/measure subPlans that are for selection
		boolean selectionPassed = true;
		for(SubPlan sPlan : subsForSelection){
			List<Span> spans = executeSubPlan(inputTuple, sPlan);
			if(spans == null || spans.isEmpty()){
				selectionPassed = false;
			}
			if(spans != null && spans.isEmpty()){
				sPlan.getSubSeq().resetMatchingSpanList(null);
			}
		}
		// Try to prune the produced spans of adjacent sub-regexes (those for selection)
		pruneSelectionMatchingSpans();
		// Remember the results of this tuple for selection sub-plans
		List<List<Span>> tupleSubPlanResults = new ArrayList<>();
		subsForSelection.stream().forEach(subPlan -> tupleSubPlanResults.add(subPlan.getSubSeq().getLatestMatchingSpanList()));
		warmUpFullSelectionResults.add(tupleSubPlanResults);
		// Done with selection sub-plans.
		if(! selectionPassed){
			return false;
		}
		// Now check the sub-plans for verification
		boolean verificationPassed = true;
		for(SubPlan sPlan : subsForVerification){
			List<Span> spans = executeSubPlan(inputTuple, sPlan);
			if(spans == null || spans.isEmpty()){
				verificationPassed = false;
			}
			if(spans != null && spans.isEmpty()){
				sPlan.getSubSeq().resetMatchingSpanList(null);
			}
		}
		// Remember the results of this tuple for verification sub-plans
		List<List<Span>> tupleSubPlanVerificationResults = new ArrayList<>();
		subsForVerification.stream().forEach(subPlan -> tupleSubPlanVerificationResults.add(subPlan.getSubSeq().getLatestMatchingSpanList()));
		warmUpFullSelectionResults.get(warmUpFullSelectionResults.size() - 1).addAll(tupleSubPlanVerificationResults);
		return verificationPassed;
	}
	
	
	private List<Span> generateFullyCoveringMatchingSpans(){
		int startingSubRegexIndex = 0;
		List<List<Span>> spanLists = new ArrayList<>();
		while(true){
			SubPlan nextSubPlan = null;
			for(SubPlan sPlan : subsForSelection){
				if(nextSubPlan == null && sPlan.getSubSeq().getStart() >= startingSubRegexIndex){
					nextSubPlan = sPlan;
					continue;
				}
				if(sPlan.getSubSeq().getStart() >= startingSubRegexIndex && sPlan.getSubSeq().getStart() < nextSubPlan.getSubSeq().getStart()){
					nextSubPlan = sPlan;
				}
			}
			for(SubPlan sPlan : subsForVerification){
				if(nextSubPlan == null && sPlan.getSubSeq().getStart() >= startingSubRegexIndex){
					nextSubPlan = sPlan;
					continue;
				}
				if(sPlan.getSubSeq().getStart() >= startingSubRegexIndex && sPlan.getSubSeq().getStart() < nextSubPlan.getSubSeq().getStart()){
					nextSubPlan = sPlan;
				}
			}
			for(SubPlan sPlan : subsForLabels){
				if(nextSubPlan == null && sPlan.getSubSeq().getStart() >= startingSubRegexIndex){
					nextSubPlan = sPlan;
					continue;
				}
				if(sPlan.getSubSeq().getStart() >= startingSubRegexIndex && sPlan.getSubSeq().getStart() < nextSubPlan.getSubSeq().getStart()){
					nextSubPlan = sPlan;
				}
			}
			if(nextSubPlan == null){
				break;
			}
			if(nextSubPlan.getSubSeq().getLatestMatchingSpanList() == null){
				System.out.println("Unexpected state. A sub-plan is failed in a success tuple.");// TODO: remove or replace with exception
				
				
				return null;
			}
			spanLists.add(nextSubPlan.getSubSeq().getLatestMatchingSpanList());
			startingSubRegexIndex = nextSubPlan.getSubSeq().getEnd();
		}
		// Now, make all possible fully covering matches.
		List<Span> expandingSpanList = new ArrayList<>();
		for(int i = 0 ; i < spanLists.size(); i++){
			List<Span> spanList = spanLists.get(i);
			if(i == 0){
				expandingSpanList.addAll(spanList);
				continue;
			}
			List<Span> newExpandingList = new ArrayList<>();
			for(Span s: expandingSpanList){
				for(Span nextS : spanList){
					if(s.getAttributeName().equals(nextS.getAttributeName()) && s.getEnd() == nextS.getStart()){
						newExpandingList.add(new Span(s.getAttributeName(), s.getStart(), nextS.getEnd(), s.getKey() + nextS.getKey(), s.getValue() + nextS.getValue()));
					}
				}
			}
			expandingSpanList.clear();
			expandingSpanList.addAll(newExpandingList);
		}
		return expandingSpanList;
	}
	
	// Finds adjacent successful subPlans and prunes their matching spans 
	// to keep those that will participate in a bigger match
	private void pruneSelectionMatchingSpans(){
		int startingSubRegexIndex = 0;
		List<SubPlan> subPlansToMerge = new ArrayList<>();
		while(true){
			SubPlan nextSubPlan = null;
			for(SubPlan sPlan : subsForSelection){
				if(nextSubPlan == null && sPlan.getSubSeq().getStart() >= startingSubRegexIndex){
					nextSubPlan = sPlan;
					continue;
				}
				if(sPlan.getSubSeq().getStart() >= startingSubRegexIndex && sPlan.getSubSeq().getStart() < nextSubPlan.getSubSeq().getStart()){
					nextSubPlan = sPlan;
				}
			}
			for(SubPlan sPlan : subsForLabels){
				if(nextSubPlan == null && sPlan.getSubSeq().getStart() >= startingSubRegexIndex){
					nextSubPlan = sPlan;
					continue;
				}
				if(sPlan.getSubSeq().getStart() >= startingSubRegexIndex && sPlan.getSubSeq().getStart() < nextSubPlan.getSubSeq().getStart()){
					nextSubPlan = sPlan;
				}
			}
			if(nextSubPlan == null){
				break;
			}
			if(nextSubPlan.getSubSeq().getLatestMatchingSpanList() == null || nextSubPlan.getSubSeq().getLatestMatchingSpanList().isEmpty()){
				if(! subPlansToMerge.isEmpty()){
					pruneSpanListsAndUpdateStats(subPlansToMerge);
					subPlansToMerge.clear();
				}
				startingSubRegexIndex = nextSubPlan.getSubSeq().getEnd();
				continue;
			}
			if(nextSubPlan.getSubSeq().getStart() > startingSubRegexIndex){
				if(! subPlansToMerge.isEmpty()){
					pruneSpanListsAndUpdateStats(subPlansToMerge);
					subPlansToMerge.clear();
				}
			}
			subPlansToMerge.add(nextSubPlan);
			startingSubRegexIndex = nextSubPlan.getSubSeq().getEnd();
		}
		if(! subPlansToMerge.isEmpty()){
			pruneSpanListsAndUpdateStats(subPlansToMerge);
			subPlansToMerge.clear();
		}
	}
	
	private void pruneSpanListsAndUpdateStats(List<SubPlan> subPlans){
		List<List<Span>> spanLists = new ArrayList<>();
		subPlans.stream().forEach(s -> spanLists.add(s.getSubSeq().getLatestMatchingSpanList()));
		List<List<Span>> newSpanLists = null;
		if(subPlans.size() == 1){
			newSpanLists = spanLists;
		}else{
			newSpanLists = pruneMergingSpans(spanLists);
		}
		for(int i = 0 ; i < newSpanLists.size(); i++){
			List<Span> newSpanList = newSpanLists.get(i);
			subPlans.get(i).getSubSeq().resetMatchingSpanList(newSpanList);
			List<Integer> matchSizesPerAttributes = new ArrayList<>();
			subPlans.get(i).getSubSeq().getAttributeNames().stream().
				forEach(attr -> matchSizesPerAttributes.add(
						newSpanList.stream().filter(s -> s.getAttributeName().equals(attr)).
												collect(Collectors.toList()).size()
				));
			subPlans.get(i).getSubSeq().stats.addTfAveragePerAttribute(matchSizesPerAttributes);
		}
	}
	// Each spanList in spanLists is the matching spans of one sub-regex 
	// For each spanList, we want to prune it to those spans that actually participate in a 
	// end-to-end expanded span.
	private List<List<Span>> pruneMergingSpans(List<List<Span>> spanLists){
		List<List<Span>> newSpanLists = new ArrayList<>();
		List<Span> expandingSpanList = new ArrayList<>();
		for(int i = 0 ; i < spanLists.size(); i++){
			List<Span> spanList = spanLists.get(i);
			List<Span> newSpanList = new ArrayList<>();
			if(i == 0){
				expandingSpanList.addAll(spanList);
				newSpanList.addAll(spanList);
				newSpanLists.add(newSpanList);
				continue;
			}
			List<Span> newExpandingList = new ArrayList<>();
			for(Span s: expandingSpanList){
				for(Span nextS : spanList){
					if(s.getAttributeName().equals(nextS.getAttributeName()) && s.getEnd() == nextS.getStart()){
						// Note: expanding lists are just temporary, we don't need key/value
						newExpandingList.add(new Span(s.getAttributeName(), s.getStart(), nextS.getEnd(), "", ""));
						if(! newSpanList.contains(nextS)){
							newSpanList.add(nextS);
						}
					}
				}
			}
			expandingSpanList.clear();
			expandingSpanList.addAll(newExpandingList);
			newSpanLists.add(newSpanList);
		}
		List<List<Span>> resultSpanLists = new ArrayList<>();
		for(int i = 0 ; i < newSpanLists.size(); i++){
			resultSpanLists.add(new ArrayList<>());
		}
		expandingSpanList.clear();
		for(int i = newSpanLists.size() - 1; i >= 0; i--){
			List<Span> spanList = newSpanLists.get(i);
			List<Span> newSpanList = resultSpanLists.get(i);
			if(i == newSpanLists.size() - 1){
				expandingSpanList.addAll(spanList);
				newSpanList.addAll(spanList);
				continue;
			}
			List<Span> newExpandingList = new ArrayList<>();
			for(Span s: expandingSpanList){
				for(Span nextS : spanList){
					if(s.getAttributeName().equals(nextS.getAttributeName()) && s.getStart() == nextS.getEnd()){
						// Note: expanding lists are just temporary, we don't need key/value
						newExpandingList.add(new Span(s.getAttributeName(), nextS.getStart(), s.getEnd(), "", ""));
						if(! newSpanList.contains(nextS)){
							newSpanList.add(nextS);
						}
					}
				}
			}
			expandingSpanList.clear();
			expandingSpanList.addAll(newExpandingList);
		}
		return resultSpanLists;
	}
	
	// Runs the given sub-plan on the given tuple. Some statistics like
	// cost samples and the total selectivity are updated for all the input
	// tuples regardless of the phase we are in.
	// Statistics of reverse regex executions are only collected in warmUp phase.
	private List<Span> executeSubPlan(Tuple inputTuple, SubPlan subPlan){
		if(subPlan.isExecuted){
			return subPlan.getSubSeq().getLatestMatchingSpanList();
		}
		
		// Execute the sub-plan
		subPlan.isExecuted = true;
		
		if(subPlan.isLabelSubSequence()){
			LabelSubSequence labelSub = (LabelSubSequence)subPlan.getSubSeq();
	        ListField<Span> spanListField = inputTuple.getField(labelSub.labelName);
			List<Span> newSpanList = new ArrayList<>();
			newSpanList.addAll(spanListField.getValue());
			subPlan.getSubSeq().resetMatchingSpanList(newSpanList);
			return newSpanList;
		}
		
		// Prepare the set of already executed sub-regexes
		Set<SubPlan> executedSubPlans = new HashSet<>();
		for(SubPlan sPlan : subsForSelection){
			if(sPlan.isExecuted){
				executedSubPlans.add(sPlan);
			}
		}
		for(SubPlan sPlan : subsForLabels){
			if(sPlan.isExecuted){
				executedSubPlans.add(sPlan);
			}
		}
		if(subPlan.isForVerification()){
			for(SubPlan sPlan : subsForVerification){
				if(sPlan.isExecuted){
					executedSubPlans.add(sPlan);
				}
			}
		}
		
		if(subPlan.isForVerification()){ // High sub-regex
			// Find the closest computed sub-regex to the left and right
			SubPlan leftBound = RegexMatcherUtils.
					findSubRegexNearestComputedLeftNeighbor(executedSubPlans, subPlan);
			SubPlan rightBound = RegexMatcherUtils.
					findSubRegexNearestComputedRightNeighbor(executedSubPlans, subPlan);
			if(! needsMoreStatistics()){
				long startMatchingTime = System.nanoTime();
				List<Span> subRegexSpans = 
						RegexMatcherUtils.computeSubRegexMatchesWithComputedNeighbors(inputTuple, subPlan.getSubRegex(), 
								leftBound, rightBound, false, false); // Not forcing the reverse/forward directions
				long endMatchingTime = System.nanoTime();
				long matchingTime = endMatchingTime - startMatchingTime;
				int numVerifications = 
						RegexMatcherUtils.computeNumberOfNeededVerifications(inputTuple, subPlan.getSubRegex(), leftBound, rightBound);
				double verificationCost = (matchingTime * 1.0) / Math.max(numVerifications, 1);
				// Save the span list for this subregex to be used in the second phase of the algorithm.
				subPlan.getSubRegex().resetMatchingSpanList(subRegexSpans);
				// Update the statistics
				if(subRegexSpans == null || subRegexSpans.isEmpty()){
					// no matches
					// update statistics with the cost of failure
					if(subPlan.getSubRegex().isReverseExecutionFaster()){
						subPlan.getSubRegex().stats.addStatsSubRegexFailure(0, numVerifications);
						subPlan.getSubRegex().getReverseSubRegex().stats.addStatsSubRegexFailure(verificationCost, numVerifications);
					}else{
						subPlan.getSubRegex().stats.addStatsSubRegexFailure(verificationCost, numVerifications);
					}
				}else{
					// some matches exist
					// update statistics upon success (df, tf, and success cost)
//					List<Integer> matchSizesPerAttributes = new ArrayList<>();
//					subPlan.sub.regex.getAttributeNames().stream().
//						forEach(attr -> matchSizesPerAttributes.add(
//								subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
//														collect(Collectors.toList()).size()
//						));
					if(subPlan.getSubRegex().isReverseExecutionFaster()){
						subPlan.getSubRegex().stats.addStatsSubRegexSuccess(0, numVerifications);
						subPlan.getSubRegex().getReverseSubRegex().stats.addStatsSubRegexSuccess(verificationCost, numVerifications);
					}else{
						subPlan.getSubRegex().stats.addStatsSubRegexSuccess(verificationCost, numVerifications);
					}
				}
				return subRegexSpans;
			}
			// Warm up is not finished. Even reverse executions should also be tried
			// Case 1.
			{	// measure the time of verification if we use direct order
				long startMatchingTime = System.nanoTime();
				List<Span> subRegexSpans = 
						RegexMatcherUtils.computeSubRegexMatchesWithComputedNeighbors(inputTuple, subPlan.getSubRegex(), 
								leftBound, rightBound, true, false); // force not to use reverse execution
				long endMatchingTime = System.nanoTime();
				long matchingTime = endMatchingTime - startMatchingTime;
				int numVerifications = 
						RegexMatcherUtils.computeNumberOfNeededVerifications(inputTuple, subPlan.getSubRegex(), leftBound, rightBound);
				double verificationCost = (matchingTime * 1.0) / Math.max(numVerifications, 1);
				// Save the span list for this subregex to be used in the second phase of the algorithm.
				subPlan.getSubRegex().resetMatchingSpanList(subRegexSpans);
				if(subRegexSpans == null || subRegexSpans.isEmpty()){
					// no matches
					// update statistics with the cost of failure
					subPlan.getSubRegex().stats.addStatsSubRegexFailure(verificationCost, numVerifications);
				}else{
					// some matches exist
					// update statistics upon success (df, tf, and success cost)
//					List<Integer> matchSizesPerAttributes = new ArrayList<>();
//					subPlan.sub.regex.getAttributeNames().stream().
//						forEach(attr -> matchSizesPerAttributes.add(
//								subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
//														collect(Collectors.toList()).size()
//						));
					subPlan.getSubRegex().stats.addStatsSubRegexSuccess(verificationCost, numVerifications);
				}
			}
			// Case 2.
			{	// measure the time of verification if we use reverse order
				long startMatchingTime = System.nanoTime();
				List<Span> subRegexSpans = 
						RegexMatcherUtils.computeSubRegexMatchesWithComputedNeighbors(inputTuple, subPlan.getSubRegex(), 
								leftBound, rightBound, true, true); // force to use reverse execution
				long endMatchingTime = System.nanoTime();
				long matchingTime = endMatchingTime - startMatchingTime;
				int numVerifications = RegexMatcherUtils.computeNumberOfNeededVerifications(inputTuple, subPlan.getSubRegex(), leftBound, rightBound);
				double verificationCost = (matchingTime * 1.0) / Math.max(numVerifications, 1);
				// Save the span list for this subregex to be used in the second phase of the algorithm.
				subPlan.getSubRegex().resetMatchingSpanList(subRegexSpans);
				if(subRegexSpans == null || subRegexSpans.isEmpty()){
					// no matches
					// update statistics with the cost of failure
					subPlan.getSubRegex().getReverseSubRegex().stats.addStatsSubRegexFailure(verificationCost, numVerifications);
				}else{
					// some matches exist
					// update statistics upon success (df, tf, and success cost)
//					List<Integer> matchSizesPerAttributes = new ArrayList<>();
//					subPlan.sub.regex.getAttributeNames().stream().
//						forEach(attr -> matchSizesPerAttributes.add(
//								subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
//														collect(Collectors.toList()).size()
//						));
					subPlan.getSubRegex().getReverseSubRegex().stats.addStatsSubRegexSuccess(verificationCost, numVerifications);
				}
			}
			return subPlan.getSubRegex().getLatestMatchingSpanList();
		}
		
		// SubPlan is for Selection (uses a non-high sub-regex)
		// Find left and right neighboring labels if there exist any
		// SubRegex doesn't have * or +, so we will find all matches to also measure tf_average
		if(! needsMoreStatistics()){ // Warmup finished. We don't force to try reverse execution.
			long startMatchingTime = System.nanoTime();
			List<Span> subRegexSpans = RegexMatcherUtils.computeAllMatchingResults(inputTuple, subPlan.getSubRegex(), false, false);;
			long endMatchingTime = System.nanoTime();
			long matchingTime = endMatchingTime - startMatchingTime;
			int totalTupleTextSize = 1;
			for(String attributeName : subPlan.getSubRegex().regex.getAttributeNames()){
				totalTupleTextSize += inputTuple.getField(attributeName).getValue().toString().length();
			}
			double matchingCost = (matchingTime * 1.0) / totalTupleTextSize;
			// also save the span list for this subregex to be used in the second phase of the algorithm.
			subPlan.getSubRegex().resetMatchingSpanList(subRegexSpans);
			if(subRegexSpans == null || subRegexSpans.isEmpty()){
				// no matches
				// update statistics with the cost of failure
				if(subPlan.getSubRegex().isReverseExecutionFaster()){
					subPlan.getSubRegex().stats.addStatsSubRegexFailure(0, totalTupleTextSize);
					subPlan.getSubRegex().getReverseSubRegex().stats.addStatsSubRegexFailure(matchingCost, totalTupleTextSize);
				}else{
					subPlan.getSubRegex().stats.addStatsSubRegexFailure(matchingCost, totalTupleTextSize);
				}
			}else{
				// some matches exist
				// update statistics upon success (df, tf, and success cost)
//				List<Integer> matchSizesPerAttributes = new ArrayList<>();
//				subPlan.sub.regex.getAttributeNames().stream().
//					forEach(attr -> matchSizesPerAttributes.add(
//							subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
//													collect(Collectors.toList()).size()
//					));
				if(subPlan.getSubRegex().isReverseExecutionFaster()){
					subPlan.getSubRegex().stats.addStatsSubRegexSuccess(0, totalTupleTextSize);
					subPlan.getSubRegex().getReverseSubRegex().stats.addStatsSubRegexSuccess(matchingCost, totalTupleTextSize);
				}else{
					subPlan.getSubRegex().stats.addStatsSubRegexSuccess(matchingCost, totalTupleTextSize);
				}
			}
			return subRegexSpans;
		}
		// Warmup not finished
		// Case 1.
		// Collect statistics for running normally
		{
			long startMatchingTime = System.nanoTime();
			List<Span> subRegexSpans = RegexMatcherUtils.computeAllMatchingResults(inputTuple, subPlan.getSubRegex(), true, false);;
			long endMatchingTime = System.nanoTime();
			long matchingTime = endMatchingTime - startMatchingTime;
			int totalTupleTextSize = 1;
			for(String attributeName : subPlan.getSubRegex().regex.getAttributeNames()){
				totalTupleTextSize += inputTuple.getField(attributeName).getValue().toString().length();
			}
			double matchingCost = (matchingTime * 1.0) / totalTupleTextSize;
			// also save the span list for this subregex to be used in the second phase of the algorithm.
			subPlan.getSubRegex().resetMatchingSpanList(subRegexSpans);
			if(subRegexSpans == null || subRegexSpans.isEmpty()){
				// no matches
				// update statistics with the cost of failure
				subPlan.getSubRegex().stats.addStatsSubRegexFailure(matchingCost, totalTupleTextSize);
			}else{
				// some matches exist
				// update statistics upon success (df, tf, and success cost)
//				List<Integer> matchSizesPerAttributes = new ArrayList<>();
//				subPlan.sub.regex.getAttributeNames().stream().
//					forEach(attr -> matchSizesPerAttributes.add(
//							subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
//													collect(Collectors.toList()).size()
//					));
				subPlan.getSubRegex().stats.addStatsSubRegexSuccess(matchingCost, totalTupleTextSize);
			}
		}
		// Case 2.
		// Collect statistics for running in reverse
		{
			long startMatchingTime = System.nanoTime();
			List<Span> subRegexSpans = RegexMatcherUtils.computeAllMatchingResults(inputTuple, subPlan.getSubRegex(), true, true);;
			long endMatchingTime = System.nanoTime();
			long matchingTime = endMatchingTime - startMatchingTime;
			int totalTupleTextSize = 1;
			for(String attributeName : subPlan.getSubRegex().regex.getAttributeNames()){
				totalTupleTextSize += inputTuple.getField(attributeName).getValue().toString().length();
			}
			double matchingCost = (matchingTime * 1.0) / totalTupleTextSize;
			// also save the span list for this subregex to be used in the second phase of the algorithm.
			subPlan.getSubRegex().resetMatchingSpanList(subRegexSpans);
			if(subRegexSpans == null || subRegexSpans.isEmpty()){
				// no matches
				// update statistics with the cost of failure
				subPlan.getSubRegex().getReverseSubRegex().stats.addStatsSubRegexFailure(matchingCost, totalTupleTextSize);
			}else{
				// some matches exist
				// update statistics upon success (df, tf, and success cost)
//				List<Integer> matchSizesPerAttributes = new ArrayList<>();
//				subPlan.sub.regex.getAttributeNames().stream().
//					forEach(attr -> matchSizesPerAttributes.add(
//							subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
//													collect(Collectors.toList()).size()
//					));
				subPlan.getSubRegex().getReverseSubRegex().stats.addStatsSubRegexSuccess(matchingCost, totalTupleTextSize);
			}
		}
		return subPlan.getSubRegex().getLatestMatchingSpanList();	
	}
	
	public void printQueryGraphNodes(){
		for(QueryGraphNode node : subPlanQueryGraphNodes){
			System.out.println("=========================================================");
			System.out.println(node.getSubPlan().getSubSeq().toStringShort());
			Map<QueryGraphNode, Integer> outEdges = node.getPossibleNextMovesPopularities();
			for(QueryGraphNode outNode : outEdges.keySet()){
				System.out.println(outNode.getSubPlan().getSubSeq().toStringShort() + " -> " + outEdges.get(outNode));
			}
		}
	}

}

class SubPlan{
	public SubSequence subSequence = null;
	public boolean isExecuted = false;
	SubPlan(SubSequence sub){
		if(sub == null){
			System.out.println("SubPlan constructor wrong input !!!!!"); //TODO remove or replace with exception
		}
		this.subSequence = sub;
	}
	public void resetForNextTuple(){
		isExecuted = false;
		subSequence.resetMatchingSpanList(null);
	}
	
	// Returns whether or not this sub-plan has had any matches in the most recent tuple.
	public boolean isSuccessful(){
		return ! (subSequence.latestMatchingSpans == null || subSequence.latestMatchingSpans.isEmpty());
	}
	
	// Returns whether or not the sub-regex is a high sub-regex. (High is for verification)
	public boolean isForVerification(){
		if(! subSequence.isSubRegex()){
			return false;
		}

		return getSubRegex().complexity == ComplexityLevel.High;
	}
	
	public double getExpectedCost(){
		if(! subSequence.isSubRegex()){
			return 0.0;
		}
		return getSubRegex().getExpectedCost();
	}
	
	public double getSelectivity(){
		if(! subSequence.isSubRegex()){
			return 1.0;
		}
		return getSubRegex().stats.getSelectivity();
	}
	
	public SubRegex getSubRegex(){
		if(! subSequence.isSubRegex()){
			System.out.println("Unexpected call to the getSubRegex method in SubPlan.");
			return null;
		}
		return (SubRegex)subSequence;
	}
	
	public SubSequence getSubSeq(){
		return subSequence;
	}
	
	public boolean isLabelSubSequence(){
		return ! subSequence.isSubRegex();
	}
}

class QueryGraphNode{
	private SubPlan subPlan;
	private Map<QueryGraphNode, Integer> nextNodePopularity = new HashMap<>();
	public QueryGraphNode(SubPlan s) {
		subPlan = s;
	}
	// Returns true if this is a new edge.
	public boolean increaseEdgeTo(QueryGraphNode toNode){
		if(! nextNodePopularity.containsKey(toNode)){
			nextNodePopularity.put(toNode, 0);
		}
		nextNodePopularity.put(toNode, nextNodePopularity.get(toNode) + 1);
		return nextNodePopularity.get(toNode) == 1;
	}
	
	public Map<QueryGraphNode, Double> getPossibleNextMovesWithCosts(){
		Map<QueryGraphNode, Double> results = new HashMap<>();
		for(QueryGraphNode nextNode : nextNodePopularity.keySet()){
			int popularity = nextNodePopularity.get(nextNode);
			results.put(nextNode, calculateCombinedScore(popularity * 1.0 / getTotalNumberOfSamples(), 
					1.001 - nextNode.getSubPlan().getSubRegex().stats.getSelectivity(), nextNode.getSubPlan().getSubRegex().getExpectedCost()));
		}
		return results;
	}

	public Map<QueryGraphNode, Integer> getPossibleNextMovesPopularities(){
		return nextNodePopularity;
	}
	
	// Note: The output of this method will be "minimized".
	private double calculateCombinedScore(double popularity, double failureProbability, double expectedCost){
		return expectedCost / 
				(popularity * failureProbability);
	}
	
	private int getTotalNumberOfSamples(){
		int total = 0;
		for(QueryGraphNode nextNode : nextNodePopularity.keySet()){
			int popularity = nextNodePopularity.get(nextNode);
			total += popularity;
		}
		return total;
	}
	
	public SubPlan getSubPlan(){
		return subPlan;
	}
	
	////// Graph traversal
	static Map<QueryGraphNode, Double> nextMoveCandidates = null;
	// Resets the iterator for trying next nodes...
	public static void startGreedyIterator(QueryGraphNode startingNode){
		nextMoveCandidates = startingNode.getPossibleNextMovesWithCosts();
	}
	
	public static void expandCurrentIteratorFrom(QueryGraphNode startingNode){
		Map<QueryGraphNode, Double> newCandidates = startingNode.getPossibleNextMovesWithCosts();
		for(QueryGraphNode newNode : newCandidates.keySet()){
			if(! nextMoveCandidates.containsKey(newNode)){
				nextMoveCandidates.put(newNode, newCandidates.get(newNode));
			}else{
				if(newCandidates.get(newNode) < nextMoveCandidates.get(newNode)){
					nextMoveCandidates.put(newNode, newCandidates.get(newNode));
				}
			}
		}
	}
	
	// End of iteration if returns null;
	public static QueryGraphNode getGreedyIteratorNextNode(){
		QueryGraphNode minScoreNode = null;
		double minScore = Double.MAX_VALUE;
		for(QueryGraphNode node : nextMoveCandidates.keySet()){
			if(nextMoveCandidates.get(node) < minScore){
				minScoreNode = node;
				minScore = nextMoveCandidates.get(node);
			}
		}
		if(minScoreNode != null){
			nextMoveCandidates.remove(minScoreNode);
		}
		return minScoreNode;
	}
}


