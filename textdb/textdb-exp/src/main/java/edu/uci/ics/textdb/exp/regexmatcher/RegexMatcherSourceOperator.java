package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import edu.uci.ics.textdb.api.constants.DataConstants;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.storage.DataReader;
import edu.uci.ics.textdb.storage.RelationManager;

public class RegexMatcherSourceOperator extends AbstractSingleInputOperator implements ISourceOperator {
    
    private final RegexSourcePredicate predicate;

    private final DataReader dataReader;
    private final RegexMatcher regexMatcher;
    
    public RegexMatcherSourceOperator(RegexSourcePredicate predicate) throws StorageException, DataFlowException {
        this.predicate = predicate;
        
        if (this.predicate.isUseIndex()) {
            this.dataReader = RelationManager.getRelationManager().getTableDataReader(this.predicate.getTableName(), 
//                    createLuceneQuery(this.predicate));
            		createLucenePhraseQuery(this.predicate));
            
        } else {
            this.dataReader = RelationManager.getRelationManager().getTableDataReader(this.predicate.getTableName(), 
                    new MatchAllDocsQuery());
        }
        
        regexMatcher = new RegexMatcher(this.predicate);
        regexMatcher.setInputOperator(dataReader);
        
        this.inputOperator = this.regexMatcher;
    }

    @Override
    protected void setUp() throws TextDBException {
        this.outputSchema = regexMatcher.getOutputSchema();        
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        return this.regexMatcher.getNextTuple();
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        return this.regexMatcher.processOneInputTuple(inputTuple);
    }

    @Override
    protected void cleanUp() throws TextDBException {
    }
    
    /*
     * This method reads the boolean tree of grams that is prepared by RegexToGramQueryTranslator and creates 
     * a Lucene query from it. This Lucene query is a boolean combination of phrase queries.
     * 
     * Input example: (abc(0,0) AND bcd(0,1) AND mno(1,0) AND nop(1,1)) OR (wxy(2,0) AND xyz(2,1))
     * In bcd(0,1) 0 is the group id and 1 is the position index of 'bcd' in 'abcd'.
     * Output for the above example:
     * 		OR (
     *            AND ( 
     *                          Phrase((abc,0), (bcd,1)), 
     *                          Phrase((mno,0), (nop,1))
     *                ) , 
     *            Phrase((wxy,0), (xyz,1))
     *         )
     */
    public static Query createLucenePhraseQuery(RegexSourcePredicate predicate) throws DataFlowException {
    	
    	// TODO: See if there is another solution to use all the attributes.
    	// Currently Lucene only supports one attribute name when a Term is added to 
    	// a phrase query. We need to make phrase queries that accept a list of attribute names.
    	String fieldName = predicate.getAttributeNames().get(0);
    	
        GramBooleanQuery queryTree;
        // Try to apply translator. If it fails, use scan query.
        try {
            queryTree = RegexToGramQueryTranslator.translate(predicate.getRegex());
        } catch (com.google.re2j.PatternSyntaxException e) {
            queryTree = null;
        }
    	if(queryTree == null || queryTree.subQuerySet.isEmpty()){
    		try {
    			return new MultiFieldQueryParser(
    					predicate.getAttributeNames().stream().toArray(String[]::new), 
    					RelationManager.getRelationManager().getTableAnalyzer(predicate.getTableName()))
    					.parse(DataConstants.SCAN_QUERY);
            } catch (ParseException e) {
                throw new StorageException (e);
            }
    	}
//        System.out.println(queryTree.printQueryTree());
    	if(queryTree.operator == GramBooleanQuery.QueryOp.AND){
    		GramBooleanQuery newRoot = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
    		newRoot.subQuerySet.add(queryTree);
    		queryTree = newRoot;
    	}
    	
        BooleanQuery.Builder booleanQueryBuilderOr = new BooleanQuery.Builder();
        /*
         * Generating:
	     * 		OR (
	     *            AND ( 
	     *                          Phrase((abc,0), (bcd,1)), 
	     *                          Phrase((mno,0), (nop,1))
	     *                ) , 
	     *            Phrase((wxy,0), (xyz,1))
	     *         )
         */
        for(GramBooleanQuery subtree : queryTree.subQuerySet){
        	GramBooleanQuery andSubtree = subtree;
        	if(subtree.operator != GramBooleanQuery.QueryOp.AND){
        		andSubtree = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        		andSubtree.subQuerySet.add(subtree);
        	}
        	// 1. Partitioning the grams into groups with same groupId
        	Map<Integer, List<GramBooleanQuery> > gramGroups = new HashMap<Integer,  List<GramBooleanQuery> >();
            for(GramBooleanQuery leaf : andSubtree.subQuerySet){
            	if(! gramGroups.containsKey(leaf.groupId)){
            		gramGroups.put(leaf.groupId, new LinkedList<GramBooleanQuery>());
            	}
            	gramGroups.get(leaf.groupId).add(leaf);
            }
            // 2. Creating a phrase query for each group and then AND on the phrases
            BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
            /* Generating:
 		     *            AND ( 
		     *                          Phrase((abc,0), (bcd,1)), 
		     *                          Phrase((mno,0), (nop,1))
		     *                ) 
             */
            for(Integer groupId: gramGroups.keySet()){
            	// Group ID -1 means the gram comes from a literal that was generated in 
            	// suffix or prefix, so we did not save the position index with the gram. We just 
            	// ignore these cases.
            	if(groupId == -1) continue;
            	//TODO: put an assert to make sure position indexes are always incremented one by one
            	PhraseQuery.Builder phraseQueryBuilder = new PhraseQuery.Builder();
            	/*
            	 * Generating (for example):
            	 * 					Phrase((abc,0), (bcd,1))
            	 */
            	
            	// Sorting the leaf list in position increasing order.
            	List<GramBooleanQuery> groupGrams = 
            			new ArrayList<GramBooleanQuery>(gramGroups.get(groupId).stream().
            					sorted((a,b) -> (a.positionIndex - b.positionIndex)).collect(Collectors.toList()));
            	for(GramBooleanQuery leaf : groupGrams){
        			phraseQueryBuilder.add(new Term(fieldName, leaf.leaf), leaf.positionIndex);
            	}
            	booleanQueryBuilder.add(phraseQueryBuilder.build(), Occur.MUST);
            }
            booleanQueryBuilderOr.add(booleanQueryBuilder.build(), Occur.SHOULD);
        }
        return booleanQueryBuilderOr.build();
    }

}
