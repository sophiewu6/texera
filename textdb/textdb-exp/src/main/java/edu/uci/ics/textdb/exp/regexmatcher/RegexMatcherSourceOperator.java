package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
            		createLucenePhraseQuery(this.predicate, "abstract"));
            
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
    
    public static Query createLuceneQuery(RegexSourcePredicate predicate) throws StorageException {
        Query luceneQuery;
        String queryString;
        
        // Try to apply translator. If it fails, use scan query.
        try {
            queryString = RegexToGramQueryTranslator.translate(predicate.getRegex()).getLuceneQueryString();
        } catch (com.google.re2j.PatternSyntaxException e) {
            queryString = DataConstants.SCAN_QUERY;
        }

        // Try to parse the query string. It if fails, raise an exception.
        try {
            luceneQuery = new MultiFieldQueryParser(
                    predicate.getAttributeNames().stream().toArray(String[]::new), 
                    RelationManager.getRelationManager().getTableAnalyzer(predicate.getTableName()))
                    .parse(queryString);
        } catch (ParseException e) {
            throw new StorageException (e);
        }
        return luceneQuery;
    }
    
    
    public static Query createLucenePhraseQuery(RegexPredicate predicate, String fieldName) throws DataFlowException {
        GramBooleanQuery queryTree;
        // Try to apply translator. If it fails, use scan query.
        try {
            queryTree = RegexToGramQueryTranslator.translate(predicate.getRegex());
        } catch (com.google.re2j.PatternSyntaxException e) {
            queryTree = null;
        }
//        System.out.println(queryTree.printQueryTree());
        // If top operator is AND, create a flat AND query
        if(queryTree.operator == GramBooleanQuery.QueryOp.AND){
        	// 1. Partitioning the grams into groups with same groupId
        	Map<Integer, List<GramBooleanQuery> > gramGroups = new HashMap<Integer,  List<GramBooleanQuery> >();
            for(GramBooleanQuery leaf : queryTree.subQuerySet){
            	if(! gramGroups.containsKey(leaf.groupId)){
            		gramGroups.put(leaf.groupId, new LinkedList<GramBooleanQuery>());
            	}
            	gramGroups.get(leaf.groupId).add(leaf);
            }
            // 2. Creating a phrase query for each group and then AND on the phrases
            BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
            for(Integer groupId: gramGroups.keySet()){
            	if(groupId == -1) continue;
            	PhraseQuery.Builder phraseQueryBuilder = new PhraseQuery.Builder();
            	for(int i=0; i < 100; i++){
	            	for(GramBooleanQuery leaf: gramGroups.get(groupId)){
	                	if(leaf.positionIndex != i){
	                		continue;
	                	}
	                    phraseQueryBuilder.add(new Term(fieldName, leaf.leaf), leaf.positionIndex);
	            	}
            	}
            	booleanQueryBuilder.add(phraseQueryBuilder.build(), Occur.MUST);
            }
//            if(gramGroups.containsKey(-1)){
//        		BooleanQuery.Builder affixBooleanQueryBuilder = new BooleanQuery.Builder();
//        		for(GramBooleanQuery leaf: gramGroups.get(-1)){
//        			affixBooleanQueryBuilder.add(new TermQuery(new Term(fieldName, leaf.leaf)), 
//        					Occur.MUST);
//        		}
//        		booleanQueryBuilder.add(affixBooleanQueryBuilder.build(), Occur.MUST);
//        	}
            
//                return phraseQueryBuilder.build();   
//            System.out.println(booleanQueryBuilder.build().toString());
            return booleanQueryBuilder.build();
        }else if (queryTree.operator == GramBooleanQuery.QueryOp.OR){
            BooleanQuery.Builder booleanQueryBuilderOr = new BooleanQuery.Builder();
            for(GramBooleanQuery andSubtree : queryTree.subQuerySet){
            	Map<Integer, List<GramBooleanQuery> > gramGroups = new HashMap<Integer,  List<GramBooleanQuery> >();
                for(GramBooleanQuery leaf : andSubtree.subQuerySet){
                	if(! gramGroups.containsKey(leaf.groupId)){
                		gramGroups.put(leaf.groupId, new LinkedList<GramBooleanQuery>());
                	}
                	gramGroups.get(leaf.groupId).add(leaf);
                }
                BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
                for(Integer groupId: gramGroups.keySet()){
                	if(groupId == -1) continue;
                	PhraseQuery.Builder phraseQueryBuilder = new PhraseQuery.Builder();
                	for(int i=0; i < 100; i++){
                		for(GramBooleanQuery leaf: gramGroups.get(groupId)){
                			if(leaf.positionIndex != i){
                				continue;
                			}
                			phraseQueryBuilder.add(new Term(fieldName, leaf.leaf), leaf.positionIndex);
                		}
                	}
                	booleanQueryBuilder.add(phraseQueryBuilder.build(), Occur.MUST);
                }
//                if(gramGroups.containsKey(-1)){
//            		BooleanQuery.Builder affixBooleanQueryBuilder = new BooleanQuery.Builder();
//            		for(GramBooleanQuery leaf: gramGroups.get(-1)){
//            			affixBooleanQueryBuilder.add(new TermQuery(new Term(fieldName, leaf.leaf)), 
//            					Occur.MUST);
//            		}
//            		booleanQueryBuilder.add(affixBooleanQueryBuilder.build(), Occur.MUST);
//            	}
                
                booleanQueryBuilderOr.add(booleanQueryBuilder.build(), Occur.SHOULD);
            }
//            System.out.println(booleanQueryBuilderOr.build().toString());
            return booleanQueryBuilderOr.build();
        }
        
        return null;
    }

}
