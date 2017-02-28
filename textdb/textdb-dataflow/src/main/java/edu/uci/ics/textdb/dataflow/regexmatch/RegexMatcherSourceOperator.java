package edu.uci.ics.textdb.dataflow.regexmatch;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.storage.reader.DataReader;
import edu.uci.ics.textdb.storage.relation.RelationManager;

public class RegexMatcherSourceOperator extends AbstractSingleInputOperator implements ISourceOperator {
    
    private RegexPredicate predicate;
    private String tableName;

    private DataReader dataReader;
    private RegexMatcher regexMatcher;
    
    public RegexMatcherSourceOperator(RegexPredicate predicate, String tableName) throws StorageException, DataFlowException {
        this.predicate = predicate;
        this.tableName = tableName;
        
        this.dataReader = RelationManager.getRelationManager().getTuples(this.tableName, 
                createLuceneQuery(this.predicate));
        
        regexMatcher = new RegexMatcher(this.predicate);
        regexMatcher.setInputOperator(dataReader);
        
        this.inputOperator = this.regexMatcher;
    }

    @Override
    protected void setUp() throws TextDBException {
        this.outputSchema = regexMatcher.getOutputSchema();        
    }

    @Override
    protected ITuple computeNextMatchingTuple() throws TextDBException {
        return this.regexMatcher.getNextTuple();
    }

    @Override
    public ITuple processOneInputTuple(ITuple inputTuple) throws TextDBException {
        return this.regexMatcher.processOneInputTuple(inputTuple);
    }

    @Override
    protected void cleanUp() throws TextDBException {
    }
    
    public static Query createLuceneQuery(RegexPredicate predicate) throws DataFlowException {
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
                    predicate.getAttributeNames().stream().toArray(String[]::new), predicate.getLuceneAnalyzer())
                    .parse(queryString);
        } catch (ParseException e) {
            throw new DataFlowException(e);
        }
        
        return luceneQuery;
    }
    
    public static Query createLucenePhraseQuery(RegexPredicate predicate, String fieldName) throws DataFlowException {
    	Query luceneQuery;
        String queryString;
        GramBooleanQuery queryTree;
        // Try to apply translator. If it fails, use scan query.
        try {
            queryTree = RegexToGramQueryTranslator.translate(predicate.getRegex());
        } catch (com.google.re2j.PatternSyntaxException e) {
            queryTree = null;
        }

        // If top operator is AND, create a flat AND query
        if(queryTree.operator == GramBooleanQuery.QueryOp.AND){
            PhraseQuery.Builder phraseQueryBuilder = new PhraseQuery.Builder();
            for(GramBooleanQuery leaf : queryTree.subQuerySet){
                phraseQueryBuilder.add(new Term(fieldName, leaf.leaf), leaf.gramOffset);
            }
            return phraseQueryBuilder.build();
        }else if (queryTree.operator == GramBooleanQuery.QueryOp.OR){
            BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
            for(GramBooleanQuery orChild : queryTree.subQuerySet){
                PhraseQuery.Builder phraseQueryBuilder = new PhraseQuery.Builder();
                for(GramBooleanQuery leaf: orChild.subQuerySet){
                    phraseQueryBuilder.add(new Term(fieldName, leaf.leaf), leaf.gramOffset);
                }
                booleanQueryBuilder.add(phraseQueryBuilder.build(), Occur.SHOULD);
            }
            return booleanQueryBuilder.build();
        }
        
        return null;
    }

}
