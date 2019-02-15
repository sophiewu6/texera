package edu.uci.ics.texera.dataflow.dictionarymatcher;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.annotation.AdvancedOption;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.texera.dataflow.resource.dictionary.SQLiteDictionaryManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

public class DictionaryPredicate extends PredicateBase {

    private final Dictionary dictionary;
    private final List<String> dictionaryEntries;
    private final String dictionaryName;
    private final List<String> attributeNames;
    private final String luceneAnalyzerStr;
    private final KeywordMatchingType keywordMatchingType;
    private final String spanListName;

    /**
     * DictionaryPredicate is used to create a DictionaryMatcher.
     * 
     * @param dictionary, the dictionary to be used
     * @param attributeNames, the names of the attributes to match the dictionary
     * @param luceneAnalyzerStr, the lucene analyzer to tokenize the dictionary entries
     * @param keywordMatchingType, the keyword matching type ({@code KeywordMatchingType}
     * @param spanListName, optional, the name of the attribute where the results (a list of spans) will be in, 
     *          default value is the id of the predicate
     */
    @JsonCreator
    public DictionaryPredicate(
            
            @JsonProperty(value = PropertyNameConstants.DICTIONARY_ENTRIES)
            List<String> dictionaryEntries,
            
            @JsonProperty(value = PropertyNameConstants.DICTIONARY_NAME)
            String dictionaryName,
            
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> attributeNames, 
            
            @AdvancedOption
            @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING, required = true,
                    defaultValue = LuceneAnalyzerConstants.STANDARD_ANALYZER)
            String luceneAnalyzerStr,
            
            @AdvancedOption
            @JsonProperty(value = PropertyNameConstants.KEYWORD_MATCHING_TYPE, required = true,
                    defaultValue = KeywordMatchingType.KeywordMatchingTypeName.PHRASE)
            KeywordMatchingType keywordMatchingType,
            
            @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME, required = false)
            String spanListName) {
    	if (dictionaryEntries == null && dictionaryName == null) {
    		throw new TexeraException(PropertyNameConstants.EMPTY_DICTIONARY_OR_NAME_EXCEPTION);
    	} else if (dictionaryEntries != null && dictionaryEntries.size() > 0 && 
    			dictionaryName != null && dictionaryName.trim().length() > 0) {
    		throw new TexeraException(PropertyNameConstants.XOR_DICTIONARY_AND_NAME_EXCEPTION);
    	}
    	
    	if (dictionaryEntries != null && dictionaryEntries.size() > 0) {
    		this.dictionaryEntries = dictionaryEntries;
    	} else {
    		try {
				List<String> storedDictionaryEntries = SQLiteDictionaryManager.getInstance().getDictionary(dictionaryName);
				if (storedDictionaryEntries == null || storedDictionaryEntries.size() == 0) {
					throw new TexeraException(PropertyNameConstants.NON_EXISTING_DICTIONARY);
				}
				this.dictionaryEntries = storedDictionaryEntries;
			} catch (SQLException e) {
				throw new TexeraException(e.getMessage());
			}
    	}
        this.dictionary = new Dictionary(this.dictionaryEntries);
        this.dictionaryName = dictionaryName;
        this.luceneAnalyzerStr = luceneAnalyzerStr;
        this.attributeNames = attributeNames;
        this.keywordMatchingType = keywordMatchingType;
        
        if (spanListName == null || spanListName.trim().isEmpty()) {
            this.spanListName = null;
        } else {
            this.spanListName = spanListName.trim();
        }
    }
    
//    @JsonProperty(value = PropertyNameConstants.DICTIONARY, required = true)
    @JsonIgnore
    public Dictionary getDictionary() {    	
        return dictionary;
    }
    
    @JsonProperty(value = PropertyNameConstants.DICTIONARY_ENTRIES)
    public List<String> getDictionaryEntries() {
        return dictionaryEntries;
    }
    
    @JsonProperty(value = PropertyNameConstants.DICTIONARY_NAME)
    public String getDictionaryName() {
    	return dictionaryName;
    }
    
    @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES)
    public List<String> getAttributeNames() {
        return new ArrayList<>(attributeNames);
    }

    @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING)
    public String getAnalyzerString() {
        return luceneAnalyzerStr;
    }
    
    @JsonProperty(value = PropertyNameConstants.KEYWORD_MATCHING_TYPE)
    public KeywordMatchingType getKeywordMatchingType() {
        return keywordMatchingType;
    }
    
    @JsonProperty(value = PropertyNameConstants.SPAN_LIST_NAME)
    public String getSpanListName() {
        return spanListName;
    }
    
    @Override
    public IOperator newOperator() {
        return new DictionaryMatcher(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Dictionary Search")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Search the documents using a dictionary (multiple keywords)")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SEARCH_GROUP)
            .build();
    }
    
    public static void main(String[] args) throws SQLException {
    	String dictionaryName = " sample ";
    	System.out.println(SQLiteDictionaryManager.getInstance().getDictionary(dictionaryName));
    }
    
}
