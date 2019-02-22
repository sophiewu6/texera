package Operator.Base;

import Utility.PropertyNameConstants;

import Operator.Sink.TupleSinkPredicate;
import Operator.Source.File.FileSourcePredicate;
import Operator.common.KeywordPredicate;
import Operator.common.Compare.ComparablePredicate;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.UUID;

/**
 * PredicateBase is the base for all predicates which follow the
 *   Predicate Bean pattern.
 *
 * Every predicate needs to register itself in the JsonSubTypes annotation
 *   so that the Jackson Library can map each JSON string to the correct type
 *
 * @author Zuozhi Wang
 * @author yuranyan
 *
 */

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME, // logical user-defined type names are used (rather than Java class names)
        include = JsonTypeInfo.As.PROPERTY, // make the type info as a property in the JSON representation
        property = PropertyNameConstants.OPERATOR_TYPE // the name of the JSON property indicating the type
)
@JsonSubTypes({
//        @Type(value = DictionaryPredicate.class, name = "DictionaryMatcher"),
//        @Type(value = DictionarySourcePredicate.class, name = "DictionarySource"),
//        @Type(value = FuzzyTokenPredicate.class, name = "FuzzyTokenMatcher"),
//        @Type(value = FuzzyTokenSourcePredicate.class, name = "FuzzyTokenSource"),
        @Type(value = KeywordPredicate.class, name = "KeywordMatcher"),
//        @Type(value = KeywordSourcePredicate.class, name = "KeywordSource"),
//        @Type(value = RegexPredicate.class, name = "RegexMatcher"),
//        @Type(value = RegexSourcePredicate.class, name = "RegexSource"),

//        @Type(value = JoinDistancePredicate.class, name = "JoinDistance"),
//        @Type(value = SimilarityJoinPredicate.class, name = "SimilarityJoin"),
//
//        @Type(value = NlpEntityPredicate.class, name = "NlpEntity"),
//        @Type(value = NlpSentimentPredicate.class, name = "NlpSentiment"),
//        @Type(value = EmojiSentimentPredicate.class, name = "EmojiSentiment"),
//        @Type(value = NltkSentimentOperatorPredicate.class, name = "NltkSentiment"),
//
//        @Type(value = ProjectionPredicate.class, name = "Projection"),
//        @Type(value = RegexSplitPredicate.class, name = "RegexSplit"),
//        @Type(value = NlpSplitPredicate.class, name = "NlpSplit"),
//        @Type(value = SamplerPredicate.class, name = "Sampler"),

        // remove comparable matcher because of the json schema "any" issue
        // TODO: fix the problem and add Comparable matcher back later
        @Type(value = ComparablePredicate.class, name = "Comparison"),
//
//        @Type(value = AsterixSourcePredicate.class, name = "AsterixSource"),
//        @Type(value = TwitterConverterPredicate.class, name = "TwitterConverter"),
//
//        @Type(value = ScanSourcePredicate.class, name = "ScanSource"),
        @Type(value = FileSourcePredicate.class, name = "FileSource"),
        @Type(value = TupleSinkPredicate.class, name = "ViewResults"),
//        @Type(value = MysqlSinkPredicate.class, name = "MysqlSink"),
//        @Type(value = TwitterFeedSourcePredicate.class, name = "TwitterFeed"),
//
//        @Type(value = WordCountIndexSourcePredicate.class, name = "WordCountIndexSource"),
//        @Type(value = WordCountOperatorPredicate.class, name = "WordCount"),
//        @Type(value = AggregatorPredicate.class, name = "Aggregation"),
})

public abstract class PredicateBase implements IPredicate
{
    // default id is random uuid (internal code doesn't care about id)
    private String id = UUID.randomUUID().toString();

    @JsonProperty(PropertyNameConstants.OPERATOR_ID)
    public void setID(String id) {
        this.id = id;
    }

    @JsonProperty(PropertyNameConstants.OPERATOR_ID)
    public String getID() {
        return id;
    }

    @JsonIgnore
    public abstract OperatorBase newOperator();

    @Override
    public int hashCode() {
        // TODO: evaluate performance impact using reflection
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object that) {
        // TODO: evaluate performance impact using reflection
        return EqualsBuilder.reflectionEquals(this, that);
    }

    @Override
    public String toString() {
        // TODO: evaluate performance impact using reflection
        return ToStringBuilder.reflectionToString(this);
    }
}
