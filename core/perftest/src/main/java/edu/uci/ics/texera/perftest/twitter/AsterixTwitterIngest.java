
package edu.uci.ics.texera.perftest.twitter;

import edu.uci.ics.texera.dataflow.source.asterix.AsterixSource;
import edu.uci.ics.texera.dataflow.source.asterix.AsterixSourcePredicate;

/**
 * A helper class to query data from the main Asterix database with more than 1 billion of tweets based on a set of keywords,
 *  and then writes the data to a table.
 * 
 * @author Zuozhi Wang
 */
public class AsterixTwitterIngest {
    
    public static void main(String[] args) {
        ingestKeywords("nsf_tweets",
                "nsf",
                null, null, null);
    }

    public static void ingestKeywords(String tableName, String keywords, String startDate, String endDate, Integer limit) {
        
        AsterixSourcePredicate asterixSourcePredicate = new AsterixSourcePredicate(
                "twitterJson",
                "americium.ics.uci.edu",
                19002,
                "twitter",
                "ds_tweet",
                "text",
                keywords,
                startDate,
                endDate,
                limit);

        AsterixSource asterixSource = asterixSourcePredicate.newOperator();

        TwitterSample.createTwitterTable(tableName, asterixSource);
    }

}
