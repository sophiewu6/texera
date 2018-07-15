
package edu.uci.ics.texera.perftest.twitter;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import edu.uci.ics.texera.api.field.DateTimeField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSinkPredicate;
import edu.uci.ics.texera.dataflow.source.asterix.AsterixSource;
import edu.uci.ics.texera.dataflow.source.asterix.AsterixSourcePredicate;
import edu.uci.ics.texera.dataflow.twitter.TwitterConverter;
import edu.uci.ics.texera.dataflow.twitter.TwitterConverterConstants;
import edu.uci.ics.texera.dataflow.twitter.TwitterConverterPredicate;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

/**
 * A helper class to query data from the main Asterix database with more than 1 billion of tweets based on a set of keywords,
 *  and then writes the data to a table.
 * 
 * @author Zuozhi Wang
 */
public class AsterixTwitterIngest {
    
    public static void main(String[] args) {
        ingestKeywords("16_twitter_immigration_policy_study_2", 
                "immigration daca muslimban nobannowall buildthewall immigrants heretostay refugeeswelcome travelban refugees defenddaca nomuslimban immigrant deportation",
                null, null, null);
    }

    public static void ingestKeywords(String tableName, String keywords, String startDate, String endDate, Integer limit) {
        
        AsterixSourcePredicate asterixSourcePredicate = new AsterixSourcePredicate(
                "twitterJson",
                "actinium.ics.uci.edu",
                19002,
                "twitter",
                "ds_tweet",
                "text",
                keywords,
                startDate,
                endDate,
                limit);

        AsterixSource asterixSource = asterixSourcePredicate.newOperator();

        
        TwitterConverter twitterConverter = new TwitterConverterPredicate("twitterJson").newOperator();

        TupleSink tupleSink = new TupleSinkPredicate(null, null).newOperator();
        
        twitterConverter.setInputOperator(asterixSource);
        tupleSink.setInputOperator(twitterConverter);

        // open the workflow plan and get the output schema
        tupleSink.open();
        
        // create the table with TupleSink's output schema
        RelationManager relationManager = RelationManager.getInstance();
        
        if (relationManager.checkTableExistence(tableName)) {
            relationManager.deleteTable(tableName);
        }
        relationManager.createTable(tableName, Utils.getDefaultIndexDirectory().resolve(tableName),
                tupleSink.getOutputSchema(), LuceneAnalyzerConstants.standardAnalyzerString());
        DataWriter dataWriter = relationManager.getTableDataWriter(tableName);
        dataWriter.open();
        
        Tuple tuple;
        
        int counter = 0;
        
        String elvanIntervalStartString = "2016-11-08T00:00:00.000Z";
        ZonedDateTime elvanIntervalStart = ZonedDateTime.parse(elvanIntervalStartString, DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault()));

        String elvanIntervalEndString = "2017-11-08T00:00:00.000Z";
        ZonedDateTime elvanIntervalEnd = ZonedDateTime.parse(elvanIntervalEndString, DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault()));
        
        int totalCounter = 0;
        int totalRetweetCounter = 0;
        
        int elvanIntervalCount = 0;
        int elvanIntervalRetweetCount = 0;
        
        while ((tuple = tupleSink.getNextTuple()) != null) {

            DateTimeField creationTime = tuple.getField(TwitterConverterConstants.CREATE_AT);
            TextField text = tuple.getField(TwitterConverterConstants.TEXT);
            
            totalCounter++;
            
            if (creationTime.getValue().isAfter(elvanIntervalStart.toLocalDateTime()) &&
                    creationTime.getValue().isBefore(elvanIntervalEnd.toLocalDateTime())) {
                elvanIntervalCount++;
                if (text.getValue().contains("RT @")) {
                    elvanIntervalRetweetCount++;
                }
            }
            
            if (text.getValue().contains("RT @")) {
                totalRetweetCounter++;
                System.out.println(text.getValue());
            } else {
                dataWriter.insertTuple(tuple);
                counter++;
            }
        }

        dataWriter.close();
        tupleSink.close();
        
        System.out.println("total tweets count: " + totalCounter);
        System.out.println("total re-tweets count: " + totalRetweetCounter);
        System.out.println("total elvan interval tweets count: " + elvanIntervalCount);
        System.out.println("total elvan interval re-tweet count: " + elvanIntervalRetweetCount);
        System.out.println("inserted tweets count: " + counter);


    }

}
