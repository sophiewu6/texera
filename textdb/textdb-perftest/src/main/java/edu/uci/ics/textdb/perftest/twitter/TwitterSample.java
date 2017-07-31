package edu.uci.ics.textdb.perftest.twitter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

public class TwitterSample {
    
    public static String twitterFilePath = PerfTestUtils.getResourcePath("/sample-data-files/twitter");
    public static String indexPath = PerfTestUtils.getResourcePath("/index/standard/twitter");
    public static String twitterClimateTable = "twitter";
    public static List<String> content;

    public static void main(String[] args) throws Exception {
        writeTwitterIndex();
    }
    
    public static void writeTwitterIndex() throws Exception {
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(twitterClimateTable);
        relationManager.createTable(twitterClimateTable, indexPath, TwitterSchemaShort.TWITTER_SCHEMA,
                LuceneAnalyzerConstants.standardAnalyzerString());
        
        DataWriter dataWriter = relationManager.getTableDataWriter(twitterClimateTable);
        dataWriter.open();
        
        int counter = 0;
        File sourceFileFolder = new File(twitterFilePath);
        for (File htmlFile : sourceFileFolder.listFiles()) {
            Scanner scanner = new Scanner(htmlFile);
            while (scanner.hasNext()) {
                String record = scanner.nextLine();
                JsonNode tweet = new ObjectMapper().readValue(record, JsonNode.class);
                content = new ArrayList<>();
                String text = tweet.get("text").asText();

                String id = tweet.get("id").toString();
                String tweetLink = "https://twitter.com/statuses/" + id;

                JsonNode userNode = tweet.get("user");
                String userScreenName = userNode.get("screen_name").asText();
                String userLink = "https://twitter.com/" + userScreenName;
                String userName = userNode.get("name").asText();
                String userDescription = userNode.get("description").asText();

                Integer userFollowersCount = userNode.get("followers_count").asInt();
                Integer userFriendsCount = userNode.get("friends_count").asInt();
                String userFollowersFriendsCount = userFriendsCount.toString() + " " + userFollowersCount.toString();
                JsonNode geoTagNode = tweet.get("geo_tag");
                String entireGeo;
                if(geoTagNode != null) {
                    String state = geoTagNode.get("stateName").asText();
                    String county = geoTagNode.get("countyName").asText();
                    String city = geoTagNode.get("cityName").asText();

                    entireGeo = city + ", " + county + ", " + state;
                }else{
                    entireGeo = "n/a";
                }
                String createAt = tweet.get("created_at").asText();
                content.add(createAt);
                content.add(userScreenName);
                content.add(userName);
                content.add(userLink);
                content.add(userDescription);
                content.add(entireGeo);
                content.add(tweetLink);
                content.add(text);
                String result = String.join(", ", content);
                Tuple tuple = new Tuple(TwitterSchemaShort.TWITTER_SCHEMA, new TextField(result));

                if (tuple != null) {
                    dataWriter.insertTuple(tuple);
                    counter++;
                }
            }
            scanner.close();
        }

        dataWriter.close();
        System.out.println("write twitter data finished");
        System.out.println(counter + " tweets written");
    }

}
