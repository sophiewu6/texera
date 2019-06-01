package edu.uci.ics.texera.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ProcessTweet {

    public static void main(String[] args) throws JsonProcessingException, IOException {
        writeTweet(args);
    }

    public static void readTweet() throws IOException {
//		ObjectMapper mapper = new ObjectMapper();
//        BufferedReader reader = new BufferedReader(new FileReader("rawtweets.txt"));
//        String line = "";
//        while ((line = reader.readLine()) != null) {
//        	System.out.println(line);
//        }
//
//        reader.close();
    }

    public static void writeTweet(String[] args) throws JsonProcessingException, IOException {
        Path srcPath = Paths.get(args[0]);
        Path destPath = Paths.get(args[1]);

//        Path srcPath = Paths.get("/Users/zuozhiw/Desktop/texera_extraction/tweet_1M.json");
//        Path destPath = Paths.get("/Users/zuozhiw/Desktop/texera_extraction/tweet_1M_cleaned.tbl");

        BufferedReader reader = Files.newBufferedReader(srcPath);
        BufferedWriter writer = Files.newBufferedWriter(destPath);

        ObjectMapper objectMapper = new ObjectMapper();

        String line;
        while ((line=reader.readLine()) != null) {
            line = line.trim();
            JsonNode dsTweet = null;
            if (line.startsWith(", {")) {
                dsTweet = objectMapper.readTree(line.substring(1).trim());
            } else {
                if (line.startsWith("\"results\": [ {")) {
                    dsTweet = objectMapper.readTree(line.substring("\"results\": [".length()).trim());
                }
            }

            if (dsTweet != null) {
                JsonNode tweet = dsTweet.get("ds_tweet");
                String rawTweet = tweet.get("text").textValue();
                String parsedTweet = rawTweet.replace('\n', ' ').replace('|', ' ');
                String newString = parsedTweet + '\n';

                ObjectNode newNode = (ObjectNode) tweet;
                newNode.put("text", newString);

                StringBuilder sb = new StringBuilder();
                for (JsonNode column: newNode) {
                    if (column.isObject() || column.isArray()) {
                        continue;
                    }
                    String columnStr = column.toString();
                    if (columnStr.startsWith("\"")) {
                        columnStr = columnStr.substring(1);
                    }
                    if (columnStr.endsWith("\"")) {
                        columnStr = columnStr.substring(0, columnStr.length() - 1);
                    }
                    sb.append(columnStr).append("|");
                }

                String parsedValue = sb.substring(0, sb.length() - 1) + "\n";
                writer.write(parsedValue);
            }
        }

        reader.close();
        writer.close();
        // create_at | id | text | in_reply_to_status | in_reply_to_user | favorite_count | lang | is_retweet
    }

}
