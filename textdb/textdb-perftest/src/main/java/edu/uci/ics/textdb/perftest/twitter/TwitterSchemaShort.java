package edu.uci.ics.textdb.perftest.twitter;

import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;

/**
 * Created by Chang on 7/25/17.
 */
public class TwitterSchemaShort {
    public static String TEXT = "content";
    public static Attribute TEXT_ATTRIBUTE = new Attribute(TEXT, AttributeType.TEXT);

    public static Schema TWITTER_SCHEMA = new Schema(TEXT_ATTRIBUTE);
}
