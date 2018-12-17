package edu.uci.ics.texera.perftest.bbcnews;

import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;

public class BBCnewsSchema {
    
    public static String TEXT = "text";
    public static Attribute TEXT_ATTRIBUTE = new Attribute(TEXT, AttributeType.TEXT);
    public static Schema BBCNEWS_SCHEMA = new Schema(TEXT_ATTRIBUTE);
}
