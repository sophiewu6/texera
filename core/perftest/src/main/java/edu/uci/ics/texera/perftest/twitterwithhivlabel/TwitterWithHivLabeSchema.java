package edu.uci.ics.texera.perftest.twitterwithhivlabel;

import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;

public class TwitterWithHivLabeSchema {
    public static String TEXT = "text";
    public static Attribute TEXT_ATTRIBUTE = new Attribute(TEXT, AttributeType.TEXT);
    public static String LABEL = "label";
    public static Attribute LABEL_ATTRIBUTE = new Attribute(LABEL, AttributeType.STRING);
    
    public static Schema TWITTER_HIV_LABEL_SCHEMA = new Schema(
            TEXT_ATTRIBUTE, LABEL_ATTRIBUTE);
}
