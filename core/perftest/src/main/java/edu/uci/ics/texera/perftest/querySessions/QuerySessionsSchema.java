package edu.uci.ics.texera.perftest.querySessions;

import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;

public class QuerySessionsSchema {
    public static String TEXT = "Session";
    public static Attribute TEXT_ATTRIBUTE = new Attribute(TEXT, AttributeType.TEXT);
    
    public static Schema QUERY_SESSIONS_SCHEMA = new Schema(TEXT_ATTRIBUTE);
}
