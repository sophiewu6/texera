package edu.uci.ics.texera.perftest.sample;

/**
 * Created by Chang on 6/26/17.
 */

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.engine.Engine;
import edu.uci.ics.texera.api.engine.Plan;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.dictionarymatcher.*;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.texera.dataflow.nlp.entity.NlpEntityOperator;
import edu.uci.ics.texera.dataflow.nlp.entity.NlpEntityPredicate;
import edu.uci.ics.texera.dataflow.nlp.entity.NlpEntityType;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexMatcher;
import edu.uci.ics.texera.dataflow.regexmatcher.RegexPredicate;
import edu.uci.ics.texera.dataflow.sink.FileSink;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.dataflow.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.texera.dataflow.source.scan.ScanSourcePredicate;
import edu.uci.ics.texera.dataflow.utils.DataflowUtils;
import edu.uci.ics.texera.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.texera.perftest.promed.PromedSchema;
import edu.uci.ics.texera.perftest.utils.PerfTestUtils;
import edu.uci.ics.texera.perftest.wikipedia.WikipediaIndexWriter;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;
import jregex.Matcher;
import jregex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import sun.misc.Perf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URISyntaxException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Dictionary;


public class WikipediaExtraction {

    public static final String WIKIPEDIA_SAMPLE_TABLE = "wikipedia";
    public static int number = 0;

    public static Path wikipediaFilesDirectory = PerfTestUtils.getResourcePath("/sample-data-files/wikipedia");
    public static Path wikipediaIndexDirectory = PerfTestUtils.getResourcePath("/index/standard/wikipedia");
    public static Path sampleDataFilesDirectory = PerfTestUtils.getResourcePath("/sample-data-files");
//
//    static {
//        try {
//            // Finding the absolute path to the sample data files directory and index directory
//
//            // Checking if the resource is in a jar
//            String referencePath = SampleExtraction.class.getResource("").toURI().toString();
//            if(referencePath.substring(0, 3).equals("jar")) {
//                medlineFilesDirectory = "/texera-perftest/src/main/resources/sample-data-files/medline/";
//                medlineIndexDirectory = "/texera-perftest/src/main/resources/index/standard/medline/";
//                sampleDataFilesDirectory = "/texera-perftest/src/main/resources/sample-data-files/";
//            }
//            else {
//                medlineFilesDirectory = Paths.get(SampleExtraction.class.getResource("/sample-data-files/medline")
//                        .toURI())
//                        .toString();
//                medlineIndexDirectory = Paths.get(SampleExtraction.class.getResource("/index/standard")
//                        .toURI())
//                        .toString() + "/medline";
//                System.out.println(medlineIndexDirectory);
//                sampleDataFilesDirectory = Paths.get(SampleExtraction.class.getResource("/sample-data-files")
//                        .toURI())
//                        .toString();
//            }
//        }
//        catch(URISyntaxException | FileSystemNotFoundException e) {
//            e.printStackTrace();
//        }
//    }
    public static void main(String[] args) throws Exception {
        // write the index of data files
        // index only needs to be written once, after the first run, this function can be commented out
//       writeSampleIndex();
       System.out.println(number);
       String regex;
//       regex = "(#?\\d+\\s+\\w+\\s+(st|dr|rd|blvd|Street)\\.?,\\s*)(San Francisco\\s*,? CA,\\s*\\d{5})"; // => 29.7 -> 26.9
//       regex = "((http|ftp)://www)((\\.\\w+)+)(\\.gov)";
       regex = "(http://(\\w+\\.)+edu)(:\\d{1,5})";
//       regex = "(\\w+\\s+([a-z]\\.\\s+)?)(Washington)";
     SCAN_REGEX_SINK("("+regex+")"); // 26s
     SCAN_REGEX_SINK(regex); // 27s
       
       regex = "(\\{\\{Cite book\\| last = )([A-Z][A-Za-z0-9\\-\\s]+)(\\| first = )([A-Z][A-Za-z0-9\\-\\s]+)(\\| "
       		+ "title = )([A-Z][A-Za-z0-9\\-\\s]+)(\\| publisher = )([A-Z][A-Za-z0-9\\-\\s]+)(\\| "
       		+ "location = )([A-Z][A-Za-z0-9\\-\\s]+)(\\| year = \\d{4} )(\\| isbn = \\d-\\d{1,10}-\\d{1,10}-\\d )(\\|page=\\d+)(\\}\\})";
//       SCAN_REGEX_SINK("("+regex+")"); // 26s
//       SCAN_REGEX_SINK(regex); // 27s
//       SCAN_REGEX_SINK("(\\{\\{Cite book\\| last = )");
//       SCAN_REGEX_SINK("(\\| first = )");
//       SCAN_REGEX_SINK("(\\| title = )");
//       SCAN_REGEX_SINK("(\\| publisher = )");
//       SCAN_REGEX_SINK("(\\| location = )");
//       SCAN_REGEX_SINK("(\\| year = \\d{4} )");
//       
//       regex = "(\\{\\{Cite book\\|)([^\\{\\}]*)(\\| isbn = \\d-\\d{1,10}-\\d{1,10}-\\d )(\\|page=\\d+)([^\\{\\}]*)(\\}\\})";
//          SCAN_REGEX_SINK("("+regex+")"); // 26s
////          SCAN_REGEX_SINK(regex); // 
//       SCAN_REGEX_SINK("(\\{\\{Cite book\\|)");
//       SCAN_REGEX_SINK("(\\| isbn = \\d-\\d{1,10}-\\d{1,10}-\\d )");
//       SCAN_REGEX_SINK("(\\}\\})");
//
//       regex = "\\{\\{[^\\{\\}]+\\}\\}";
//       SCAN_REGEX_SINK("("+regex+")");
//       SCAN_REGEX_SINK(regex);
       
       regex = "<ref (name= ([A-Za-z0-9]|\\.|\\-|\\s)+)?(>\\{\\{cite journal )(\\| title = )([A-Za-z0-9]|\\.|\\-|\\s)+"
         		+ "(\\| author = )(\\w|\\.|\\-|,|\\s)*(Patrick J\\. Keeling|Charles Short|Laura Wegener Parfrey|"
         		+ "Erika Barbero|Elyse Lasser|Micah Dunthorn|"
         		+ "Debashish Bhattacharya|David J Patterson|Burki F|Shalchian-Tabrizi K|Minge M)"
         		+ "(\\w|\\.|\\-|,|\\s)*\\| [^\\{\\}]*(\\}\\}<\\/ref>)";
//       SCAN_REGEX_SINK("("+regex+")"); // 26s
//       SCAN_REGEX_SINK(regex); // 28s
//       SCAN_REGEX_SINK("(<ref )");
//       SCAN_REGEX_SINK("(>\\{\\{cite journal )");
//       SCAN_REGEX_SINK("(\\| title = )");
//       SCAN_REGEX_SINK("(\\| author = )");
//       SCAN_REGEX_SINK("(Patrick J\\. Keeling|Charles Short|Laura Wegener Parfrey|"
//         		+ "Erika Barbero|Elyse Lasser|Micah Dunthorn|"
//         		+ "Debashish Bhattacharya|David J Patterson|Burki F|Shalchian-Tabrizi K|Minge M)");
//       SCAN_REGEX_SINK("(\\}\\}<\\/ref>)");
       
       regex = "<ref (name= [A-Za-z0-9\\.\\-\\s]+)?(>\\{\\{cite journal )\\|[^\\{\\}]*"
         		+ "(\\| url = )(http:\\/\\/www.[\\w-]+.(org|com|edu|gov|net)(\\/[\\w-\\.])*) \\|[^\\{\\}]*(\\}\\}<\\/ref>)";
//       SCAN_REGEX_SINK("("+regex+")"); // 25s // 169s
//       SCAN_REGEX_SINK(regex); // 24s // 181s
//       SCAN_REGEX_SINK("(<ref )");
//       SCAN_REGEX_SINK("(>\\{\\{cite journal )");
//       SCAN_REGEX_SINK("(\\| url = )");
//       SCAN_REGEX_SINK("(\\}\\}<\\/ref>)");
       
//       regex = "(\\|)"; // Real selectivity is about 0.6
//       SCAN_REGEX_SINK(regex);
    		   
       regex = "(Patrick J\\. Keeling|Charles Short|Laura Wegener Parfrey|"
        		+ "Erika Barbero|Elyse Lasser|Micah Dunthorn|"
        		+ "Debashish Bhattacharya|David J Patterson|Burki F|Shalchian-Tabrizi K|Minge M)"
        		+ "(\\w|\\.|\\-|,|\\s)*\\| [^\\{\\}]*(\\}\\}<\\/ref>)";
//      SCAN_REGEX_SINK("("+regex+")"); // 28s // ####8####2050.168 // 100K ####3####284.331
//      SCAN_REGEX_SINK(regex); // 32s // 100K ####3####29.967
//      SCAN_REGEX_SINK("((Patrick J\\. Keeling|Charles Short|Laura Wegener Parfrey|"
//        		+ "Erika Barbero|Elyse Lasser|Micah Dunthorn|"
//        		+ "Debashish Bhattacharya|David J Patterson|Burki F|Shalchian-Tabrizi K|Minge M))"); // ####86####2052.144
//      SCAN_REGEX_SINK("((Patrick J\\. Keeling|Charles Short|Laura Wegener Parfrey|"
//        		+ "Erika Barbero|Elyse Lasser|Micah Dunthorn|"
//        		+ "Debashish Bhattacharya|David J Patterson|Burki F|Shalchian-Tabrizi K|Minge M)"
//        		+ "(\\w|\\.|\\-|,|\\s)*)"); // ####86####2050.982
//      SCAN_REGEX_SINK("((Patrick J\\. Keeling|Charles Short|Laura Wegener Parfrey|"
//	      		+ "Erika Barbero|Elyse Lasser|Micah Dunthorn|"
//	      		+ "Debashish Bhattacharya|David J Patterson|Burki F|Shalchian-Tabrizi K|Minge M)"
//	      		+ "(\\w|\\.|\\-|,|\\s)*\\| )");  // ####10####2051.441
//      SCAN_REGEX_SINK("((Patrick J\\. Keeling|Charles Short|Laura Wegener Parfrey|"
//        		+ "Erika Barbero|Elyse Lasser|Micah Dunthorn|"
//        		+ "Debashish Bhattacharya|David J Patterson|Burki F|Shalchian-Tabrizi K|Minge M)"
//        		+ "(\\w|\\.|\\-|,|\\s)*\\| [^\\{\\}]*)");  // ####10####2053.379
//      SCAN_REGEX_SINK("((Patrick J\\. Keeling|Charles Short|Laura Wegener Parfrey|"
//	      		+ "Erika Barbero|Elyse Lasser|Micah Dunthorn|"
//	      		+ "Debashish Bhattacharya|David J Patterson|Burki F|Shalchian-Tabrizi K|Minge M)"
//	      		+ "(\\w|\\.|\\-|,|\\s)*\\| [^\\{\\}]*(\\}\\}<\\/ref>))"); // ####8####2054.861 && if run in reverse: ####8####197.477
//      SCAN_REGEX_SINK("(\\}\\}<\\/ref>)"); // ####209277####181.744
      regex = "("
      		+ "(\\[\\[[\\w\\|\\.'\\-\\s]+\\]\\])"
      		+ "|"
      		+ "("
      		+ "(Angie|Ashton|Aubrey|Avery|Bek|Benedict|Bernadine|Bethany|Bette|Betty|"
      		+ "Blanche|Braden|Bradley|Bret|Brett|Burdine|Caden|Cadence|Carrington|Charles|"
      		+ "Charlton|Chay|Chet|Christopher|Clinton|Corina|Cristalyn|Dany|Daniel|Daris|Darleen|"
      		+ "Darlene|Darnell|David|Deb|Debbie|Demi|Dennis|Destiny|Diamond|Dorothy|Earlene|"
      		+ "Edith|Elaine|Elfriede|Evan|Gabriel|Georgiana|Gladys|Greenbury|Gregory|H|"
      		+ "Harley|Hazel|Heather|Henrietta|Howard|Hulda|Increase|India|Irene|Iris|Jack|Jackie|"
      		+ "Jade|Jemma|Jenny|Jerrold|Jerry|Jethro|Jigar|Jill|Jocelyn|Jodie|Julia|Julie|"
      		+ "Justine|Kate|Kathryn|Kendall|Kendra|Kerr|Kimball|Kitty|Kristy|Kymber|Lawrence|"
      		+ "Leanne|Leonora|Lianne|Lisa|Liza|Lizzie|Louise|Luci|Lucius|M|Maddox|Madelaine|"
      		+ "Malford|Marlene|Mary|Mari|Maud|Melinda|Mike|Michael|Michelle|Mildred|Miley|Millicent|Mindy|Moira|Mycroft|Nancy|"
      		+ "Naomi|Nelson|Nevaeh|Nigel|Osbert|Ottilie|Ottiwell|Peter|Paris|Pascoe|Patricia|Percy|"
      		+ "Philip|Philippa|Pippa|Poppy|Priscilla|Quentin|Quintus|Rebecca|Reynold|Riley|"
      		+ "Rosaleen|Rosalie|Rosie|Ruby|Ruth|Sanford|Sara|Sarah|Savannah|Scarlett|Sharon|Sheridan|"
      		+ "Shiloh|Simone|Stacy|Sylvia|Tabitha|Tammy|Thaddeus|Timothy|Travis|Trent|Tyler|"
      		+ "Velma|Vicary|Vince|Virginia|Whitney|Whittaker|Wilfried)( \\w\\.?)? (\\w+)( Jr\\.?)?)"
      		+ ")"
      		+ "( (((is|was) born)|(died)) in )"
      		+ "("
      		+ "(\\[\\[[\\w\\|\\.'\\-\\s]+\\]\\])"
      		+ "|"
      		+ "((Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|"
      		+ "Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|"
      		+ "Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|"
      		+ "New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|"
      		+ "Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|"
      		+ "Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming|"
      		+ "AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|"
      		+ "NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)(, (USA|U\\.S\\.A\\.))?)"
      		+ ")";
//      SCAN_REGEX_SINK("("+regex+")"); // ####1####138.265 (4K records) // ####1####74.657 (reverse execution)
//      SCAN_REGEX_SINK(regex); // ####1####5.56
//      SCAN_REGEX_SINK("("
//      		+ "(\\[\\[[\\w\\|\\.'\\-\\s]+\\]\\])"
//      		+ "|"
//      		+ "("
//      		+ "(Angie|Ashton|Aubrey|Avery|Bek|Benedict|Bernadine|Bethany|Bette|Betty|"
//      		+ "Blanche|Braden|Bradley|Bret|Brett|Burdine|Caden|Cadence|Carrington|Charles|"
//      		+ "Charlton|Chay|Chet|Christopher|Clinton|Corina|Cristalyn|Dany|Daniel|Daris|Darleen|"
//      		+ "Darlene|Darnell|David|Deb|Debbie|Demi|Dennis|Destiny|Diamond|Dorothy|Earlene|"
//      		+ "Edith|Elaine|Elfriede|Evan|Gabriel|Georgiana|Gladys|Greenbury|Gregory|H|"
//      		+ "Harley|Hazel|Heather|Henrietta|Howard|Hulda|Increase|India|Irene|Iris|Jack|Jackie|"
//      		+ "Jade|Jemma|Jenny|Jerrold|Jerry|Jethro|Jigar|Jill|Jocelyn|Jodie|Julia|Julie|"
//      		+ "Justine|Kate|Kathryn|Kendall|Kendra|Kerr|Kimball|Kitty|Kristy|Kymber|Lawrence|"
//      		+ "Leanne|Leonora|Lianne|Lisa|Liza|Lizzie|Louise|Luci|Lucius|M|Maddox|Madelaine|"
//      		+ "Malford|Marlene|Mary|Mari|Maud|Melinda|Mike|Michael|Michelle|Mildred|Miley|Millicent|Mindy|Moira|Mycroft|Nancy|"
//      		+ "Naomi|Nelson|Nevaeh|Nigel|Osbert|Ottilie|Ottiwell|Peter|Paris|Pascoe|Patricia|Percy|"
//      		+ "Philip|Philippa|Pippa|Poppy|Priscilla|Quentin|Quintus|Rebecca|Reynold|Riley|"
//      		+ "Rosaleen|Rosalie|Rosie|Ruby|Ruth|Sanford|Sara|Sarah|Savannah|Scarlett|Sharon|Sheridan|"
//      		+ "Shiloh|Simone|Stacy|Sylvia|Tabitha|Tammy|Thaddeus|Timothy|Travis|Trent|Tyler|"
//      		+ "Velma|Vicary|Vince|Virginia|Whitney|Whittaker|Wilfried)( \\w\\.?)? (\\w+)( Jr\\.?)?)"
//      		+ ")"); // ####3118####122.28
//      SCAN_REGEX_SINK("( (((is|was) born)|(died)) in )"); // ####259####2.241
//      SCAN_REGEX_SINK("("
//      		+ "(\\[\\[[\\w\\|\\.'\\-\\s]+\\]\\])"
//      		+ "|"
//      		+ "((Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|"
//      		+ "Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|"
//      		+ "Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|"
//      		+ "New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|"
//      		+ "Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|"
//      		+ "Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming|"
//      		+ "AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|"
//      		+ "NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)(, (USA|U\\.S\\.A\\.))?)"
//      		+ ")"); // ####3976####67.808
       
      regex = "("
	      		+ "(\\[\\[[A-Z][a-z]+( [A-Z]\\.)? [A-Z][a-z]+\\]\\])"
	      		+ "|"
	      		+ "("
		      		+ "(Angie|Ashton|Aubrey|Avery|Bek|Benedict|Bernadine|Bethany|Bette|Betty|"
		      		+ "Blanche|Braden|Bradley|Bret|Brett|Burdine|Caden|Cadence|Carrington|Charles|"
		      		+ "Charlton|Chay|Chet|Christopher|Clinton|Corina|Cristalyn|Dany|Daniel|Daris|Darleen|"
		      		+ "Darlene|Darnell|David|Deb|Debbie|Demi|Dennis|Destiny|Diamond|Dorothy|Earlene|"
		      		+ "Edith|Elaine|Elfriede|Evan|Gabriel|Georgiana|Gladys|Greenbury|Gregory|H|"
		      		+ "Harley|Hazel|Heather|Henrietta|Howard|Hulda|Increase|India|Irene|Iris|Jack|Jackie|"
		      		+ "Jade|Jemma|Jenny|Jerrold|Jerry|Jethro|Jigar|Jill|Jocelyn|Jodie|Julia|Julie|"
		      		+ "Justine|Kate|Kathryn|Kendall|Kendra|Kerr|Kimball|Kitty|Kristy|Kymber|Lawrence|"
		      		+ "Leanne|Leonora|Lianne|Lisa|Liza|Lizzie|Louise|Luci|Lucius|M|Maddox|Madelaine|"
		      		+ "Malford|Marlene|Mary|Mari|Maud|Melinda|Mike|Michael|Michelle|Mildred|Miley|Millicent|Mindy|Moira|Mycroft|Nancy|"
		      		+ "Naomi|Nelson|Nevaeh|Nigel|Osbert|Ottilie|Ottiwell|Peter|Paris|Pascoe|Patricia|Percy|"
		      		+ "Philip|Philippa|Pippa|Poppy|Priscilla|Quentin|Quintus|Rebecca|Reynold|Riley|"
		      		+ "Rosaleen|Rosalie|Rosie|Ruby|Ruth|Sanford|Sara|Sarah|Savannah|Scarlett|Sharon|Sheridan|"
		      		+ "Shiloh|Simone|Stacy|Sylvia|Tabitha|Tammy|Thaddeus|Timothy|Travis|Trent|Tyler|"
		      		+ "Velma|Vicary|Vince|Virginia|Whitney|Whittaker|Wilfried)( \\w\\.?)? (\\w+)( Jr\\.?)?"
	      		+ ")"
      		+ ")"
    		//
      		+ "( (patent(ed|s)|built|builds|made|makes|invent(s|ed)|found(s|ed)|start(s|ed)|begins|began) )"
      		//
      		+ "("
      			+ "(the )?"
      			+ "("
      				+ "(\\[\\[[\\w\\s\\-\\.'\\|]+\\]\\])"
      				+ "|"
      				+ "([A-Z][a-z]+)"
      			+ ")( (Inc\\.|Incorporation|Company|Palace|Castle|University|Institute|Org\\.|Organization|Kingdom|Impire|Regime|(F|f)actory|engine|Engine))?"
      		+ ")"
      		//
      		+ "("
      			+ " in (the )?"
      			+ "("
      				+ "(\\[\\[[\\w\\s\\-\\.'\\|]+\\]\\])" // entity
      				+ "|"
      				+ "(\\d{4})" // year
      				+ "|"
      				+ "(Jan|January|Feb|February|Mar|March|Apr|April|"
      						+ "May|Jun|June|Jul|July|Aug|August|Sep|September|"
      						+ "Oct|October|Nov|November|Dec|December)" // month
      				+ "|"
      				+ "(U\\.S\\.(A\\.)?|America|Germany|Poland|France|Italy|Iran|Iraq|Egypt|"
	      				+ "China|Russia|Netherlands|Switzerland|Sweden|Denmark|"
	      				+ "Canada|Chile|India|(North |South )?Korea)" // country
      			+ ")"
      		+ ")";
      
//      SCAN_REGEX_SINK("("+regex+")"); // ####1####235.396 (for 16K records) // ####1####602.032 (reverse execution)
//      SCAN_REGEX_SINK(regex); // ####1####12.6
       // perform the extraction task
      
      regex = "(book)";
//      SCAN_REGEX_SINK("(" + regex + ")");
//      SCAN_REGEX_SINK(regex);
      
      regex = "(book )(is )(written)";
//      SCAN_REGEX_SINK("(" + regex + ")");
//      SCAN_REGEX_SINK(regex);
      
      regex = "( [^\\}\\{]+ )";
//      SCAN_REGEX_SINK("(" + regex + ")");
//      SCAN_REGEX_SINK(regex);
      
      regex = "( [^\\}\\{]+ )([^\\}\\{]+)";
//      SCAN_REGEX_SINK("(" + regex + ")");
//      SCAN_REGEX_SINK(regex);
      
      regex = "((book|film|movie|story|company|magazine) )([^\\}\\{]+ )((written|made|built) by )(\\w+)";
//      SCAN_REGEX_SINK("(" + regex + ")");
//      SCAN_REGEX_SINK(regex);
      
      regex = "((book|film|movie|story|company|magazine) )([^\\}\\{]+ )((written|made|built) by )(\\[\\[)(\\w+)(\\]\\])";
//      SCAN_REGEX_SINK("(" + regex + ")");
//      SCAN_REGEX_SINK(regex);
      
      regex = "<<test_product_type_label>> ([^\\}\\{]+ )((written|made|built) by )(\\[\\[)(\\w+)(\\]\\])";
//      SCAN_LABELS_REGEX_SINK(regex, "test_product_type_label");
      
      /// movie => 364 results in 1000 tuples
      /// 
      
      regex = "<<movie>>\\w+ (was|is) made by <<director>> in \\d{4}\\.";
//      SCAN_LABELS_REGEX_SINK(regex, "movie", "director");
      
      regex = "<<movie>>";
//      SCAN_LABELS_REGEX_SINK(regex, "movie");
      
      regex = "<<entity>> <<verb>> <<entity>> in <<date>>";
//      SCAN_LABELS_REGEX_SINK(regex, "entity", "verb", "date");
      
      regex = "From the <<entity>> until <<entity>>";
//      SCAN_LABELS_REGEX_SINK(regex, "entity");
      
      regex = "However,( in \\d{4},)? <<test_subject_label>>( (has always|do|was|is|only|would|has been|will|were|will be))?"
      		+ " <<test_verb_label>>( (an|\\[[^\\[\\]]+\\]|vulnerable|all|that|a strong|to|different|certain|the|on))";
//      SCAN_REGEX_SINK("(" + regex + ")"); // 1 results in 1000 records
//      SCAN_LABELS_REGEX_SINK(regex, "test_subject_label", "test_verb_label");
      
      regex = "(Philippa|Pippa|Poppy|Priscilla|Quentin|Quintus|Rebecca|Reynold|Riley|"
		      		+ "Rosaleen|Rosalie|Rosie|Ruby|Ruth|Sanford|Sara|Sarah|Savannah|Scarlett|Sharon|Sheridan|"
		      		+ "Shiloh|Simone|Stacy|Sylvia|Tabitha|Tammy|Thaddeus|Timothy|Travis|Trent|Tyler|"
		      		+ "Velma|Vicary|Vince|Virginia|Whitney|Whittaker|Wilfried|.+)"
		      		+ "(\\{\\{)"
		      		+ "(Tabitha|Tammy|Thaddeus|Timothy|Travis|Trent|Tyler|"
		      		+ "Velma|Vicary|Vince|Virginia|Whitney|Whittaker|Wilfried|[^\\{\\}]*)"
		      		+ "(\\}\\})"
		      		+ "(Rosaleen|Rosalie|Rosie|Ruby|Ruth|Sanford|Sara|Sarah|Savannah|Scarlett|Sharon|Sheridan|"
		      		+ "Shiloh|Simone|Stacy|Sylvia|Tabitha|Tammy|Thaddeus|Timothy|Travis|Trent|Tyler|"
		      		+ "Velma|Vicary|Vince|Virginia|Whitney|Whittaker|Wilfried|.+)";
//    SCAN_REGEX_SINK(regex);
      
//      regex = "(he|she) went";
//      regex = "(he|she)";
//      regex = "[^z]*";
//      Pattern p = new Pattern(regex);
//      Matcher m = p.matcher();
//      m.setTarget("zzzdabc", 3, 7);
//      while(m.proceed()){
//    	  System.out.print("(" + m.start() + "," + m.end() + "),");
//      }
      
      
      /*
       * 0
The original regex is not breakable.
<<test_product_type_label>> ([^\}\{]+ )((written|made|built) by )(\[\[)(\w+)(\]\])####7####44.291
The original regex is not breakable.
The original regex is not breakable.
<<movie>>\w+ (was|is) made by <<director>> in \d{4}\.####0####6841.605
The original regex is not breakable.
<<movie>>####6363####599.866
The original regex is not breakable.
The original regex is not breakable.
<<entity>> <<verb>> <<entity>> in <<date>>####0####81.75
From the <<entity>> until <<entity>>####0####3.278
The original regex is not breakable.
The original regex is not breakable.
However,( in \d{4},)? <<test_subject_label>>( (has always|do|was|is|only|would|has been|will|were|will be))? <<test_verb_label>>( (an|\[[^\[\]]+\]|vulnerable|all|that|a strong|to|different|certain|the|on))####4####58.544
       */
    }



    public static Tuple parsePromedHTML(String fileName, String content) {
        try {
            Document parsedDocument = Jsoup.parse(content);
            String mainText = parsedDocument.getElementById("preview").text();
            Tuple tuple = new Tuple(PromedSchema.PROMED_SCHEMA, new StringField(fileName), new TextField(mainText));
            return tuple;
        } catch (Exception e) {
            return null;
        }
    }

    public static void writeSampleIndex() throws Exception {
        // parse the original file
        File sourceFileFolder = new File(wikipediaFilesDirectory.toString());
        ArrayList<Tuple> fileTuples = new ArrayList<>();
        for (File htmlFile : sourceFileFolder.listFiles()) {
            StringBuilder sb = new StringBuilder();
            Scanner scanner = new Scanner(htmlFile);
            while (scanner.hasNext()) {
                Tuple tuple = WikipediaIndexWriter.recordToTuple(scanner.nextLine());
                if (tuple != null) {
                    fileTuples.add(tuple);
                }
            }
            scanner.close();
        }

        // write tuples into the table
        RelationManager relationManager = RelationManager.getInstance();

        relationManager.deleteTable(WIKIPEDIA_SAMPLE_TABLE);
        relationManager.createTable(WIKIPEDIA_SAMPLE_TABLE, wikipediaIndexDirectory,
                WikipediaIndexWriter.SCHEMA_WIKIPEDIA, LuceneAnalyzerConstants.standardAnalyzerString());

        DataWriter dataWriter = relationManager.getTableDataWriter(WIKIPEDIA_SAMPLE_TABLE);
        dataWriter.open();
        for (Tuple tuple : fileTuples) {
            dataWriter.insertTuple(tuple);
            number += 1;
        }
        dataWriter.close();
    }
    
    public static void SCAN_LABELS_REGEX_SINK(String regex, String... labels){
    	ScanSourcePredicate scanSourcePredicate = new ScanSourcePredicate(WIKIPEDIA_SAMPLE_TABLE);
        ScanBasedSourceOperator scanBasedSourceOperator = new ScanBasedSourceOperator(scanSourcePredicate);
        List<String> attributeNames = Arrays.asList(WikipediaIndexWriter.TEXT);
    
        RelationManager relationManager = RelationManager.getInstance();
        String luceneAnalyzerStr = relationManager.getTableAnalyzerString(WIKIPEDIA_SAMPLE_TABLE);
        
        IOperator lastOperator = scanBasedSourceOperator;
        for(String label : labels){
        	if(label.equals("noun")){
                RegexPredicate regexPredicate = new RegexPredicate("(\\w+)", attributeNames, label);
                RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
                regexMatcher.setInputOperator(lastOperator);
                lastOperator = regexMatcher;
        	}else if(label.equals("verb")){
                RegexPredicate regexPredicate = new RegexPredicate("(make|makes|made|build|builds|built|wrote|write|writes)", attributeNames, label);
                RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
                regexMatcher.setInputOperator(lastOperator);
                lastOperator = regexMatcher;
        	}else if(label.equals("country")){
                RegexPredicate regexPredicate = new RegexPredicate("(America|Germany|Poland|France|Italy|Iran|Iraq|Egypt|"
	      				+ "China|Russia|Netherlands|Switzerland|Sweden|Denmark|"
	      				+ "Canada|Chile|India|(North |South )?Korea)", attributeNames, label);
                RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
                regexMatcher.setInputOperator(lastOperator);
                lastOperator = regexMatcher;        		
        	}else if(label.equals("date")){
                RegexPredicate regexPredicate = new RegexPredicate("(\\d{4}|((Jan|January|Feb|February|Mar|March|Apr|April|"
      						+ "May|Jun|June|Jul|July|Aug|August|Sep|September|"
      						+ "Oct|October|Nov|November|Dec|December)(, \\d{1,2}(st|nd|rd|th)(, \\d{4})?)?)|(\\d{1,2}/\\d{1,2}/\\d{4})|(\\d+ (BC|AC)))", attributeNames, label);
                RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
                regexMatcher.setInputOperator(lastOperator);
                lastOperator = regexMatcher;  
        	}else if(label.equals("test_subject_label")){
                RegexPredicate regexPredicate = new RegexPredicate("(anarchism|they|he|Thomas|Plato|they|this|amateurs|it|very little work)", attributeNames, label);
                RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
                regexMatcher.setInputOperator(lastOperator);
                lastOperator = regexMatcher; 
        	}else if(label.equals("test_verb_label")){
                RegexPredicate regexPredicate = new RegexPredicate("(included|form|left|lost|reports|had|forced|cover|highlights|span|require|done)", attributeNames, label);
                RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
                regexMatcher.setInputOperator(lastOperator);
                lastOperator = regexMatcher;
        	}else if(label.equals("test_product_type_label")){
                RegexPredicate regexPredicate = new RegexPredicate("(book|film|movie|story|company|magazine)", attributeNames, label);
                RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
                regexMatcher.setInputOperator(lastOperator);
                lastOperator = regexMatcher;
        	}else if(label.equals("drug")){
                RegexPredicate regexPredicate = new RegexPredicate("antibutic|peneciline", attributeNames, label);
                RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
                regexMatcher.setInputOperator(lastOperator);
                lastOperator = regexMatcher; 
        	}else if(label.equals("disease")){
                RegexPredicate regexPredicate = new RegexPredicate("cold|flu|HIV|AIDS|cancer", attributeNames, label);
                RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
                regexMatcher.setInputOperator(lastOperator);
                lastOperator = regexMatcher; 
        	}else if(label.equals("person")){
                RegexPredicate regexPredicate = new RegexPredicate("(Angie|Ashton|Aubrey|Avery|Bek|Benedict|Bernadine|Bethany|Bette|Betty|"
		      		+ "Blanche|Braden|Bradley|Bret|Brett|Burdine|Caden|Cadence|Carrington|Charles|"
		      		+ "Charlton|Chay|Chet|Christopher|Clinton|Corina|Cristalyn|Dany|Daniel|Daris|Darleen|"
		      		+ "Darlene|Darnell|David|Deb|Debbie|Demi|Dennis|Destiny|Diamond|Dorothy|Earlene|"
		      		+ "Edith|Elaine|Elfriede|Evan|Gabriel|Georgiana|Gladys|Greenbury|Gregory|H|"
		      		+ "Harley|Hazel|Heather|Henrietta|Howard|Hulda|Increase|India|Irene|Iris|Jack|Jackie|"
		      		+ "Jade|Jemma|Jenny|Jerrold|Jerry|Jethro|Jigar|Jill|Jocelyn|Jodie|Julia|Julie|"
		      		+ "Justine|Kate|Kathryn|Kendall|Kendra|Kerr|Kimball|Kitty|Kristy|Kymber|Lawrence|"
		      		+ "Leanne|Leonora|Lianne|Lisa|Liza|Lizzie|Louise|Luci|Lucius|M|Maddox|Madelaine|"
		      		+ "Malford|Marlene|Mary|Mari|Maud|Melinda|Mike|Michael|Michelle|Mildred|Miley|Millicent|Mindy|Moira|Mycroft|Nancy|"
		      		+ "Naomi|Nelson|Nevaeh|Nigel|Osbert|Ottilie|Ottiwell|Peter|Paris|Pascoe|Patricia|Percy|"
		      		+ "Philip|Philippa|Pippa|Poppy|Priscilla|Quentin|Quintus|Rebecca|Reynold|Riley|"
		      		+ "Rosaleen|Rosalie|Rosie|Ruby|Ruth|Sanford|Sara|Sarah|Savannah|Scarlett|Sharon|Sheridan|"
		      		+ "Shiloh|Simone|Stacy|Sylvia|Tabitha|Tammy|Thaddeus|Timothy|Travis|Trent|Tyler|"
		      		+ "Velma|Vicary|Vince|Virginia|Whitney|Whittaker|Wilfried)( (sr\\.|jr\\.|\\w\\.))?( \\w+)", attributeNames, label);
                RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
                regexMatcher.setInputOperator(lastOperator);
                lastOperator = regexMatcher; 
        	}else if(label.equals("entity")){
                RegexPredicate regexPredicate = new RegexPredicate("\\[\\[[^\\[\\]]+\\]\\]", attributeNames, label);
                RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
                regexMatcher.setInputOperator(lastOperator);
                lastOperator = regexMatcher; 
        	}else if(label.equals("movie")){
                RegexPredicate regexPredicate = new RegexPredicate("(The Shawshank Redemption|The Godfather|The Godfather: Part II|The Dark Knight|12 Angry Men"
                		+ "|Schindler's List|Pulp Fiction|The Lord of the Rings: The Return of the King|The Good, the Bad and the Ugly|Fight Club"
                		+ "|The Lord of the Rings: The Fellowship of the Ring|Forrest Gump|Star Wars: Episode V - The Empire Strikes Back|Inception"
                		+ "|The Lord of the Rings: The Two Towers|One Flew Over the Cuckoo's Nest|Goodfellas|The Matrix|Seven Samurai"
                		+ "|Star Wars: Episode IV - A New Hope|City of God|Se7en|The Silence of the Lambs|It's a Wonderful Life"
                		+ "|Life Is Beautiful|The Usual Suspects|Léon: The Professional|Saving Private Ryan|Spirited Away|Once Upon a Time in the West"
                		+ "|American History X|Interstellar|Psycho|City Lights|Casablanca|The Green Mile|The Intouchables|Modern Times|Raiders of the Lost Ark"
                		+ "|The Pianist|Rear Window|The Departed|Terminator 2: Judgment Day|Back to the Future|Whiplash|Gladiator|The Prestige"
                		+ "|The Lion King|Memento|Apocalypse Now|Alien|The Great Dictator|Sunset Boulevard"
                		+ "|Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb|Cinema Paradiso|The Lives of Others|Grave of the Fireflies"
                		+ "|Paths of Glory|Django Unchained|The Shining|WALL·E|American Beauty|The Dark Knight Rises|Princess Mononoke|Aliens|Oldboy"
                		+ "|Witness for the Prosecution|Once Upon a Time in America|Dunkirk|Das Boot|Citizen Kane|Dangal|North by Northwest|Vertigo"
                		+ "|Star Wars: Episode VI - Return of the Jedi|Braveheart|Reservoir Dogs|M|Requiem for a Dream|Amélie|Like Stars on Earth"
                		+ "|A Clockwork Orange|Lawrence of Arabia|Double Indemnity|Taxi Driver|Amadeus|Eternal Sunshine of the Spotless Mind|Your Name"
                		+ "|To Kill a Mockingbird|Full Metal Jacket|Toy Story 3|2001: A Space Odyssey|Singin' in the Rain|The Sting|Toy Story|Bicycle Thieves"
                		+ "|Inglourious Basterds|The Kid|3 Idiots|Snatch|Monty Python and the Holy Grail|For a Few Dollars More|L.A. Confidential|The Hunt"
                		+ "|Good Will Hunting|Scarface|The Apartment|Rashomon|A Separation|Metropolis|My Father and My Son|Indiana Jones and the Last Crusade"
                		+ "|All About Eve|Yojimbo|Batman Begins|Up|Some Like It Hot|The Treasure of the Sierra Madre|Unforgiven|Downfall|Die Hard|Raging Bull"
                		+ "|Heat|The Third Man|Children of Heaven|The Great Escape|Chinatown|Ikiru|Pan's Labyrinth|My Neighbor Totoro|Ran|Inside Out|The Gold Rush"
                		+ "|The Secret in Their Eyes|On the Waterfront|Incendies|The Bridge on the River Kwai|Judgment at Nuremberg|Howl's Moving Castle|Room"
                		+ "|Blade Runner|The Seventh Seal|Lock, Stock and Two Smoking Barrels|Mr. Smith Goes to Washington|Casino|A Beautiful Mind|The Elephant Man"
                		+ "|Wild Strawberries|V for Vendetta|The Wolf of Wall Street|The General|Warrior|La La Land|Andrei Rublev|Trainspotting|Dial M for Murder"
                		+ "|Sunrise|The Bandit|Gran Torino|The Deer Hunter|Gone with the Wind|Fargo|The Sixth Sense|The Big Lebowski|The Thing|No Country for Old Men"
                		+ "|Logan|Finding Nemo|Tokyo Story|Hacksaw Ridge|Cool Hand Luke|Rang De Basanti|There Will Be Blood|Rebecca|The Passion of Joan of Arc|Come and See"
                		+ "|Kill Bill: Vol. 1|How to Train Your Dragon|Mary and Max|Gone Girl|A Wednesday|Into the Wild|Shutter Island|It Happened One Night|Life of Brian"
                		+ "|Wild Tales|Platoon|Baby Driver|Hotel Rwanda|Rush|The Wages of Fear|Network|In the Name of the Father|Stand by Me|Ben-Hur|The 400 Blows"
                		+ "|The Grand Budapest Hotel|Persona|Mad Max: Fury Road|Spotlight|12 Years a Slave|Million Dollar Baby|Memories of Murder|Jurassic Park"
                		+ "|Butch Cassidy and the Sundance Kid|Amores Perros|Stalker|The Maltese Falcon|Paper Moon|The Truman Show|Hachi: A Dog's Tale"
                		+ "|The Nights of Cabiria|Nausicaä of the Valley of the Wind|The Princess Bride|Before Sunrise|Munna Bhai M.B.B.S."
                		+ "|Harry Potter and the Deathly Hallows: Part 2|The Grapes of Wrath|Prisoners|Rocky|Star Wars: The Force Awakens|Touch of Evil"
                		+ "|Catch Me If You Can|Sholay|Gandhi|Diabolique|Donnie Darko|Monsters, Inc.|Annie Hall|The Bourne Ultimatum|The Terminator|Barry Lyndon"
                		+ "|The Wizard of Oz|Groundhog Day|La Haine|Jaws|Twelve Monkeys|Infernal Affairs|The Best Years of Our Lives|Hera Pheri|The Help"
                		+ "|Beauty and the Beast|In the Mood for Love|The Battle of Algiers|Gangs of Wasseypur|Dog Day Afternoon|What Ever Happened to Baby Jane"
                		+ "|Pirates of the Caribbean: The Curse of the Black Pearl|PK)", attributeNames, label);
                RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
                regexMatcher.setInputOperator(lastOperator);
                lastOperator = regexMatcher; 
        	}else if(label.equals("director")){
                RegexPredicate regexPredicate = new RegexPredicate("(Abdisalam Aato|Dodo Abashidze|George Abbott|Jim Abrahams|Abiola Abrams|J\\. J\\. Abrams|"
                		+ "Tengiz Abuladze|Herbert Achternbusch|Andrew Adamson|Percy Adlon|John G\\. Adolfi|Franklin Adreon|Alejandro Agresti|Aqeel Ahmed|"
                		+ "Alexandre Aja|Chantal Akerman|Desiree Akhavan|Fatih Akın|Moustapha Akkad|Timothy Albee|Barbara Albert|Alan Alda|Robert Aldrich|"
                		+ "Tomás Gutiérrez Alea|Grigori Aleksandrov|Marc Allégret|Yves Allégret|Elizabeth Allen|Irwin Allen|Lewis Allen|Woody Allen|Sherman Alexie|"
                		+ "Syed Ali Raza Usama|Pedro Almodóvar|Paul Almond|Robert Altman|Fede Alvarez|Mathieu Amalric|Ned Ambler|Gianni Amelio|Alejandro Amenábar|"
                		+ "Jon Amiel|Lindsay Anderson|Michael Anderson|Mitch Anderson|Paul Thomas Anderson|Paul W\\. S\\. Anderson|Wes Anderson|Brad Anderson|"
                		+ "Roy Andersson|Metodi Andonov|Raoul André|Kostas Andritsos|Theo Angelopoulos|Kenneth Anger|Threes Anna|Ken Annakin|Jean\\-Jacques Annaud|"
                		+ "Hideaki Anno|Arnold Antonin|Michelangelo Antonioni|Oscar Apfel|Michael Apted|Gregg Araki|Alfonso Aráu|Denys Arcand|George Archainbaud|"
                		+ "Jane Arden|Emile Ardolino|Asia Argento|Dario Argento|Dionciel Armstrong|Gillian Armstrong|Andrea Arnold|Jack Arnold|Darren Aronofsky|"
                		+ "Fernando Arrabal|Miguel Arteta|Dorothy Arzner|Hal Ashby|John Mallory Asher|Olivier Assayas|Anthony Asquith|Carlos Atanes|Fatih Akin|"
                		+ "Richard Attenborough|David Attwood|Jacques Audiard|Jacqueline Audry|John H\\. Auer|Bille August|Claude Autant\\-Lara|Aram Avakian|Roger Avary|"
                		+ "Pupi Avati|Tex Avery|John G\\. Avildsen|Dan Aykroyd|Nabil Ayouch|Mary Ayubi|Héctor Babenco|Lloyd Bacon|Clarence G\\. Badger|"
                		+ "John Badham|Bae Yong\\-Kyun|Cindy Baer|Prince Bagdasarian|King Baggot|Stuart Baird|Roy Ward Baker|Mohammad Bakri|Ralph Bakshi|"
                		+ "Aleksei Balabanov|Kailasam Balachander|Carroll Ballard|Monty Banks|Juan Antonio Bardem|Richard L\\. Bare|Chris Barfoot|Sooraj R\\. Barjatya|"
                		+ "Clive Barker|Reginald Barker|Tom Barman|Boris Barnet|Matthew Barney|Daniel Barnz|Chuck Barris|Drew Barrymore|Lionel Barrymore|Andrzej Bartkowiak|"
                		+ "Jason Bateman|Paul Bartel|Hall Bartlett|Charles Barton|Felix Basch|Mario Bava|RD Bawa|Michael Bay|Juan Antonio Bayona|Warren Beatty|"
                		+ "William Beaudine|Harry Beaumont|Harold Becker|Jacques Becker|Josh Becker|Wolfgang Becker|Guy Norman Bee|Ford Beebe|Hans Behrendt|"
                		+ "Jean\\-Jacques Beineix|Monta Bell|Marco Bellocchio|Jerry Belson|Maria Luisa Bemberg|Jack Bender|László Benedek|Shyam Benegal|Roberto Benigni|"
                		+ "Richard Benjamin|Spencer Gordon Bennet|Robert Benton|Bruce Beresford|Andrew Bergman|Ingmar Bergman|Busby Berkeley|Luis Garcia Berlanga|"
                		+ "Marc Berlin|Paul Bern|Ishmael Bernal|Curtis Bernhardt|Claude Berri|John Berry|Arthur Berthelet|André Berthomieu|Bernardo Bertolucci|"
                		+ "Luc Besson|Frank Beyer|Bharathiraja|John Biddle|Fabián Bielinsky|Kathryn Bigelow|Tony Bill|Peter Billingsley|Bruce Bilson|Brad Bird|"
                		+ "Alice Guy\\-Blaché|Herbert Blaché|Nicola Black|George Blair|Alessandro Blasetti|Bertrand Blier|Neill Blomkamp|Don Bluth|John G\\. Blystone|"
                		+ "Carl Boese|Budd Boetticher|Paul Bogart|Peter Bogdanovich|José Bohr|Michel Boisrond|Patrick Bokanowski|Richard Boleslawski|Uwe Boll|"
                		+ "Mauro Bolognini|Fyodor Bondarchuk|Sergei Bondarchuk|Bong Joon\\-ho|John Boorman|Walerian Borowczyk|Frank Borzage|John and Roy Boulting|"
                		+ "Lucien Bourjeily|David Bowers|Danny Boyle|Charles Brabin|Robert N\\. Bradbury|John Brahm|Stan Brakhage|A\\.V\\. Bramble|Kenneth Branagh|"
                		+ "Catherine Breillat|Herbert Brenon|Robert Bresson|Martin Brest|Vinko Brešan|Howard Bretherton|Sean Bridgers|James Bridges|Steven Brill|"
                		+ "Lino Brocka|Peter Brook|Matt Brookens|Albert Brooks|James L\\. Brooks|Mel Brooks|Richard Brooks|Nick Broomfield|Simon Bross|James Broughton|"
                		+ "Otto Brower|Clarence Brown|Rowland Brown|Tod Browning|Adrian Brunel|Detlev Buck|Tom Buckingham|Steve Buscemi|Harold S\\. Bucquet|Jan Bucquoy|"
                		+ "Danny Buday|John Carl Buechler|Andrew Bujalski|Luis Buñuel|Charles Burnett|Edward Burns|Tim Burstall|Tim Burton|Alexander Butler|David Butler|"
                		+ "Robert Butler|Jörg Buttgereit|Edward Buzzell|Bobby Razak|Christy Cabanne|Michael Cacoyannis|Israel Adrián Caetano|Nicolas Cage|Mike Cahill|"
                		+ "Edward L\\. Cahn|James Cameron|Donald Cammell|Joe Camp|Juan José Campanella|Colin Campbell|Martin Campbell|Jane Campion|Danny Cannon|Dyan Cannon|"
                		+ "Graham Cantwell|Albert Capellani|Frank Capra|Leos Carax|Marcel Carné|es:Marcos Carnevale|Marc Caro|Niki Caro|John Carpenter|Thomas Carr|"
                		+ "Enrique Carreras|John Paddy Carstairs|Chris Carter|D\\.J\\. Caruso|John Cassavetes|Nick Cassavetes|P\\. J\\. Castellaneta|William Castle|"
                		+ "Torre Catalano|Michael Caton\\-Jones|Peter Cattaneo|Alberto Cavalcanti Brazilian|Liliana Cavani|André Cayatte|Ralph Ceder|Jeff Celentano|"
                		+ "Nuri Bilge Ceylan|Claude Chabrol|Gurinder Chadha|Don Chaffey|Youssef Chahine|Fruit Chan|Jackie Chan|Peter Chan|Charlie Chaplin|Charley Chase|"
                		+ "Émile Chautard|Jeremiah S\\. Chechik|Peter Chelsom|Kaige Chen|Pierre Chenal|Pierre Chevalier|Abigail Child|Ching Siu\\-Tung|Samson Chiu|"
                		+ "Lisa Cholodenko|Yash Chopra|Chor Yuen|Stephen Chow|Benjamin Christensen|Roger Christian|Christian\\-Jaque|Rich Christiano|Grigori Chukhrai|"
                		+ "Peter Chung|Věra Chytilová|Michael Cimino|Souleymane Cissé|René Clair|Larry Clamage|Bob Clark|Larry Clark|Shirley Clarke|Jack Clayton|"
                		+ "William Clemens|René Clément|Elmer Clifton|Edward F\\. Cline|George Clooney|Robert Clouse|Henri\\-Georges Clouzot|Enrico Cocozza|Jean Cocteau|"
                		+ "Coen Brothers|Larry Cohen|Rob Cohen|Keri Collins|Lewis D\\. Collins|Mary Collins|Chris Columbus|Luigi Comencini|Bill Condon|Bruce Conner|"
                		+ "Jack Conway|Fielder Cook|Merian C\\. Cooper|Francis Ford Coppola|Sofia Coppola|Frank Coraci|Roger Corman|Alain Corneau|Joe Cornish|"
                		+ "Lloyd Corrigan|Raiya Corsiglia|Don Coscarelli|George Pan Cosmatos|Pedro Costa|Ricardo Costa|Costa\\-Gavras|Kevin Costner|Manny Coto|"
                		+ "T\\. Arthur Cottam|Alex Cox|Paul Cox|William James Craft|Wes Craven|Charles Crichton|Michael Crichton|Jon Cring|Donald Crisp|Donald Crombie|"
                		+ "John Cromwell|David Cronenberg|Alan Crosland|Matthew Crouch|Cameron Crowe|James Cruze|Alfonso Cuarón|George Cukor|Irving Cummings|"
                		+ "James Cunningham|Sean S\\. Cunningham|Richard Curtis|Michael Curtiz|Paul Czinner|Debarun Pal|Joe D'Amato|Diminas Dagogo|John Dahl|"
                		+ "Georgi Daneliya|Frank Daniel|Lee Daniels|Vladimir Danilevich|Joe Dante|Frank Darabont|Harry d'Abbadie d'Arrast|Jules Dassin|Hayato Date|"
                		+ "Byambasuren Davaa|Delmer Daves|Alki David|Terence Davies|Andrew Davis|Ossie Davis|Tamra Davis|J\\. Searle Dawley|Jonathan Dayton|"
                		+ "Drew Daywalt|Basil Dean|Basil Dearden|Tiffanie DeBartolo|Jan de Bont|Philippe de Broca|Fred de Cordova|Albert de Courville|"
                		+ "Russell DeGrazier|Rolf de Heer|Alex De La Iglesia|Jean Delannoy|Bruce Dellis|Hampton Del Ruth|Roy Del Ruth|Guillermo del Toro|"
                		+ "Cecil B\\. DeMille|William C\\. deMille|Jonathan Demme|Ted Demme|Jacques Demy|Reginald Denham|Robert De Niro|Claire Denis|Pen Densham|"
                		+ "Ruggero Deodato|Manoel de Oliveira|Brian De Palma|Serge de Poligny|Johnny Depp|John Derek|Maya Deren|Scott Derrickson|Giuseppe de Santis|"
                		+ "Vittorio De Sica|Howard Deutch|Michel Deville|Danny DeVito|David Dhavan|Tom DiCillo|Thorold Dickinson|William Kennedy Dickson|Carlos Diegues|"
                		+ "William Dieterle|Helmut Dietl|John Francis Dillon|Walt Disney|Edward Dmytryk|Pete Docter|Jacques Doillon|Michael Dolan|Andrew Dominik|"
                		+ "Roger Donaldson|Stanley Donen|Richard Donner|Mark Donskoi|Robert Dornhelm|Doris Dörrie|Nelson Pereira dos Santos|Gordon Douglas|"
                		+ "Aleksandr Dovzhenko|B\\. E\\. Doxat\\-Pratt|Oliver Drake|Polly Draper|Jean Dréville|Ben Drew|Carl Theodor Dreyer|Marcel Duchamp|Peter Duffell|"
                		+ "Troy Duffy|Dennis Dugan|Joshua Dugdale|Bill Duke|Bruno Dumont|George Dunning|Ewald André Dupont|Marguerite Duras|Richard Dutcher|Guru Dutt|"
                		+ "Ava DuVernay|Julien Duvivier|Allan Dwan|B\\. Reeves Eason|Clint Eastwood|Uli Edel|Blake Edwards|Gareth Edwards|Harry Edwards|Shawn Efran|"
                		+ "Atom Egoyan|Sergei Eisenstein|Richard Elfman|Stephan Elliott|Robert Ellis|Maurice Elvey|John Emerson|Roland Emmerich|Cy Endfield|John English|"
                		+ "Robert Enrico|Ray Enright|Nora Ephron|Jean Epstein|Víctor Erice|Chester Erskine|Juan Escobedo|Jean Eustache|Gareth Evans|Marc Evans|Valie Export|"
                		+ "Chris Eyre|Richard Eyre|Peter Faiman|James Fargo|Harun Farocki|Asghar Farhadi|Valerie Faris|Farrelly brothers|John Farrow|Rainer Werner Fassbinder|"
                		+ "Jon Favreau|Fei Mu|Henry Feinberg|Paul Fejos|Andrea Fellers|Federico Fellini|Marc Femenella|Emilio Fernández|Abel Ferrara|Marco Ferreri|"
                		+ "Giorgio Ferroni|Louis Feuillade|Jacques Feyder|Sally Field|Todd Field|Mike Figgis|David Fincher|Will Finn|Terence Fisher|Dallas M\\. Fitzgerald|"
                		+ "George Fitzmaurice|Robert J\\. Flaherty|Gary Fleder|Richard Fleischer|Victor Fleming|Benedek Fliegauf|James Flood|Robert Florey|Emmett J\\. Flynn|"
                		+ "James Foley|Jorge Fons|Aleksander Ford|Jeremy J\\. Ford|John Ford|Philip Ford|Eugene Forde|Miloš Forman|Tom Forman|Willi Forst|Marc Forster|"
                		+ "Bill Forsyth|Bob Fosse|Jodie Foster|Lilibet Foster|Norman Foster|Wallace Fox|Bryan Foy|Jonathan Frakes|Coleman Francis|Jesús Franco|Georges Franju|"
                		+ "Melvin Frank|John Frankenheimer|Sidney Franklin|Harry L\\. Fraser|James Frawley|Stephen Frears|Sydney Freeland|Thornton Freeland|Morgan Freeman|"
                		+ "Mark Freiburger|Karl Freund|Ron Fricke|Fridrik Thor Fridriksson|Jason Friedberg|Lionel Friedberg|William Friedkin|Seymour Friedman|"
                		+ "Su Friedrich|Soleil Moon Frye|Kinji Fukasaku|Lucio Fulci|Sam Fuller|Antoine Fuqua|Sidney J\\. Furie|Béla Gaál|Leonid Gaidai|Florian Gallenberger|"
                		+ "Alex Galvin|Harry Gamboa, Jr\\.|Abel Gance|Christophe Gans|Dennis Gansel|Arline Gant|Carla Garapedian|Tay Garnett|Philippe Garrel|William Garwood|"
                		+ "Louis J\\. Gasnier|Tony Gatlif|Nils Gaup|Roberto Gavaldón|Barrie Gavin|Jean Genet|Giacomo Gentilomo|Aleksei German|Pietro Germi|Clyde Geronimi|"
                		+ "Kurt Gerron|Douchan Gersi|Viktor Gertler|Subhash Ghai|Ritwik Ghatak|Bahman Ghobadi|Charles Giblyn|Angela Gibson|Mel Gibson|Joaquin Kino Gil|"
                		+ "John Gilbert|Lewis Gilbert|David Giler|Stuart Gillard|Terry Gilliam|Arvid E\\. Gillstrom|Marino Girolami|Pavel Giroud|Amos Gitai|Jonathan Glazer|"
                		+ "Peter Glenville|Jean\\-Luc Godard|Theo van Gogh|Menahem Golan|Bobcat Goldthwait|Michel Gondry|Alejandro González Iñárritu|Leslie Goodwins|"
                		+ "Adoor Gopalakrishnan|Michael Gordon|Stuart Gordon|Hideo Gosha|Ryan Gosling|Alfred Goulding|Edmund Goulding|Todd Graff|William A\\. Graham|"
                		+ "Lee Grant|F\\. Gary Gray|Alfred E\\. Green|David Gordon Green|Guy Green|Norm Green|Pamela Green|Tom Green|Peter Greenaway|Paul Greengrass|"
                		+ "Robert Greenwald|Edwin Greenwood|Robert Gregson|Jean Gremillon|Johannes Grenzfurthner|John Greyson|John Grierson|D\\. W\\. Griffith|"
                		+ "Edward H\\. Griffith|Murray Grigor|Nick Grinde|Christopher Guest|Val Guest|John Guillermin|Fred Guiol|Sacha Guitry|Yilmaz Güney|James Gunn|"
                		+ "Hrafn Gunnlaugsson|Manish Gupta|Stephen Gurewitz|Sebastian Gutierrez|Alice Guy\\-Blaché|Patricio Guzmán|Kamal Haasan|Taylor Hackford|Tala Hadid|"
                		+ "Lucile Hadžihalilović|Piers Haggard|Paul Haggis|Larry Hagman|Alexander Hall|Lasse Hallström|Victor Hugo Halperin|Gary Halvorson|Bent Hamer|"
                		+ "Robert Hamer|Guy Hamilton|Victor Hanbury|John D\\. Hancock|Michael Haneke|Tom Hanks|Curtis Hanson|Geir Hansteen Jörgensen|Catherine Hardwicke|"
                		+ "Renny Harlin|Mary Harron|William S\\. Hart|Hal Hartley|Anthony Harvey|Herk Harvey|Jack Harvey|Wojciech Has|Henry Hathaway|Howard Hawks|Will Hay|"
                		+ "Salma Hayek|David Hayman|Todd Haynes|Victor Heerman|Stuart Heisler|Brian Helgeland|Monte Hellman|Michael Ffish Hemschoot|Joseph Henabery|"
                		+ "Florian Henckel von Donnersmarck|Dell Henderson|Frank Henenlotter|Hobart Henley|Brian Henson|Jim Henson|Perry Henzell|Paula Heredia|Stephen Herek|"
                		+ "William Blake Herron|Michael Herz|Werner Herzog|Zako Heskiya|Jared Hess|Jerusha Hess|Marianne Hettinger|Jennifer Love Hewitt|David Hewlett|"
                		+ "John Heys|Scott Hicks|Howard Higgin|George Roy Hill|George W\\. Hill|Jack Hill|Robert F\\. Hill|Sinclair Hill|Walter Hill|Arthur Hiller|"
                		+ "Lambert Hillyer|Anthony Himbs|Art Hindle|Ryūichi Hiroki|Leslie S\\. Hiscott|Alfred Hitchcock|Ho Ping|Gregory Hoblit|Brent Hodge|Mike Hodges|"
                		+ "Michael Hoffman|Jack Hofsiss|James P\\. Hogan|Clive Holden|Agnieszka Holland|Savage Steve Holland|Tonya Holly|Ben Holmes|Seth Holt|"
                		+ "P\\.J\\. Hogan|Ishirô Honda|Tobe Hooper|Tom Hooper|Stephen Hopkins|Dennis Hopper|James W\\. Horne|Hou Hsiao\\-hsien|John Hough|David Howard|"
                		+ "Michael Howard|Ron Howard|William Howard|King Hu|L\\. Ron Hubbard|Reginald Hudlin|Hugh Hudson|Howard Hughes|John Hughes|Ken Hughes|Ann Hui|"
                		+ "Danièle Huillet|H\\. Bruce Humberstone|André Hunebelle|Helen Hunt|Sam Hurwitz|Waris Hussein|John Huston|Charles Hutchison|Brian G\\. Hutton|"
                		+ "Willard Huyck|Peter Hyams|Nicholas Hytner|Armando Iannucci|Kon Ichikawa|Im Kwon\\-Taek|Shōhei Imamura|Hiroshi Inagaki|John Ince|Ralph Ince|"
                		+ "Ivan Olita|Thomas H\\. Ince|Rex Ingram|Ciro Ippolito|John Irvin|George Irving|Jūzō Itami|Aleksandr Ivanovsky|Joris Ivens|James Ivory|"
                		+ "Jacques Jaccard|Mick Jackson|Peter Jackson|Sarah Jacobson|Henry Jaglom|Wanda Jakubowska|Alan James|Steve James|Jami|Miklós Jancsó|Jang Joon\\-hwan|"
                		+ "Derek Jarman|Jim Jarmusch|Charles Jarrott|Risto Jarva|Leigh Jason|Garth Jennings|Humphrey Jennings|Dallas Jenkins|Patty Jenkins|Jean\\-Pierre Jeunet|"
                		+ "Norman Jewison|Jia Zhangke|Jiang Wen|Jaromil Jireš|Phil Joanou|Alejandro Jodorowsky|Mark Joffe|Roland Joffé|Clark Johnson|Craig Johnson|"
                		+ "Lamont Johnson|Mark Steven Johnson|Nunnally Johnson|Rian Johnson|Joe Johnston|Angelina Jolie|Chuck Jones|Duncan Jones|F\\. Richard Jones|"
                		+ "Grover Jones|Michael Jones|Terry Jones|Tommy Lee Jones|Spike Jonze|Neil Jordan|Edward José|Max Joseph|Jon Jost|Louis Jouvet|Mike Judge|"
                		+ "Rupert Julian|Miranda July|Karel Kachyňa|Jeremy Kagan|Mauricio Kagel|Mikhail Kalatozov|Kamal|Sekhar Kammula|Deborah Kampmeier|Puttanna Kanagal|"
                		+ "Joseph Kane|Garson Kanin|D\\.W\\. Kann|Jonathan Kaplan|Raj Kapoor|Shekhar Kapur|Pekka Karjalainen|Phil Karlson|Roman Karmen|Jason Kartalian|"
                		+ "Jake Kasdan|Lawrence Kasdan|Mathieu Kassovitz|Aaron Katz|Lloyd Kaufman|Philip Kaufman|Aki Kaurismäki|Mika Kaurismäki|Helmut Käutner|"
                		+ "Jerzy Kawalerowicz|Tony Kaye|Elia Kazan|Mithaq Kazimi|Helmut Käutner|Buster Keaton|Abdellatif Kechiche|William Keighley|Frederick King Keller|"
                		+ "Harry Keller|Michael Keller|David Kellogg|Gene Kelly|Richard Kelly|Gil Kenan|Alex Kendrick|Erle Kenton|Robert Kerr|Irvin Kershner|"
                		+ "James Kerwin|Abbas Kiarostami|Krzysztof Kieślowski|Kim Jee\\-woon|Kim Ki\\-duk|Max Kimmich|Anthony Kimmins|Burton King|Gary King|George King|"
                		+ "Henry King|Louis King|Stephen King|Keisuke Kinoshita|Teinosuke Kinugasa|Randal Kirk|Lyudmil Kirkov|Ryuhei Kitamura|Takeshi Kitano|"
                		+ "Randal Kleiser|Elem Klimov|León Klimovsky|Alexander Kluge|Harley Knoles|Masaki Kobayashi|Vincent Kok|Henry Kolker|Satoshi Kon|Andrei Konchalovsky|"
                		+ "Tadeusz Konwicki|Alexander Korda|Zoltan Korda|Hirokazu Koreeda|Harmony Korine|Caryl Korma|Baltasar Kormákur|John Korty|Joseph Kosinski|"
                		+ "Henry Koster|Ted Kotcheff|Mariusz Kotowski|Serguei Kouchnerov|Nikos Koundouros|Jan Kounen|Aaron Kozak|Ivan Kraljevic|Stanley Kramer|Kurt Kren|"
                		+ "Krishnan–Panju|William Kronick|Stanley Kubrick|George Kuchar|Lev Kuleshov|Roger Kumble|Zacharias Kunuk|Akira Kurosawa|Kiyoshi Kurosawa|"
                		+ "Emir Kusturica|Stanley Kwan|Ken Kwapis|John La Bouchardiere|Nadine Labaki|Neil LaBute|Gregory La Cava|Harry Lachman|Edward Laemmle|René Laloux|"
                		+ "Ringo Lam|Charles Lamont|Lew Landers|John Landis|Sidney Lanfield|Fritz Lang|Walter Lang|Rémi Lange|Doug Langway|Claude Lanzmann|Janez Lapajne|"
                		+ "Victoria Larimore|Lalet Bist|John Lasseter|Andrew Lau|Jeffrey Lau|Francis Lawrence|Marc Lawrence|J\\. F\\. Lawton|Tracie Laymon|David Lean|"
                		+ "Patrice Leconte|D\\. Ross Lederman|Ang Lee|Lee Cheol\\-ha|Lee Lik\\-Chi|Rowland V\\. Lee|Spike Lee|Michael Lehmann|Henry Lehrman|Mike Leigh|"
                		+ "Danny Leiner|Mitchell Leisen|Logan Leistikow|Claude Lelouch|Michael Lembeck|Umberto Lenzi|Robert Z\\. Leonard|Sergio Leone|Robert Lepage|"
                		+ "Mervyn LeRoy|George Lessey|Mark L\\. Lester|Richard Lester|Louis Leterrier|Jørgen Leth|Brian Levant|Henry Levin|Barry Levinson|Shawn Levy|"
                		+ "Jerry Lewis|Joseph H\\. Lewis|Li Han\\-Hsiang|Lee Tit|Li Yang|Doug Liman|Max Linder|Willy Lindwer|Richard Linklater|Miguel Littín|Anatole Litvak|"
                		+ "Luis Llosa|Frank Lloyd|Lo Wei|Ken Loach|Ulli Lommel|Robert Longo|Del Lord|Joseph Losey|Lou Ye|Arthur Lubin|Ernst Lubitsch|George Lucas|"
                		+ "Wilfred Lucas|Edward Ludwig|Baz Luhrmann|Sidney Lumet|Leopold Lummerstorfer|Ida Lupino|Rod Lurie|David Lynch|Jennifer Lynch|Liam Lynch|"
                		+ "Adrian Lyne|Jonathan Lynn|Ma–Mc[edit]|David MacDonald|Manish Jain|Kevin Macdonald|Carl Macek|Seth MacFarlane|Gustav Machatý|Willard Mack|"
                		+ "Alexander Mackendrick|David Mackenzie|Gillies MacKinnon|Murdock MacQuarrie|Guy Maddin|Madonna|Holger\\-Madsen|Ivan Magrin\\-Chagnolleau|Charles Maigne|"
                		+ "Norman Mailer|Alan Mak|Dušan Makavejev|Myjohnbritto Antivirus|Mohsen Makhmalbaf|Samira Makhmalbaf|Károly Makk|Sundeep Malani|Terrence Malick|"
                		+ "Louis Malle|William Malone|David Maloney|Leo D\\. Maloney|Henrik Malyan|Djibril Diop Mambéty|Manakis brothers|Milcho Manchevski|Chris Mandia|"
                		+ "Luis Mandoki|James Mangold|Joseph L\\. Mankiewicz|Anthony Mann|Daniel Mann|Delbert Mann|Michael Mann|Guy Manos|Sophie Marceau|Max Marcin|"
                		+ "Bam Margera|Edwin L\\. Marin|José Mojica Marins|Chris Marker|Richard Marquand|Laïla Marrakchi|Frank Marshall|Garry Marshall|George Marshall|"
                		+ "Neil Marshall|Penny Marshall|Marco Martins|Leslie H\\. Martinson|Andrew Marton|Nico Mastorakis|Yasuzo Masumura|Arūnas Matelis|Sean Mathias|"
                		+ "Elaine May|Archie Mayo|Brad Mays|Paul Mazursky|Jim McBride|Leo McCarey|Ray McCarey|Tom McCarthy|Martin McDonagh|Frank McDonald|Bernard McEveety|"
                		+ "Vincent McEveety|McG|William C\\. McGann|Scott McGehee|J\\.P\\. McGowan|Joseph McGrath|Paul McGuigan|John McKay|Lucky McKee|Norman McLaren|"
                		+ "Norman Z\\. McLeod|John McNaughton|Daniel McNicoll|John McTiernan|Shane Meadows|Dariush Mehrjui|Deepa Mehta|Gus Meins|Fernando Meirelles|"
                		+ "Adolfas Mekas|Jonas Mekas|Bill Melendez|George Melford|Wilco Melissant|Craig Melville|Jean\\-Pierre Melville|Lothar Mendes|Sam Mendes|"
                		+ "Jim Mendiola|Chris Menges|E\\. Elias Merhige|Saul Metzstein|Russ Meyer|Leah Meyerhoff|Georges Méliès|Oscar Micheaux|Roger Michell|Takashi Miike|"
                		+ "Nikita Mikhalkov|Lewis Milestone|John Milius|Frank Miller|George Miller|Kara Miller|Robert Ellis Miller|Sharron Miller|Anthony Minghella|"
                		+ "Rob Minkoff|Vincente Minnelli|Howard M\\. Mitchell|John Cameron Mitchell|Noël Mitrani|Hayao Miyazaki|Kenji Mizoguchi|Édouard Molinaro|"
                		+ "Momina Duraid|Mario Monicelli|Cesar Montano|Eduardo Montes\\-Bradley|Dave Moody|Lukas Moodysson|Michael Moore|Robert Moore|Stan Moore|"
                		+ "Jocelyn Moorhouse|Ken Mora|Jacobo Morales|Nanni Moretti|Sidney Morgan|Anders Morgenthaler|Errol Morris|Terry O\\. Morse|Cynthia Mort|"
                		+ "Edmund Mortimer|Greg Mottola|Allan Moyle|Otto Muehl|Russell Mulcahy|John Mulholland|Robert Mulligan|Kira Muratova|F\\. W\\. Murnau|"
                		+ "Dudley Murphy|Geoff Murphy|Ralph Murphy|Ryan Murphy|Floyd Mutrux|Daniel Myrick|Nadeem Baig|Amir Naderi|Mira Nair|Ilya Naishuller|Hideo Nakata|"
                		+ "Khodzha Kuli Narliyev|Mikio Naruse|Janusz Nasfeter|Percy Nash|Vincenzo Natali|Gregory Nava|Ronald Neame|Jean Negulesco|Marshall Neilan|"
                		+ "Roy William Neill|Gary Nelson|Gene Nelson|Ralph Nelson|Laura Neri|Max Neufeld|Kurt Neumann|Mike Newell|Sam Newfield|Don Newland|Joseph Newman|"
                		+ "Fred Newmeyer|Lionel Ngakane|Thuc Nguyen|Fred Niblo|Andrew Niccol|Mike Nichols|Jack Nicholson|William Nigh|Nikos Nikolaidis|"
                		+ "Leopoldo Torre Nilsson|Rob Nilsson|Leonard Nimoy|Marcus Nispel|David Nixon|Manfred Noa|Gaspar Noé|Christopher Nolan|Tom Noonan|Syed Noor|"
                		+ "Jehane Noujaim|Wilfred Noy|Phillip Noyce|Elliott Nugent|Bruno Nuytten|Chris O'Dea|Atsushi Ogata|Kingsley Ogoro|Izu Ojukwu|Kihachi Okamoto|"
                		+ "Sidney Olcott|Laurence Olivier|Jorge Olguín|Gunnar Olsson|Max Ophüls|Benjamin Orifici|Kenny Ortega|Mamoru Oshii|Nagisa Oshima|Richard Oswald|"
                		+ "Katsuhiro Otomo|Ulrike Ottinger|Idrissa Ouedraogo|Horace Ové|Frank Oz|François Ozon|Yasujirō Ozu|Georg Wilhelm Pabst|P\\. Padmarajan|"
                		+ "Alan J\\. Pakula|Pan Anzi|Jafar Panahi|Gleb Panfilov|Pang Ho\\-Cheung|Sergei Parajanov|K\\-Michel Parandi|Dean Parisot|Park Chan\\-Wook|Jerry Paris|"
                		+ "Nick Park|Alan Parker|Albert Parker|Trey Parker|James Parrott|Reza Parsa|Gabriel Pascal|Goran Paskaljevic|Pier Paolo Pasolini|Ivan Passer|"
                		+ "Stuart Paton|Anand Patwardhan|Alexander Payne|György Pálfi|Leslie Pearce|Richard Pearce|George Pearson|Raoul Peck|Sam Peckinpah|Scott Pembroke|"
                		+ "Arthur Penn|Sean Penn|Mark Pellington|Ivan Perestiani|Lester James Peries|Quincy Perkins|Léonce Perret|Tyler Perry|Christian Peschken|"
                		+ "Wolfgang Petersen|Daniel Petrie|Elio Petri|Maurice Pialat|Irving Pichel|Andy Picheta|Frank Pierson|Derek Pike|Lucian Pintilie|Pitof|"
                		+ "René Plaissetty|Ihor Podolchak|Roman Polanski|Mark Polish|Michael Polish|Rudolph Polk|Kay Pollak|Sydney Pollack|Harry A\\. Pollard|"
                		+ "Joshua Pomer|Gillo Pontecorvo|Ted Post|H\\. C\\. Potter|Sally Potter|Richard Pottier|Frank Powell|Michael Powell|Paul Powell|Rosa von Praunheim|"
                		+ "Otto Preminger|Emeric Pressburger|Michael Pressman|Sarah Price|Prince|Yakov Protazanov|Alex Proyas|Vsevolod Pudovkin|Jon Puno|Derek Purvis|"
                		+ "Ivan Pyryev|Ayoub Qanir|Brothers Quay|John Quigley|Richard Quine|Ra–Re[edit]|Peer Raben|Michael Radford|Bob Rafelson|Jeff Ragsdale|Sam Raimi|"
                		+ "Yvonne Rainer|Hossein Rajabian|Harold Ramis|Irving Rapper|Mani Ratnam|Brett Ratner|Gregory Ratoff|Shero Rauf|John Rawlins|Albert Ray|Man Ray|"
                		+ "Nicholas Ray|Rick Ray|Satyajit Ray|Herman C\\. Raymaker|Patrick Rea|Justin Reardon|Eric Red|Robert Redford|Carol Reed|Dee Rees|Keanu Reeves|"
                		+ "Matt Reeves|Nicolas Winding Refn|Godfrey Reggio|Kelly Reichardt|Carl Reiner|Rob Reiner|Max Reinhardt|Irving Reis|Charles Reisner|Karel Reisz|"
                		+ "Ivan Reitman|Jason Reitman|Edgar Reitz|Chris Renaud|Drew Renaud|Jean Renoir|Alain Resnais|Carlos Reygadas|Kevin Reynolds|Lynn Reynolds|John Rich|"
                		+ "Tony Richardson|Hans Richter|W\\. D\\. Richter|Leni Riefenstahl|Ransom Riggs|Arthur Ripley|Arturo Ripstein|Guy Ritchie|Michael Ritchie|Martin Ritt|"
                		+ "Fernand Rivers|Joan Rivers|Jacques Rivette|Saeed Rizvi|Jay Roach|Alain Robbe\\-Grillet|Brian Robbins|Jerome Robbins|Tim Robbins|Stephen Roberts|"
                		+ "John S\\. Robertson|Bruce Robinson|Lee Robinson|Phil Alden Robinson|Mark Robson|Glauber Rocha|Alexandre Rockwell|João Pedro Rodrigues|"
                		+ "Robert Rodriguez|John Roecker|Nicolas Roeg|Daniel Roemer|Albert S\\. Rogell|Éric Rohmer|Mark Romanek|George A\\. Romero|"
                		+ "Joaquín Luis Romero Marchent|Mikhail Romm|Am Rong|Mickey Rooney|Bernard Rose|Phil Rosen|Stuart Rosenberg|Rick Rosenthal|Mark Rosman|Herbert Ross|"
                		+ "Roberto Rossellini|Robert Rossen|Franco Rossi|Arthur Rosson|Richard Rosson|Eli Roth|Tim Roth|Roy Rowland|Patricia Rozema|Joseph Ruben|Alan Rudolph|"
                		+ "Wesley Ruggles|Raúl Ruiz|Pavel Ruminov|Richard Rush|Chuck Russell|David O\\. Russell|Ken Russell|Anthony and Joe Russo|Stefan Ruzowitzky|"
                		+ "Eldar Ryazanov|Zbigniew Rybczyński|Mark Rydell|Sathish Kalathil|Maher Sabry|Bob Saget|Abdulkadir Ahmed Said|Gene Saks|Sidney Salkow|Walter Salles|"
                		+ "Mikael Salomon|Anja Salomonowitz|Carlos Saldanha|Victor Salva|Shakti Samanta|Scott Sanders|Helma Sanders\\-Brahms|Mark Sandrich|Alfred Santell|"
                		+ "Joseph Santley|Richard C\\. Sarafian|Valeria Sarmiento|Peter Sasdy|Yūichi Satō|Hubert Sauper|Claude Sautet|Fred Savage|Philip Saville|Seth Savoy|"
                		+ "Donald Sawyer|Geoffrey Sax|John Sayles|John Scagliotti|Armand Schaefer|George Schaefer|Franklin Schaffner|Peter Schamoni|Paul Schattel|"
                		+ "Frank Scheffer|Fred Schepisi|Victor Schertzinger|Paul Scheuring|Kyle Schickner|Thomas Schlamme|Craig Schlattman|John Schlesinger|"
                		+ "Christoph Schlingensief|Volker Schlöndorff|Julian Schnabel|Dan Schneider|Ian Schneider|Rob Schneider|Ernest B\\. Schoedsack|Renen Schorr|"
                		+ "Paul Schrader|Rick Schroder|Barbet Schroeder|Werner Schroeter|Hugh Schulze|Michael Schultz|Joel Schumacher|Reinhold Schünzel|Rudolf Schwarzkogler|"
                		+ "Til Schweiger|John Schwert|David Schwimmer|Ettore Scola|Martin Scorsese|Ridley Scott|Shaun Scott|Tony Scott|Aubrey Scotto|Steven Seagal|"
                		+ "George Seaton|Edward Sedgwick|Alex Segal|Peter Segal|Susan Seidelman|Ulrich Seidl|Lewis Seiler|William A\\. Seiter|Franz Seitz, Sr\\.|George Seitz|"
                		+ "Steve Sekely|Lesley Selander|Henry Selick|Herbert Selpin|Aaron Seltzer|David Seltzer|Selvaraghavan|Edgar Selwyn|Ousmane Sembène|Larry Semon|"
                		+ "Mrinal Sen|Dominic Sena|Mack Sennett|Craig Serling|Tom Shadyac|S\\. Shankar|Ted Sharks|Jim Sharman|William Shatner|Melville Shavelson|Jenn Shaw|"
                		+ "Scott Shaw|Ron Shelton|Darren Shepherd|Larisa Shepitko|Jim Sheridan|Gary Sherman|George Sherman|Lowell Sherman|Vincent Sherman|Takashi Shimizu|"
                		+ "Alexandra Shiva|Daryush Shokof|Jack Sholder|Cate Shortland|M\\. Night Shyamalan|Charles Shyer|Ginny Stikeman|George Sidney|Scott Sidney|"
                		+ "David Siegel|Don Siegel|Pedro Sienna|Brad Silberling|Dean Silvers|S\\. Sylvan Simon|Yves Simoneau|Giorgio Simonelli|Bryan Singer|Manmohan Singh|"
                		+ "Tarsem Singh|John Singleton|Robert Siodmak|Puneet Sira|Douglas Sirk|Cheick Oumar Sissoko|Andrea Sisson|Chris Sivertson|Vilgot Sjöman|"
                		+ "Victor Sjöström|Jerzy Skolimowski|Paul Sloane|Edward Sloman|Yannis Smaragdis|Ralph Smart|Jack Smight|Brian Trenchard\\-Smith|Charles Martin Smith|"
                		+ "Gary Smith|George Albert Smith|Harry Everett Smith|Kevin Smith|Noel M\\. Smith|Seth Grahame\\-Smith|Adam Smoluk|Michael Snow|Zack Snyder|"
                		+ "Michele Soavi|Steven Soderbergh|Aleksandr Sokurov|Frances\\-Anne Solomon|Todd Solondz|Stephen Sommers|Barry Sonnenfeld|Shion Sono|Tressie Souders|"
                		+ "Jaap Speyer|Penelope Spheeris|Steven Spielberg|Götz Spielmann|Roger Spottiswoode|Alejandro Springall|R\\. G\\. Springsteen|John M\\. Stahl|"
                		+ "Sylvester Stallone|Andrew Stanton|Ladislas Starevich|Ralph Staub|Malcolm St\\. Clair|J\\.A\\. Steel|Paul L\\. Stein|Andrew Stevens|George Stevens|"
                		+ "Robert Stevenson|Gordon Stewart|Ben Stiller|Mauritz Stiller|Whit Stillman|Francis Stokes|Marcus Stokes|Benjamin Stoloff|Andrew L\\. Stone|"
                		+ "Matt Stone|Oliver Stone|Jerome Storm|Jean\\-Marie Straub|Frank R\\. Strayer|Suresh Joachim|Amanda Street|Graham Streeter|Barbra Streisand|"
                		+ "James Strong|Mel Stuart|John Sturges|Preston Sturges|K\\. Subash|Arne Sucksdorff|Elia Suleiman|Sun Yu|A\\. Edward Sutherland|Seijun Suzuki|"
                		+ "Jan Švankmajer|Harry Sweet|Justin Swibel|Hans\\-Jürgen Syberberg|Khady Sylla|István Szabó|Peter Szewczyk|Tina Takemoto|Rachel Talalay|Patrick Tam|"
                		+ "Lee Tamahori|Alain Tanner|Danis Tanovic|Quentin Tarantino|Andrei Tarkovsky|Béla Tarr|Genndy Tartakovsky|Frank Tashlin|Jacques Tati|Norman Taurog|"
                		+ "Bertrand Tavernier|Alan Taylor|Don Taylor|Ray Taylor|Sam Taylor|Stanner E\\.V\\. Taylor|William Desmond Taylor|Lewis Teague|André Téchiné|"
                		+ "Julien Temple|Suzie Templeton|Andy Tennant|George Terwilliger|Duccio Tessari|Ted Tetzlaff|Brandon Thaxton|Wilhelm Thiele|John G\\. Thomas|"
                		+ "Ralph Thomas|J\\. Lee Thompson|Robert Thornby|Billy Bob Thornton|Richard Thorpe|Tian Zhuangzhuang|George Tillman, Jr\\.|Constance Tillotson|"
                		+ "Mihai Timofti|James Tinling|Johnnie To|James Toback|Norman Tokar|Giuseppe Tornatore|Miguel Contreras Torres|Ivan Tors|André de Toth|"
                		+ "Laurent Touil\\-Tartour|Viktor Tourjansky|Jacques Tourneur|Maurice Tourneur|Wendy Toye|Josh Trank|David Trainer|Pablo Trapero|Scott Treleaven|"
                		+ "Jeff Tremaine|Nadine Trintignant|Jan Troell|François Truffaut|Ming\\-liang Tsai|Peter Tscherkassky|Tsui Hark|Shinya Tsukamoto|Stanley Tucci|"
                		+ "Jon Turteltaub|Frank Tuttle|David Twohy|Chris Twomey|Tom Tykwer|George Tzavellas|Edgar G\\. Ulmer|Ron Underwood|Upendra|Urszula Urbaniak|"
                		+ "Kinka Usher|Peter Ustinov|Roger Vadim|Ladislao Vajda|Luis Valdez|Mike Valerio|W\\. S\\. Van Dyke|Vanelle|Andre van Heerden|Erik Van Looy|"
                		+ "Melvin Van Peebles|Gus Van Sant|Norman Thaddeus Vane|Agnès Varda|Ram Gopal Varma|Marcel Varnel|Petar B\\. Vasilev|Matthew Vaughn|Pam Veasey|"
                		+ "Perry N\\. Vekroff|Gore Verbinski|Paul Verhoeven|Dziga Vertov|Charles Vidor|King Vidor|Ric Viers|Berthold Viertel|Denis Villeneuve|"
                		+ "Robert G\\. Vignola|Jean Vigo|Vijayakrishnan|Agusti Villaronga|Thomas Vinterberg|Luchino Visconti|Biju Viswanath|Kasinadhuni Viswanath|"
                		+ "Erik Voake|Jean\\-Marc Vallée|Géza von Bolváry|Géza von Cziffra|Josef von Sternberg|Erich von Stroheim|Lars von Trier|Margarethe von Trotta|"
                		+ "Bernard Vorhaus|Slavko Vorkapić|Kurt Voss|Jurgen Vsych|The Wachowskis|Michael Wadleigh|Wai Ka\\-Fai|Andrzej Wajda|Stuart Walker|David Wall|"
                		+ "Randall Wallace|Richard Wallace|Tom Walls|Raoul Walsh|Charles Walters|Wan brothers|James Wan|Sam Wanamaker|Wang Quan'an|Wang Xiaoshuai|"
                		+ "Wayne Wang|Albert Ward|David S\\. Ward|Vincent Ward|Andy Warhol|Denzel Washington|Darrell Wasyk|John Waters|Peter Watkins|"
                		+ "Keenen Ivory Wayans|Sean Weathers|Kenneth Webb|Marc Webb|Millard Webb|Lois Weber|Chris Wedge|Apichatpong Weerasethakul|Peter Weibel|"
                		+ "Hans Weingartner|Peter Weir|Chris Weitz|Paul Weitz|Orson Welles|Arthur Wellin|William Wellman|Wim Wenders|Alfred L\\. Werker|Lina Wertmüller|"
                		+ "Roland West|Simon West|James Whale|Leopold Wharton|Joyce Wieland|Theodore Wharton|Joss Whedon|Tim Whelan|Richard Whorf|Kanchi Wichmann|"
                		+ "Bernhard Wicki|Bo Widerberg|Virgil Widrich|Erin Wiedner|Robert Wiene|Crane Wilbur|Herbert Wilcox|Cornel Wilde|Billy Wilder|Gene Wilder|"
                		+ "W\\. Lee Wilder|Gordon Wiles|Diane Wilkins|Irvin Willat|Adim Williams|Paul Andrew Williams|Richard Williams|Dennis Willis|Rex Wilson|"
                		+ "Simon Wincer|Bretaigne Windust|Henry Winkler|Irwin Winkler|Michael Winner|David Winning|Michael Winterbottom|Robert Wise|Tommy Wiseau|"
                		+ "Frederick Wiseman|Len Wiseman|Doris Wishman|Chester Withey|William Witney|James Wong|Wong Jing|Wong Kar\\-wai|John Woo|Andrés Wood|Ed Wood|"
                		+ "Sam Wood|Arthur B\\. Woods|John Griffith Wray|Edgar Wright|Jay Wright|Mack V\\. Wright|Rupert Wyatt|William Wyler|Xie Jin|Boaz Yakin|"
                		+ "Edward Yang|Ruby Yang|Jean Yarbrough|David Yates|Hal Yates|Peter Yates|Derek Yee|Lev Yilmaz|Wilson Yip|Yaky Yosha|Yamada Youji|Harold Young|"
                		+ "James Young|Terence Young|Yuen Woo\\-ping|Brian Yuzna|Yugander V\\.V\\.|Eduard Zahariev|Krzysztof Zanussi|Franco Zeffirelli|Alfred Zeisler|"
                		+ "Frederic Zelnik|Robert Zemeckis|David Zennie)", attributeNames, label);
                RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
                regexMatcher.setInputOperator(lastOperator);
                lastOperator = regexMatcher; 
        	}
        }
        
        
        RegexPredicate regexPredicate = 
        		new RegexPredicate(regex, 
        				attributeNames, "report");
        
        RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
        //      regexMatcher.setInputOperator(dictionaryMatcher);
        regexMatcher.setInputOperator(lastOperator);
        
        TupleSink tupleSink = new TupleSink();
        //      tupleSink.setInputOperator(regexMatcher);
        tupleSink.setInputOperator(regexMatcher);

        long startMatchTime = System.currentTimeMillis();
        tupleSink.open();
        List<Tuple> result = tupleSink.collectAllTuples();
        tupleSink.close();
        long endMatchTime = System.currentTimeMillis();
        double matchTime = (endMatchTime - startMatchTime) / 1000.0;

//        for(Tuple t: result){
//        	System.out.println(t.getField(0).getValue().toString());
//        	System.out.println(t.getField("abstract").toString());
//        	for(Span span: (List<Span>) t.getField("report").getValue()){
//        		System.out.println(span.getAttributeName() + " " + span.getStart() + " " + span.getEnd() + " " + span.getValue());
//        	}
//        	//         System.out.println("This is for another dictionary matcher");
//        	//          for(Span span: (List<Span>) t.getField(disease).getValue()){
//        	//              System.out.println(span.getAttributeName() + " " + span.getStart() + " " + span.getEnd() + " " + span.getValue());
//        	//          }
//        }
        int count = result.size();
//        System.out.println("================================================");
        System.out.println(regex + "####" + count + "####" + matchTime );
//        System.out.println("Done_number of tuples" + count);
//        System.out.println("Total matching time: " + matchTime);
    }
    
    public static void SCAN_REGEX_SINK(String regex) throws Exception {
    	ScanSourcePredicate scanSourcePredicate = new ScanSourcePredicate(WIKIPEDIA_SAMPLE_TABLE);
        ScanBasedSourceOperator scanBasedSourceOperator = new ScanBasedSourceOperator(scanSourcePredicate);
        List<String> attributeNames = Arrays.asList(WikipediaIndexWriter.TEXT);
    
        RelationManager relationManager = RelationManager.getInstance();
        String luceneAnalyzerStr = relationManager.getTableAnalyzerString(WIKIPEDIA_SAMPLE_TABLE);
        
        RegexPredicate regexPredicate = 
        		new RegexPredicate(regex, 
        				attributeNames, "report");
        
        RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
        //      regexMatcher.setInputOperator(dictionaryMatcher);
        regexMatcher.setInputOperator(scanBasedSourceOperator);


        TupleSink tupleSink = new TupleSink();
        //      tupleSink.setInputOperator(regexMatcher);
        tupleSink.setInputOperator(regexMatcher);

        long startMatchTime = System.currentTimeMillis();
        tupleSink.open();
        List<Tuple> result = tupleSink.collectAllTuples();
        tupleSink.close();
        long endMatchTime = System.currentTimeMillis();
        double matchTime = (endMatchTime - startMatchTime) / 1000.0;

//        for(Tuple t: result){
//        	System.out.println(t.getField(0).getValue().toString());
//        	System.out.println(t.getField("abstract").toString());
//        	for(Span span: (List<Span>) t.getField("report").getValue()){
//        		System.out.println(span.getAttributeName() + " " + span.getStart() + " " + span.getEnd() + " " + span.getValue());
//        	}
//        	//         System.out.println("This is for another dictionary matcher");
//        	//          for(Span span: (List<Span>) t.getField(disease).getValue()){
//        	//              System.out.println(span.getAttributeName() + " " + span.getStart() + " " + span.getEnd() + " " + span.getValue());
//        	//          }
//        }
        int count = result.size();
//        System.out.println("================================================");
        System.out.println(regex + "####" + count + "####" + matchTime );
//        System.out.println("Done_number of tuples" + count);
//        System.out.println("Total matching time: " + matchTime);
    }
    
    
    public static void extractDrugsDiseases() throws Exception {
    	
    	ScanSourcePredicate scanSourcePredicate = new ScanSourcePredicate(WIKIPEDIA_SAMPLE_TABLE);
        ScanBasedSourceOperator scanBasedSourceOperator = new ScanBasedSourceOperator(scanSourcePredicate);
        List<String> attributeNames = Arrays.asList(MedlineIndexWriter.ABSTRACT);
        
        String drug = "drug";
        String disease = "disease";
        
        RelationManager relationManager = RelationManager.getInstance();
        String luceneAnalyzerStr = relationManager.getTableAnalyzerString(WIKIPEDIA_SAMPLE_TABLE);
        
        String dic_drugs_path = PerfTestUtils.getResourcePath("/dictionary/drugs_dict.txt").toString();
        List<String> list_drugs = tokenizeFile(dic_drugs_path);
        
        String dic_disease_path = PerfTestUtils.getResourcePath("/dictionary/disease_dict.txt").toString();
        List<String> list_disease = tokenizeFile(dic_disease_path);
        
        edu.uci.ics.texera.dataflow.dictionarymatcher.Dictionary dic_drugs = 
        		new edu.uci.ics.texera.dataflow.dictionarymatcher.Dictionary(list_drugs);
        edu.uci.ics.texera.dataflow.dictionarymatcher.Dictionary dic_disease = 
        		new edu.uci.ics.texera.dataflow.dictionarymatcher.Dictionary(list_disease);
        
        System.out.println("size of drugs dictionary" + dic_drugs.getDictionaryEntries().size());
        System.out.println(" size of disease dictionary" + dic_disease.getDictionaryEntries().size());
        
        
//        DictionaryPredicate dictionaryPredicate = 
//        		new DictionaryPredicate(dic_drugs, attributeNames, luceneAnalyzerStr, 
//        				KeywordMatchingType.SUBSTRING_SCANBASED, drug);
//        DictionaryMatcher dictionaryMatcher = new DictionaryMatcher(dictionaryPredicate);
//        
//        dictionaryMatcher.setInputOperator(scanBasedSourceOperator);
        
//        DictionaryPredicate dictionaryPredicate1 = 
//        		new DictionaryPredicate(dic_disease, attributeNames, luceneAnalyzerStr, 
//        				KeywordMatchingType.SUBSTRING_SCANBASED, disease);
//        DictionaryMatcher dictionaryMatcher1 = new DictionaryMatcher(dictionaryPredicate1);
//        
//        dictionaryMatcher1.setInputOperator(dictionaryMatcher);

//        RegexPredicate regexPredicate = 
//        		new RegexPredicate("(In )?\\d+ cases?(?: of)?(?: transplantation),? *(?:the recipient was made)", 
//        				attributeNames, "report");
        RegexPredicate regexPredicate = 
        		new RegexPredicate("((taking|injecting|injections?|usage|using|dose|dosage|prescriptions?|prescribing)( of)?( the)? "
        				+ "(?:[A-Za-z0-9]+ *){1,3}"
        				+ " (injection|tablets?|inhalation|vaccine|capsules?|inhibitors?|powder|gel|cream|oinment)).*,", 
        				attributeNames, "report");
//        RegexPredicate regexPredicate = 
//        		new RegexPredicate("\\s*(abcd|pqrs)(word)?([A-Z]\\w){1,2}(?:[A-Za-z0-9]+ *){1,3}\\s*hello", 
//        				attributeNames, "report");
//        RegexPredicate regexPredicate = 
//        		new RegexPredicate("a.*b",
//        				attributeNames, "report");
        

//        RegexPredicate regexPredicate = 
//        		new RegexPredicate("(taking|injecting|injections?|usage|using|dose|dosage|prescriptions?|prescribing)( of)?( the)? "
//        				+ "<drug>"
//        				+ "( )?(injection|tablets?|inhalation|vaccine|capsules?|inhibitors?|powder|gel|cream|oinment)?( )?(can|could|will|would|should)? (cures?|heals?|solves?|resolves?|improves?|reduces?|impacts?|helps?)( the)? "
//        				+ "<disease>"
//        				+ "( )?(syndrome|infection|sickness|deficiency|disorder|defects?|disease|problems?)? (successfully|partially|positively|negatively|completely|gradually|quickly|significantly|slowly)", 
//        				attributeNames, "report");
//        RegexPredicate regexPredicate = 
//        		new RegexPredicate("(The )?surgeon (involved|involving|involves?) <drug> willing to <disease>( of)? their ovarian tissue.", 
//        				attributeNames, "report");
        RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
//        regexMatcher.setInputOperator(dictionaryMatcher);
        regexMatcher.setInputOperator(scanBasedSourceOperator);
        
        
        TupleSink tupleSink = new TupleSink();
//        tupleSink.setInputOperator(regexMatcher);
        tupleSink.setInputOperator(regexMatcher);
        
        long startMatchTime = System.currentTimeMillis();
        tupleSink.open();
        List<Tuple> result = tupleSink.collectAllTuples();
        tupleSink.close();
        long endMatchTime = System.currentTimeMillis();
        double matchTime = (endMatchTime - startMatchTime) / 1000.0;
        
        for(Tuple t: result){
            System.out.println(t.getField("abstract").toString());
            for(Span span: (List<Span>) t.getField("report").getValue()){
                System.out.println(span.getAttributeName() + " " + span.getStart() + " " + span.getEnd() + " " + span.getValue());
            }
//           System.out.println("This is for another dictionary matcher");
//            for(Span span: (List<Span>) t.getField(disease).getValue()){
//                System.out.println(span.getAttributeName() + " " + span.getStart() + " " + span.getEnd() + " " + span.getValue());
//            }
        }
        int count = result.size();
        System.out.println("Done_number of tuples" + count);
        System.out.println("Total matching time: " + matchTime);
        
    }
    public static void extractPersonLocation() throws Exception {
    //    ScanSourcePredicate scanSourcePredicate = new ScanSourcePredicate(PEOPLE_TABLE);
     //   ScanBasedSourceOperator scanBasedSourceOperator = new ScanBasedSourceOperator(scanSourcePredicate);
        ScanSourcePredicate scanSourcePredicate = new ScanSourcePredicate(WIKIPEDIA_SAMPLE_TABLE);
        ScanBasedSourceOperator scanBasedSourceOperator = new ScanBasedSourceOperator(scanSourcePredicate);
        List<String> attributeNames = Arrays.asList(MedlineIndexWriter.ABSTRACT);
        String org = "organization";
        String per = "person";
        RelationManager relationManager = RelationManager.getInstance();
        String luceneAnalyzerStr = relationManager.getTableAnalyzerString(WIKIPEDIA_SAMPLE_TABLE);
        String dic_org_path = PerfTestUtils.getResourcePath("/dictionary/dic_noun.txt").toString();
        List<String> list_org = tokenizeFile(dic_org_path);
       // List<String> list_org = new ArrayList<>();
        String dic_per_path = PerfTestUtils.getResourcePath("/dictionary/adj_dic.txt").toString();
        List<String> list_per = tokenizeFile(dic_per_path);
        edu.uci.ics.texera.dataflow.dictionarymatcher.Dictionary dic_per = 
        		new edu.uci.ics.texera.dataflow.dictionarymatcher.Dictionary(list_per);
        edu.uci.ics.texera.dataflow.dictionarymatcher.Dictionary dic_org = 
        		new edu.uci.ics.texera.dataflow.dictionarymatcher.Dictionary(list_org);
        System.out.println("size of organization" + dic_org.getDictionaryEntries().size());
        System.out.println(" size of person" + dic_per.getDictionaryEntries().size());
        DictionaryPredicate dictionaryPredicate = new DictionaryPredicate(dic_org, attributeNames, luceneAnalyzerStr, KeywordMatchingType.SUBSTRING_SCANBASED, org);
        DictionaryMatcher dictionaryMatcher = new DictionaryMatcher(dictionaryPredicate);
        dictionaryMatcher.setInputOperator(scanBasedSourceOperator);
        DictionaryPredicate dictionaryPredicate1 = new DictionaryPredicate(dic_per, attributeNames, luceneAnalyzerStr, KeywordMatchingType.SUBSTRING_SCANBASED, per);
        DictionaryMatcher dictionaryMatcher1 = new DictionaryMatcher(dictionaryPredicate1);
        dictionaryMatcher1.setInputOperator(dictionaryMatcher);
        String name = "locationspanresult";
     //   NlpEntityPredicate nlpEntityPredicate = new NlpEntityPredicate(NlpEntityType.ADJECTIVE, Arrays.asList(MedlineIndexWriter.ABSTRACT), name);
      //  NlpEntityOperator nlpEntityOperator = new NlpEntityOperator(nlpEntityPredicate);
        //SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy-HH-mm-ss");
        //FileSink fileSink = new FileSink(
          //      new File(sampleDataFilesDirectory + "/person-location-result-"
            //            + sdf.format(new Date(System.currentTimeMillis())).toString() + ".txt"));

        //fileSink.setToStringFunction((tuple -> DataflowUtils.getTupleString(tuple)));
    //    nlpEntityOperator.setInputOperator(scanBasedSourceOperator);
       // fileSink.setInputOperator(nlpEntityOperator);
       // Plan extractPersonPlan = new Plan(fileSink);
       // Engine.getEngine().evaluate(extractPersonPlan);
        TupleSink tupleSink = new TupleSink();
        tupleSink.setInputOperator(dictionaryMatcher1);
        tupleSink.open();
        List<Tuple> result = tupleSink.collectAllTuples();
        for(Tuple t: result){
            System.out.println(t.getField("abstract").toString());
            for(Span span: (List<Span>) t.getField(org).getValue()){
                System.out.println(span.getAttributeName() + " " + span.getStart() + " " + span.getEnd() + " " + span.getValue());
            }
           System.out.println("This is for another dictionary matcher");
            for(Span span: (List<Span>) t.getField(per).getValue()){
                System.out.println(span.getAttributeName() + " " + span.getStart() + " " + span.getEnd() + " " + span.getValue());
            }
        }
        int count = result.size();
//        List<String> results = tupleSink.collectAttributes(name);
//        FileWriter writer = new FileWriter("adj_dic.txt");
//        int cnt = 0;
//        for(String str: results) {
//            writer.write(str);
//            cnt += 1;
//            writer.write(", ");
//        }
//        writer.close();
        System.out.println("Done_number of tuples" + count);
     //   System.out.print("done_number of dictionary element" + cnt);

    }
    public static List<String> tokenizeFile(String inputfile) {
        List<String> tokenlist = new ArrayList<String>();
        try {
            File file = new File(inputfile);
            FileReader in = new FileReader(file);
            BufferedReader reader = new BufferedReader(in);
            String tmp = null;
            tmp = reader.readLine();
            int count = 1000000;
            while (tmp !=null) {
                String tokenstring = tmp.trim();
                if (tokenstring.contains("[^a-zA-Z0-9' ]")){
                    continue;
                }
                //String change = tokenstring.replaceAll(",", " ").toLowerCase();
                String[] a =tokenstring.trim().split(",");
                List<String> temp = Arrays.asList(a);
                temp.stream().forEach(s -> tokenlist.add(s.trim()));
                if (tokenlist.size() >= count) break;
                //tokenlist.addAll(temp);
                tmp = reader.readLine();
            }
            reader.close();
            in.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return tokenlist;


    }




}
