package edu.uci.ics.textdb.perftest.runme;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.perftest.keywordmatcher.*;
import edu.uci.ics.textdb.perftest.nlpextractor.NlpExtractorPerformanceTest;
import edu.uci.ics.textdb.perftest.regexmatcher.RegexMatcherPerformanceTest;
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.StorageException;
import edu.uci.ics.textdb.perftest.dictionarymatcher.*;
import edu.uci.ics.textdb.perftest.fuzzytokenmatcher.*;

/**
 * @author Hailey Pan
 */
public class RunPerftests {

    /**
     * Run all performance tests.
     * 
     * 
     * Passed in below arguments: file folder path (where data set stored)
     * result folder path (where performance test results stored) standard index
     * folder path (where standard index stored) trigram index folder path
     * (where trigram index stored) queries folder path (where query files
     * stored)
     * 
     * If above arguments are not passed in, default paths will be used (refer
     * to PerfTestUtils.java) If some of the arguments are not applicable,
     * define them as empty string.
     * 
     * Make necessary changes for arguments, such as query file name, threshold
     * list, and regexQueries
     *
     */
    public static void main(String[] args) {
        try {
            PerfTestUtils.setResultFolder(args[0]);
            PerfTestUtils.setStandardIndexFolder(args[1]);
            PerfTestUtils.setTrigramIndexFolder(args[2]);
            PerfTestUtils.setQueryFolder(args[3]);
        } catch (ArrayIndexOutOfBoundsException e){
            System.out.println("missing arguments will be set to default");
        }

        try {
            List<Double> thresholds = Arrays.asList(0.8, 0.65, 0.5, 0.35);
            List<String> regexQueries = Arrays.asList(
            		"(cures|helps|treats) ([A-Za-z'-]+\\s*){1,2} (disorder|condition|illness|syndrome)", 
            		"(cases?) (of) ([A-Za-z'-]+\\s*){1,2} (transplantation)",
            		"[A-Za-z]+ (treatment)",
            		"[A-Za-z]+ (injections?) (of) [A-Za-z]+ (drug)",
            		"(the) [A-Za-z]* (test|experiment) (show(s|ed))",
            		"(severe) (cases?) (of) [A-Za-z']+ (syndrome)",
            		"(considerably|very|much|so) ([A-Za-z]*er) (than)",
            		"(advanced|severe|intense) ([A-Za-z'-]+\\s*){1,2} (disorders?|syndromes?|conditions?|diseases?)",
            		"\\d{1,2}% (of) (cases|patients|recipients)",
            		"[A-Za-z]+therapy",
            		"(caused) (by) ([A-Za-z'-]+\\s*){1,3} (virus) (infection)");
            List<String> dictionary1 = Arrays.asList(
            		"phrase num1","phrase num2",
            		"phrase num3");
            List<String> dictionary2 = Arrays.asList(
            		"phrase num1","phrase num2",
            		"phrase num3");

//            KeywordMatcherPerformanceTest.runTest("sample_queries.txt");
//            DictionaryMatcherPerformanceTest.runTest("sample_queries.txt");
//            FuzzyTokenMatcherPerformanceTest.runTest("sample_queries.txt", thresholds);
//            RegexMatcherPerformanceTest.runTest(regexQueries);
            RegexMatcherPerformanceTest.runSelectivityTest(regexQueries, dictionary1, dictionary2);
//            NlpExtractorPerformanceTest.runTest();

        } catch (StorageException | DataFlowException | IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
}