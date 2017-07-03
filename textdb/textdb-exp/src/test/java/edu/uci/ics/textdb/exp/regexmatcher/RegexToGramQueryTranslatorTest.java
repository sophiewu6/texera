package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.itextpdf.text.log.SysoCounter;

/**
 * @author Shuying Lai
 * @author Zuozhi Wang
 */

public class RegexToGramQueryTranslatorTest {

    /*
     * We need to check equivalence of two trees, but two equivalent trees could
     * have many different forms. The equals function in GramBooleanQuery only
     * compares two trees shallowly, it returns true if two trees' form (and
     * content) are identical.
     * 
     * So we transform the tree to DNF form, and apply simplifications to remove
     * redundant nodes. After transformation and simplification, two equivalent
     * trees should have identical form. Then we can use the equals() function
     * two check equivalence.
     * 
     */

    // Helper function to print query tree for debugging purposes.
    private void printTranslatorResult(String regex) {
        boolean DEBUG = false;
        if (!DEBUG) {
            return;
        }

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex,
                TranslatorUtils.DEFAULT_GRAM_LENGTH);

        System.out.println();
        System.out.println("----------------------------");

        System.out.println("regex: " + regex);
        System.out.println("boolean expression: " + exactQuery.getLuceneQueryString());
        System.out.println();

        // System.out.println("original query tree: ");
        // System.out.println(exactQuery.printQueryTree());
        //
        // System.out.println("DNF: ");
        // System.out.println(dnf.printQueryTree());

        System.out.println("Simplified DNF: ");
        System.out.println(exactQuery.printQueryTree());

        System.out.println("----------------------------");
        System.out.println();
    }

    class LeafNode{
    	public String leafValue;
    	int position;
    	int groupId;
    	public LeafNode(String l, int p, int g) {
			leafValue = l;
			position = p;
			groupId = g;
		}
    }
    // Helper function to transform a list of strings to a list of Leaf Node
    private List<GramBooleanQuery> getLeafNodeList(LeafNode... leafArray) {
        return Arrays.asList(leafArray).stream().map(x -> GramBooleanQuery.newLeafNode(x.leafValue, x.position, x.groupId))
                .collect(Collectors.toList());
    }

    @Test
    public void testEmptyRegex() {
        String regex = "";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);

        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testStarRegex() {
        String regex = "a*";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);

        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testLiteral1() {
        String regex = "abc";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.LEAF);
        expectedQuery.groupId = expectedQuery.positionIndex = 0;
        expectedQuery.leaf = "abc";

        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    // "ab" can't form a gram(default length 3), so the result is an empty OR
    // node.
    @Test
    public void testLiteral2() {
        String regex = "ab";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);

        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testLiteral3() {
        String regex = "abcd";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);

        expectedQuery.subQuerySet.addAll(getLeafNodeList(
        		new LeafNode("abc", 0, 0), 
        		new LeafNode("bcd", 1, 0)));
        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testLiteral4() {
        String regex = "ucirvine";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);

        expectedQuery.subQuerySet.addAll(getLeafNodeList(
        		new LeafNode("cir", 1, 0), 
        		new LeafNode("irv", 2, 0), 
        		new LeafNode("vin", 4, 0), 
        		new LeafNode("uci", 0, 0),
        		new LeafNode("ine", 5, 0),
        		new LeafNode("rvi", 3, 0)));

        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testLiteral5() {
        String regex = "textdb";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);

        expectedQuery.subQuerySet.addAll(getLeafNodeList(
        		new LeafNode("xtd", 2, 0), 
        		new LeafNode("tex", 0, 0), 
        		new LeafNode("ext", 1, 0),
        		new LeafNode("tdb", 3, 0)));

        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testCharClass1() {
        String regex = "[a-b][c-d][e-f]";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);

        expectedQuery.subQuerySet.addAll(getLeafNodeList(
        		new LeafNode("acf", 0, 1), 
        		new LeafNode("bde", 0, 6), 
        		new LeafNode("ade", 0, 2),
        		new LeafNode("adf", 0, 3), 
        		new LeafNode("bce", 0, 4), 
        		new LeafNode("bcf", 0, 5),
        		new LeafNode("ace", 0, 0),
        		new LeafNode("bdf", 0, 7)));
        
        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testAlternate1() {
        String regex = "uci|ics";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        expectedQuery.subQuerySet.addAll(getLeafNodeList(
        		new LeafNode("ics", 0, 0),
        		new LeafNode("uci", 0, 1)));

        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testAlternate2() {
        String regex = "data*(bcd|pqr)";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);


        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testPlus1() {
        String regex = "abc+";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);

        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testPlus2() {
        String regex = "abc+pqr+";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
       
        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        
        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testQuest1() {
        String regex = "abc?";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);

        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testQuest2() {
        String regex = "abc?pqr?";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);

        GramBooleanQuery expectedAnd1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        expectedQuery.subQuerySet.add(expectedAnd1);
        expectedAnd1.subQuerySet.addAll(getLeafNodeList(
        		new LeafNode("bcp", 1, 0),
        		new LeafNode("cpq", 2, 0),
        		new LeafNode("abc", 0, 0)));

        GramBooleanQuery expectedAnd2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        expectedQuery.subQuerySet.add(expectedAnd2);
        expectedAnd2.subQuerySet.addAll(getLeafNodeList(
        		new LeafNode("abp", 0, 1),
        		new LeafNode("bpq", 1, 1)));
        
        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    // RE2J will simplify REPEAT to equivalent form with QUEST.
    // abc{1,3} will be simplified to abcc?c?
    public void testRepeat1() {
        String regex = "abc{1,3}";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);

        GramBooleanQuery expectedAnd1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        expectedQuery.subQuerySet.add(expectedAnd1);
        expectedAnd1.subQuerySet.addAll(getLeafNodeList(
        		new LeafNode("bcc", 1, 1),
        		new LeafNode("abc", 0, 1)));

        GramBooleanQuery expectedAnd2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        expectedQuery.subQuerySet.add(expectedAnd2);
        expectedAnd2.subQuerySet.addAll(getLeafNodeList(
        		new LeafNode("abc", 0, 2),
        		new LeafNode("bcc", 1, 2),
        		new LeafNode("ccc", 2, 2)));

        expectedQuery.subQuerySet.addAll(getLeafNodeList(
        		new LeafNode("abc", 0, 0)));

        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testCapture1() {
        String regex = "(abc)(qwer)";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);

        expectedQuery.subQuerySet.addAll(getLeafNodeList(
        		new LeafNode("qwe", 0, 0),
        		new LeafNode("wer", 1, 0)));

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testRegexCropUrl() {
        String regex = "^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);

        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

}