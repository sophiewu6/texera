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

    class LeafNodeStruct{
    	public String leaf;
    	int pos;
    	int groupId;
    	public LeafNodeStruct(String l, int p, int g) {
			leaf = l;
			pos = p;
			groupId = g;
		}
    }
    // Helper function to transform a list of strings to a list of Leaf Node
    private List<GramBooleanQuery> getLeafNodeList(LeafNodeStruct... leafStringArray) {
        return Arrays.asList(leafStringArray).stream().map(x -> GramBooleanQuery.newLeafNode(x.leaf, x.pos, x.groupId))
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

        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);

        expectedQuery.subQuerySet.addAll(getLeafNodeList(new LeafNodeStruct("abc", -1, -1), new LeafNodeStruct("abc", 0, 0)));

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
        		new LeafNodeStruct("abc", -1, -1), 
        		new LeafNodeStruct("abc", 0, 0), 
        		new LeafNodeStruct("bcd", 1, 0), 
        		new LeafNodeStruct("bcd", -1, -1)));
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
        		new LeafNodeStruct("cir", 1, 0), 
        		new LeafNodeStruct("irv", 2, 0), 
        		new LeafNodeStruct("vin", 4, 0), 
        		new LeafNodeStruct("uci", -1, -1),
        		new LeafNodeStruct("uci", 0, 0),
        		new LeafNodeStruct("ine", 5, 0),
        		new LeafNodeStruct("rvi", 3, 0),
        		new LeafNodeStruct("ine", -1, -1)));

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
        		new LeafNodeStruct("xtd", 2, 0), 
        		new LeafNodeStruct("tex", -1, -1), 
        		new LeafNodeStruct("tex", 0, 0), 
        		new LeafNodeStruct("ext", 1, 0),
        		new LeafNodeStruct("tdb", 3, 0),
        		new LeafNodeStruct("tdb", -1, -1)));

        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testCharClass1() {
        String regex = "[a-b][c-d][e-f]";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);

        String expectedQueryStr = "((bdf AND acf) OR (bcf AND bdf) OR (bde AND ade) OR "
        		+ "(bce AND bdf) OR (ade AND bdf) OR (bce AND ade) OR (bcf AND ade) OR "
        		+ "(ade AND ace) OR (bdf AND bde) OR (bde AND bde) OR (bdf AND adf) OR "
        		+ "(ace AND ace) OR (bdf AND bdf) OR (bde AND ace) OR (bcf AND bce) OR "
        		+ "(bde AND bdf) OR (bce AND ade) OR (bdf AND bcf) OR (bcf AND bde) OR "
        		+ "(acf AND bde) OR (bce AND bde) OR (acf AND ade) OR (acf AND ace) OR "
        		+ "(bde AND ace) OR (bcf AND ace) OR (adf AND ade) OR (acf AND bcf) OR "
        		+ "(bdf AND ade) OR (acf AND adf) OR (bcf AND bcf) OR (bce AND ace) OR "
        		+ "(ade AND bde) OR (adf AND bde) OR (bdf AND bce) OR (acf AND bde) OR "
        		+ "(adf AND ace) OR (bcf AND adf) OR (ade AND ade) OR (bcf AND adf) OR "
        		+ "(bcf AND ace) OR (acf AND adf) OR (bde AND adf) OR (acf AND ade) OR "
        		+ "(adf AND bdf) OR (acf AND ace) OR (ade AND adf) OR (bce AND acf) OR "
        		+ "(bcf AND acf) OR (bcf AND bde) OR (bdf AND ace) OR (bce AND adf) OR "
        		+ "(bce AND adf) OR (bce AND bcf) OR (bcf AND ade) OR (acf AND bdf) OR "
        		+ "(ace AND bdf) OR (ade AND ace) OR (adf AND adf) OR (acf AND bce) OR "
        		+ "(bce AND bde) OR (bce AND bce) OR (adf AND ace) OR (bce AND ace) OR "
        		+ "(acf AND acf))";
        
        printTranslatorResult(regex);

        Assert.assertEquals(expectedQueryStr, exactQuery.toString());
        Assert.assertEquals(exactQuery.toString(), expectedQueryStr);
    }

    @Test
    public void testAlternate1() {
        String regex = "uci|ics";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
        
        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);

        GramBooleanQuery expectedAnd1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        expectedQuery.subQuerySet.add(expectedAnd1);
        expectedAnd1.subQuerySet.addAll(getLeafNodeList(
        		new LeafNodeStruct("ics", -1, -1),
        		new LeafNodeStruct("ics", 0, 0)));

        GramBooleanQuery expectedAnd2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        expectedQuery.subQuerySet.add(expectedAnd2);
        expectedAnd2.subQuerySet.addAll(getLeafNodeList(
        		new LeafNodeStruct("ics", -1, -1),
        		new LeafNodeStruct("uci", 0, 1)));

        GramBooleanQuery expectedAnd3 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        expectedQuery.subQuerySet.add(expectedAnd3);
        expectedAnd3.subQuerySet.addAll(getLeafNodeList(
        		new LeafNodeStruct("ics", 0, 0),
        		new LeafNodeStruct("uci", -1, -1)));
        
        GramBooleanQuery expectedAnd4 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        expectedQuery.subQuerySet.add(expectedAnd4);
        expectedAnd4.subQuerySet.addAll(getLeafNodeList(
        		new LeafNodeStruct("uci", 0, 1),
        		new LeafNodeStruct("uci", -1, -1)));
        
        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testAlternate2() {
        String regex = "data*(bcd|pqr)";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
        
        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);

        GramBooleanQuery expectedAnd1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        expectedQuery.subQuerySet.add(expectedAnd1);
        expectedAnd1.subQuerySet.addAll(getLeafNodeList(
        		new LeafNodeStruct("dat", -1, -1),
        		new LeafNodeStruct("pqr", -1, -1)));

        GramBooleanQuery expectedAnd2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        expectedQuery.subQuerySet.add(expectedAnd2);
        expectedAnd2.subQuerySet.addAll(getLeafNodeList(
        		new LeafNodeStruct("dat", -1, -1),
        		new LeafNodeStruct("bcd", -1, -1)));

        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testPlus1() {
        String regex = "abc+";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
        
        GramBooleanQuery expectedQuery = GramBooleanQuery.newLeafNode("abc", -1, -1);

        printTranslatorResult(regex);

        Assert.assertEquals(expectedQuery, exactQuery);
        Assert.assertEquals(exactQuery, expectedQuery);
    }

    @Test
    public void testPlus2() {
        String regex = "abc+pqr+";

        GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
        
        GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);

        expectedQuery.subQuerySet.addAll(getLeafNodeList(
        		new LeafNodeStruct("cpq", -1, -1),
        		new LeafNodeStruct("pqr", -1, -1),
        		new LeafNodeStruct("abc", -1, -1)));

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

        String expectedQueryStr = "((abp AND abp AND cpq AND bpq) OR "
        		+ "(abp AND abp AND bpq AND bpq) OR "
        		+ "(bcp AND cpq AND abc AND abc AND bpq) OR "
        		+ "(bcp AND abp AND cpq AND cpq AND abc) OR "
        		+ "(bcp AND abp AND cpq AND abc AND bpq) OR "
        		+ "(bcp AND cpq AND cpq AND abc AND abc) OR "
        		+ "(abp AND cpq AND abc AND bpq) OR "
        		+ "(abp AND abc AND bpq AND bpq))";
        
        printTranslatorResult(regex);

        Assert.assertEquals(expectedQueryStr, exactQuery.toString());
        Assert.assertEquals(exactQuery.toString(), expectedQueryStr);
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
        		new LeafNodeStruct("abc", -1, -1),
        		new LeafNodeStruct("bcc", 1, 1),
        		new LeafNodeStruct("abc", 0, 1)));

        GramBooleanQuery expectedAnd2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        expectedQuery.subQuerySet.add(expectedAnd2);
        expectedAnd2.subQuerySet.addAll(getLeafNodeList(
        		new LeafNodeStruct("abc", 0, 2),
        		new LeafNodeStruct("bcc", 1, 2),
        		new LeafNodeStruct("abc", -1, -1),
        		new LeafNodeStruct("ccc", 2, 2)));

        GramBooleanQuery expectedAnd3 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        expectedQuery.subQuerySet.add(expectedAnd3);
        expectedAnd3.subQuerySet.addAll(getLeafNodeList(
        		new LeafNodeStruct("abc", -1, -1),
        		new LeafNodeStruct("abc", 0, 0)));

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
        		new LeafNodeStruct("qwe", -1, -1),
        		new LeafNodeStruct("qwe", 0, 0),
        		new LeafNodeStruct("cqw", -1, -1),
        		new LeafNodeStruct("wer", -1, -1),
        		new LeafNodeStruct("wer", 1, 0),
        		new LeafNodeStruct("bcq", -1, -1),
        		new LeafNodeStruct("abc", -1, -1)));

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