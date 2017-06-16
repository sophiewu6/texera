package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class GramBooleanQueryTest {

    @Test
    public void testEmptySameOp() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        Assert.assertTrue(query1.equals(query2));
    }

    @Test
    public void testEmptyDifferentOp() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        Assert.assertFalse(query1.equals(query2));
    }

    @Test
    public void testSameAnd() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("abc")), true);
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("abc")), true);

        Assert.assertEquals(query1, query2);
        Assert.assertEquals(query2, query1);
    }

    @Test
    public void testSameOr() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("abcdef")), true);
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("abcdef")), true);

        Assert.assertEquals(query1, query2);
        Assert.assertEquals(query2, query1);
    }

    @Test
    public void testDifferentAnd() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("abc")), true);
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("pqr")), true);

        Assert.assertFalse(query1.equals(query2));
        Assert.assertFalse(query2.equals(query1));
    }

    @Test
    public void testSameMultiple() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("abc")), true);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("pqr")), true);
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("abc")), true);
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("pqr")), true);

        Assert.assertEquals(query1, query2);
        Assert.assertEquals(query2, query1);
    }

    @Test
    public void testDifferentMultiple() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("qwe")), true);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("asd")), true);
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("zxc")), true);
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("vbn")), true);

        Assert.assertFalse(query1.equals(query2));
        Assert.assertFalse(query2.equals(query1));
    }

    @Test
    public void testSameDifferentOrder() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("abc", "pqr")), true);
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("pqr", "abc")), true);

        Assert.assertEquals(query1, query2);
        Assert.assertEquals(query2, query1);
    }

    @Test
    public void testSameDifferentOrder2() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("asdfg", "poiuy")), true);
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("poiuy", "asdfg")), true);

        Assert.assertEquals(query1, query2);
        Assert.assertEquals(query2, query1);
    }

    @Test
    public void testSameDifferentOrder3() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("abcd", "qwer", "zxc")), true);
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("zxc", "qwer", "abcd")), true);

        Assert.assertEquals(query1, query2);
        Assert.assertEquals(query2, query1);
    }

    @Test
    public void testDifferentDifferentOrder() {
        GramBooleanQuery query1 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        GramBooleanQuery query2 = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
        query1 = GramBooleanQuery.combine(query1, (Arrays.asList("abcd", "qwer", "zxc")), true);
        query2 = GramBooleanQuery.combine(query2, (Arrays.asList("abc", "qwe", "zxcv")), true);

        Assert.assertFalse(query1.equals(query2));
        Assert.assertFalse(query2.equals(query1));
    }

}
