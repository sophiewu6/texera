package edu.uci.ics.texera.web.resource.metadata;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.exception.TexeraException;

public class MetadataResourceTest {
    private static final List<String> content1 = Arrays.asList("hello", "UCI");
    private static final List<String> content2 = Arrays.asList("Hi", "texera");
    
    /*
     * Test connection & createDictionaryManager
     */
    @Test
    public void connectionTest() throws TexeraException {
        MetadataResource metadataResource = MetadataResource.getInstance();
        Assert.assertEquals(metadataResource.checkTableExistence("Dictionary"), true);
    }

    /*
     * Test adding dictionary
     */
    @Test
    public void addDictionaryTest() throws TexeraException {
        MetadataResource metadataResource = MetadataResource.getInstance();
        metadataResource.addDictionary("dict1", content1);
        metadataResource.addDictionary("dict2", content2);
    }

   /*
    * Test getDictionaries
    */
   @Test
   public void getDictionariesTest() throws TexeraException {
    MetadataResource metadataResource = MetadataResource.getInstance();
    List<Dictionary> dictionaries = metadataResource.getDictionaries();
    Assert.assertEquals("dict1", dictionaries.get(0).getDictID());
    Assert.assertEquals("dict1", dictionaries.get(0).getDictName());
    Assert.assertEquals(content1, dictionaries.get(0).getContent());
    Assert.assertEquals("dict2", dictionaries.get(1).getDictID());
    Assert.assertEquals("dict2", dictionaries.get(1).getDictName());
    Assert.assertEquals(content2, dictionaries.get(1).getContent());
   }

   /*
    * Test getDictionary
    */
   @Test
   public void getDictionaryTest() throws TexeraException {
    MetadataResource metadataResource = MetadataResource.getInstance();
    Dictionary dict1 = metadataResource.getDictionary("dict1");
    Assert.assertEquals("dict1", dict1.getDictID());
    Assert.assertEquals("dict1", dict1.getDictName());
    Assert.assertEquals(content1, dict1.getContent());

    Dictionary dict2 = metadataResource.getDictionary("dict2");
    Assert.assertEquals("dict2", dict2.getDictID());
    Assert.assertEquals("dict2", dict2.getDictName());
    Assert.assertEquals(content2, dict2.getContent());
   }

}