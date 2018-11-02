package edu.uci.ics.texera.web.resource.metadata;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.texera.api.exception.TexeraException;

import edu.uci.ics.texera.web.resource.metadata.tables.records.*;

public class MetadataResourceTest {
    
    /*
     * Test connection
     */
    @Test
    public void connectionTest() throws TexeraException {
        MetadataResource.getInstance();
    }

    /*
     * Test adding dictionary
     */
    @Test
    public void addDictionaryTest() throws TexeraException {
        MetadataResource.getInstance().addDictionary("dict1", "The content of dict1.");
        MetadataResource.getInstance().addDictionary("dict2", "The content of dict2.");
    }

   /*
    * Test getDictionaries
    */
   @Test
   public void getDictionariesTest() throws TexeraException {
       MetadataResource metadataResource = MetadataResource.getInstance();
       List<DictionaryRecord> dictionaries = metadataResource.getDictionaries();
       Assert.assertEquals("dict1", dictionaries.get(0).getId());
       Assert.assertEquals("dict1", dictionaries.get(0).getName());
       Assert.assertEquals("The content of dict1.", dictionaries.get(0).getContent());
       Assert.assertEquals("dict2", dictionaries.get(1).getId());
       Assert.assertEquals("dict2", dictionaries.get(1).getName());
       Assert.assertEquals("The content of dict2.", dictionaries.get(1).getContent());

   }

   /*
    * Test getDictionary
    */
   @Test
   public void getDictionaryTest() throws TexeraException {
    MetadataResource metadataResource = MetadataResource.getInstance();
    DictionaryRecord dict1 = metadataResource.getDictionary("dict1");
    Assert.assertEquals("dict1", dict1.getId());
    Assert.assertEquals("dict1", dict1.getName());
    Assert.assertEquals("The content of dict1.", dict1.getContent());

    DictionaryRecord dict2 = metadataResource.getDictionary("dict2");
    Assert.assertEquals("dict2", dict2.getId());
    Assert.assertEquals("dict2", dict2.getName());
    Assert.assertEquals("The content of dict2.", dict2.getContent());
   }

}