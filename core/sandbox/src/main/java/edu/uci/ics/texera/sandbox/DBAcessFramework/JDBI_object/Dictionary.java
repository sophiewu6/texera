package edu.uci.ics.texera.sandbox.DBAcessFramework.JDBI_object;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.minidev.json.annotate.JsonIgnore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;

/**
 * Dictionary is a data class holding the fields consisting of a dictionary.
 *
 * The Dictionary's content is a list of String (keywords), however, when mapping
 * to a database, it must be converted to a single string.
 * Dictionary provides toContentString to convert to a string
 * and fromContentString that converts back to a list.
 *
 * --Note: for this sample, look at User for explanation of a data class
 *
 * @author Zuozhi Wang
 * Created at 10/21/2018
 */
public class Dictionary {

    private String dictID;
    private String dictName;
    private List<String> content;

    public Dictionary() {
    }

    public Dictionary(final String dictID, final String dictName, final List<String> content) {
        this.dictID = dictID;
        this.dictName = dictName;
        this.content = content;
    }

    public String getDictID() {
        return dictID;
    }

    public void setDictID(final String dictID) {
        this.dictID = dictID;
    }

    public String getDictName() {
        return dictName;
    }

    public void setDictName(String dictName) {
        this.dictName = dictName;
    }

    public List<String> getContent() {
        return content;
    }

    public void setContent(final List<String> content) {
        this.content = content;
    }

    @JsonIgnore
    public String toContentString() {
        try {
            return new ObjectMapper().writeValueAsString(this.content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<String> fromContentString(String contentString) {
        try {
            return new ObjectMapper().readValue(contentString, new TypeReference<List<String>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Dictionary that = (Dictionary) o;
        return Objects.equals(dictID, that.dictID) &&
                Objects.equals(dictName, that.dictName) &&
                Objects.equals(content, that.content);
    }

    @Override
    public int hashCode() {

        return Objects.hash(dictID, dictName, content);
    }

    @Override
    public String toString() {
        return "Dictionary{" +
                "dictID='" + dictID + '\'' +
                ", dictName='" + dictName + '\'' +
                ", content=" + content +
                '}';
    }
}
