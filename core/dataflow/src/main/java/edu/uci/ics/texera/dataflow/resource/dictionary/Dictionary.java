package edu.uci.ics.texera.dataflow.resource.dictionary;

import java.io.IOException;
import java.util.List;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonSerialize(as = ImmutableDictionary.class)
@JsonDeserialize(as = ImmutableDictionary.class)
public abstract class Dictionary {
	@Value.Parameter
	public abstract int getId();
	
	@Value.Parameter
	public abstract String getName();
	
	@Value.Parameter
	public abstract String getContent();
	
	@Value.Derived
	public List<String> getDictionaryEntries(){
		String currentContent = getContent();
		try {
			@SuppressWarnings("unchecked")
			List<String> dictionaryEntries = new ObjectMapper().readValue(currentContent, List.class);
			return dictionaryEntries;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
