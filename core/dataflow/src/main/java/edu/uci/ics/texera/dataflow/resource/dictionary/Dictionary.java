package edu.uci.ics.texera.dataflow.resource.dictionary;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonSerialize(as = ImmutableDictionary.class)
@JsonDeserialize(as = ImmutableDictionary.class)
public abstract class Dictionary {
	@Value.Parameter
	abstract int getId();
	
	@Value.Parameter
	abstract String getName();
	
	@Value.Parameter
	abstract String getContent();
}
