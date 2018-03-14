package edu.uci.ics.texera.web.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.dataflow.resource.dictionary.DictionaryManager;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.TableMetadata;
import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.web.response.TexeraWebResponse;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

@Path("/resources")
@Produces(MediaType.APPLICATION_JSON)
public class SystemResource {

	@GET
	@Path("/metadata")
	public TexeraWebResponse getMetadata() throws StorageException, JsonProcessingException {
		List<TableMetadata> tableMetadata = RelationManager.getInstance().getMetaData();
		return new TexeraWebResponse(0, new ObjectMapper().writeValueAsString(tableMetadata));
	}

    /**
     * Get the list of dictionaries
     */
	@GET
	@Path("/dictionaries")
	public TexeraWebResponse getDictionaries() throws StorageException, JsonProcessingException {
		DictionaryManager dictionaryManager = DictionaryManager.getInstance();
		List<String> dictionaries = dictionaryManager.getDictionaries();

		return new TexeraWebResponse(0, new ObjectMapper().writeValueAsString(dictionaries));
	}

	 /**
     * Get the content of dictionary
     */
	@GET
	@Path("/dictionary/{name}")
	public TexeraWebResponse getDictionaryByName(@PathParam("name") String name) {
        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
        String dictionaryContent = dictionaryManager.getDictionary(name);

		return new TexeraWebResponse(0, dictionaryContent);
	}

//    /**
//     * Get the content of dictionary
//     */
//    @GET
//    @Path("/dictionary/{id}")
//    public TexeraWebResponse getDictionaryByID(@PathParam("id") int id) {
//        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
//        String dictionaryContent = dictionaryManager.getDictionaryByID(id);
//
//        return new TexeraWebResponse(0, dictionaryContent);
//    }

    /**
     * Delete the content of dictionary
     */
    @DELETE
    @Path("/dictionary/{id}")
    public TexeraWebResponse deleteDictionaryByID(@PathParam("id") int id) {
        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
        dictionaryManager.removeDictionaryByID(id);
        return new TexeraWebResponse(0, "Success");
    }


}