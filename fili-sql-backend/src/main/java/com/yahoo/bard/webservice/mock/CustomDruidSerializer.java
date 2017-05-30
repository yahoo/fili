package com.yahoo.bard.webservice.mock;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 * Created by hinterlong on 5/31/17.
 */
public class CustomDruidSerializer extends JsonSerializer {

    @Override
    public void serialize(Object o, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        DruidMockResponse druidMockResponse = (DruidMockResponse) o;

        jsonGenerator.writeStartArray();
        for (DruidMockResponse.TimeStampResult t : druidMockResponse.results) {
            jsonGenerator.writeObject(t);
        }
        jsonGenerator.writeEndArray();
    }
}
