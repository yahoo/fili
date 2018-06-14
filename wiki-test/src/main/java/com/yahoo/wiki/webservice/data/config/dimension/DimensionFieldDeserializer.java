package com.yahoo.wiki.webservice.data.config.dimension;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.LinkedHashSet;

public class DimensionFieldDeserializer extends JsonDeserializer<WikiDimensionField> {

    @Override
    public WikiDimensionField deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {

        WikiDimensionField wikiDimensionField = new WikiDimensionField();

        if(jp.getCurrentToken() == JsonToken.VALUE_STRING) {

            wikiDimensionField.setFieldName(jp.getText());

        } else {

            ObjectCodec oc = jp.getCodec();
            ObjectMapper objectMapper = new ObjectMapper();

            JsonNode node = oc.readTree(jp);
            LinkedHashSet<WikiDimensionFieldTemplate> list = objectMapper.convertValue(node, new TypeReference<LinkedHashSet<WikiDimensionFieldTemplate>>(){ });

            wikiDimensionField.setFieldList(list);
        }

        return wikiDimensionField;
    }

}

