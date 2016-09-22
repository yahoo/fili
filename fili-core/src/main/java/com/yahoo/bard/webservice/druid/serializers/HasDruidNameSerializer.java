// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.serializers;

import com.yahoo.bard.webservice.druid.model.HasDruidName;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 * Serializer to write out fields which have druid names.
 */
public class HasDruidNameSerializer extends StdSerializer<HasDruidName> {
    /**
     * Singleton instance to use.
     */
    public final static HasDruidNameSerializer INSTANCE = new HasDruidNameSerializer();

    /**
     * Note: usually you should NOT create new instances, but instead use {@link #INSTANCE} which is stateless and fully
     * thread-safe. However, there are cases where constructor is needed; for example, when using explicit serializer
     * annotations like {@link com.fasterxml.jackson.databind.annotation.JsonSerialize#using}.
     */
    public HasDruidNameSerializer() {
        super(HasDruidName.class);
    }

    @Override
    public boolean isEmpty(SerializerProvider provider,  HasDruidName value) {
        return value == null || value.getDruidName() == null || value.getDruidName().isEmpty();
    }

    @Override
    public void serialize(HasDruidName value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeString(value.getDruidName());
    }

    /**
     * Default implementation will write type prefix, call regular serialization method (since assumption is that value
     * itself does not need JSON Array or Object start/end markers), and then write type suffix. This should work for
     * most cases; some sub-classes may want to change this behavior.
     */
    @Override
    public void serializeWithType(
            HasDruidName value,
            JsonGenerator jgen,
            SerializerProvider provider,
            TypeSerializer typeSer
    ) throws IOException {
        typeSer.writeTypePrefixForScalar(value, jgen);
        serialize(value, jgen, provider);
        typeSer.writeTypeSuffixForScalar(value, jgen);
    }

    @Override
    public JsonNode getSchema(SerializerProvider provider, Type typeHint) throws JsonMappingException {
        return createSchemaNode("string", true);
    }

    @Override
    public void acceptJsonFormatVisitor(JsonFormatVisitorWrapper visitor, JavaType typeHint)
            throws JsonMappingException {
        if (visitor != null) {
            visitor.expectStringFormat(typeHint);
        }
    }
}
