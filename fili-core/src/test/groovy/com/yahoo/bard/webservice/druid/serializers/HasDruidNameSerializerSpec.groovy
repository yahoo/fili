// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.serializers

import com.yahoo.bard.webservice.druid.model.HasDruidName

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper
import com.fasterxml.jackson.databind.jsontype.TypeSerializer

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Tests that the HasDruidNameSerializer behaves as expected
 */
class HasDruidNameSerializerSpec extends Specification {

    def "acceptJsonFormatVisitor calls visitor.expectStringFormat when visitor != null"() {
        given: "A mock visitor and a type hint"
        def visitor = Mock(JsonFormatVisitorWrapper)
        def typeHint = Mock(JavaType)

        when: "We call acceptJsonFormatVisitor with a non-null visitor"
        HasDruidNameSerializer.INSTANCE.acceptJsonFormatVisitor(visitor, typeHint)

        then: "visitor.expectStringFormat is called with the given typeHint as the parameter"
        1 * visitor.expectStringFormat(typeHint)
    }

    def "acceptJsonFormatVisitor doesn't call visitor.expectStringFormat when visitor == null"() {
        given: "A null visitor and a type hint"
        def visitor = null
        def typeHint = Mock(JavaType)

        when: "We call acceptJsonFormatVisitor with a null visitor"
        HasDruidNameSerializer.INSTANCE.acceptJsonFormatVisitor(visitor, typeHint)

        then: "visitor.expectStringFormat is not called with the given typeHint as the parameter"
        0 * visitor.expectStringFormat(typeHint)
    }

    def "getSchema returns a JsonNode with type: string"() {
        when: "We get the schema"
        def schema = HasDruidNameSerializer.INSTANCE.getSchema(null, null)

        then: "the JsonNode has type: string"
        schema.get("type").asText() == "string"
    }

    def "serializeWithType writes the type prefix, calls regular serialization, and writes the type suffix"() {
        given: "Some mock parameters"
        def value = Mock(HasDruidName)
        def jgen = Mock(JsonGenerator)
        def provider = Mock(SerializerProvider)
        def typeSer = Mock(TypeSerializer)

        and: "value.getDruidName returns a known string"
        def knownString = "A Sample String"
        value.getDruidName() >> knownString

        when: "We call serializeWithType"
        HasDruidNameSerializer.INSTANCE.serializeWithType(value, jgen, provider, typeSer)

        then: "We've written the type prefix"
        1 * typeSer.writeTypePrefixForScalar(value, jgen)

        and: "We've called regular serialization, writing value.getDruidName()"
        1 * jgen.writeString(knownString)

        and: "We've written the type suffix"
        1 * typeSer.writeTypeSuffixForScalar(value, jgen)
    }

    def "serialize writes value.getDruidName"() {
        given: "Some mock parameters"
        def value = Mock(HasDruidName)
        def jgen = Mock(JsonGenerator)
        def provider = Mock(SerializerProvider)

        and: "value.getDruidName returns a known string"
        def knownString = "A Sample String"
        value.getDruidName() >> knownString

        when: "We call serialize"
        HasDruidNameSerializer.INSTANCE.serialize(value, jgen, provider)

        then: "We've written value.getDruidName()"
        1 * jgen.writeString(knownString)
    }

    def "isEmpty handles a null value"() {
        expect: "Null value is empty"
        HasDruidNameSerializer.INSTANCE.isEmpty(null)
    }

    @Unroll
    def "isEmpty returns #expectedReturn for #druidName as a druid name"() {
        given: "A HasDruidName with the druid name we're testing"
        def value = Mock(HasDruidName)
        value.getDruidName() >> druidName

        expect: "isEmpty works as expected"
        HasDruidNameSerializer.INSTANCE.isEmpty(value) == expectedReturn

        where:
        druidName   | expectedReturn
        null        | true
        ""          | true
        "non-empty" | false
    }
}
