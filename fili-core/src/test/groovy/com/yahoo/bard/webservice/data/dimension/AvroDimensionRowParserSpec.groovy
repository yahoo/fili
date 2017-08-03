// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension

import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager

import org.apache.avro.generic.GenericRecord

import spock.lang.Specification

import java.util.function.Consumer

class AvroDimensionRowParserSpec extends Specification {
    LinkedHashSet<DimensionField> dimensionFields
    KeyValueStoreDimension dimension
    AvroDimensionRowParser avroDimensionRowParser

    def setup() {
        dimensionFields = [BardDimensionField.ID, BardDimensionField.DESC]
        dimension = new KeyValueStoreDimension("foo", "desc-foo", dimensionFields, MapStoreManager.getInstance("foo"), ScanSearchProviderManager.getInstance("foo"))
        avroDimensionRowParser = new AvroDimensionRowParser(DimensionFieldNameMapper.underscoreSeparatedConverter())
    }

    def "Schema file containing all the dimension fields and data parses to expected rows"() {
        given:
        DimensionRow dimensionRow1 = BardDimensionField.makeDimensionRow(dimension, "12345", "bar")
        DimensionRow dimensionRow2 = BardDimensionField.makeDimensionRow(dimension, "67890", "baz")
        Set<DimensionRow> dimSet = [dimensionRow1, dimensionRow2] as Set

        expect:
        avroDimensionRowParser.parseAvroFileDimensionRows(dimension, "target/avro/avroFilesTesting/sampleData.avro") == dimSet
    }

    def "Schema file containing all the dimension fields and data parses to expected rows, using consumer"() {
        given:
        DimensionRow dimensionRow1 = BardDimensionField.makeDimensionRow(dimension, "12345", "bar")
        DimensionRow dimensionRow2 = BardDimensionField.makeDimensionRow(dimension, "67890", "baz")
        Set<DimensionRow> dimSet = [dimensionRow1, dimensionRow2] as Set

        Set<DimensionRow> actual = new LinkedHashSet<>()
        Consumer<DimensionRow> rowConsumer = {actual.add(it)}

        when:
        avroDimensionRowParser.parseAvroFileDimensionRows(dimension, "target/avro/avroFilesTesting/sampleData.avro", rowConsumer)

        then:
        actual == dimSet
    }

    def "Schema file does not contain all the dimension fields throws an IllegalArgumentException"() {
        given:
        dimensionFields.add(BardDimensionField.FIELD1)

        when:
        avroDimensionRowParser.parseAvroFileDimensionRows(dimension, "target/avro/avroFilesTesting/sampleData.avro")

        then:
        IllegalArgumentException exception = thrown(IllegalArgumentException)
        exception.message == "The AVRO schema file does not contain all the configured dimension fields"
    }

    def "Null value will be replaced with null string by the avro parser"() {
        given:
        GenericRecord genericRecord = Mock() {
            get(_) >> null
        }

        expect:
        avroDimensionRowParser.resolveRecordValue(genericRecord, "random") == ""
    }
}
