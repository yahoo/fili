// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension

import com.fasterxml.jackson.databind.ObjectMapper
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import spock.lang.Specification


class AvroDimensionRowParserSpec extends Specification {
    def LinkedHashSet<DimensionField> dimensionFields
    def KeyValueStoreDimension dimension
    def AvroDimensionRowParser avroDimensionRowParser

    def setup() {
        dimensionFields = [BardDimensionField.ID, BardDimensionField.DESC]
        dimension = new KeyValueStoreDimension("foo", "desc-foo", dimensionFields, MapStoreManager.getInstance("foo"), ScanSearchProviderManager.getInstance("foo"))
        avroDimensionRowParser = new AvroDimensionRowParser(DimensionFieldNameMapper.underscoreSeparatedConverter(), new ObjectMapper())
    }

    def "Schema file containing all the dimension fields and data parses to expected rows"() {
        given:
        DimensionRow dimensionRow1 = BardDimensionField.makeDimensionRow(dimension, "12345", "bar")
        DimensionRow dimensionRow2 = BardDimensionField.makeDimensionRow(dimension, "67890", "baz")
        Set<DimensionRow> dimSet = [dimensionRow1, dimensionRow2] as Set

        expect:
        avroDimensionRowParser.parseAvroFileDimensionRows(dimension, "src/test/resources/avroFilesTesting/sampleData.avro", "src/test/resources/avroFilesTesting/sampleData.avsc") == dimSet
    }

    def "Invalid schema - Schema file does not exist throws an IllegalArgumentException"() {
        when:
        avroDimensionRowParser.parseAvroFileDimensionRows(dimension, "src/test/resources/avroFilesTesting/sampleData.avro", "src/test/resources/avroFilesTesting/foo.avsc")

        then:
        IllegalArgumentException exception = thrown(IllegalArgumentException)
        exception.message == "Unable to process the file, at the location src/test/resources/avroFilesTesting/foo.avsc"
    }

    def "Schema file does not contain all the dimension fields throws an IllegalArgumentException"() {
        given:
        dimensionFields.add(BardDimensionField.FIELD1)

        when:
        avroDimensionRowParser.parseAvroFileDimensionRows(dimension, "src/test/resources/avroFilesTesting/sampleData.avro", "src/test/resources/avroFilesTesting/sampleData.avsc")

        then:
        IllegalArgumentException exception = thrown(IllegalArgumentException)
        exception.message == "The AVRO schema file does not contain all the configured dimension fields"
    }

    def "Schema file does not contain the required `fields` section in its JSON throws an IllegalArgumentException" () {
        when:
        avroDimensionRowParser.parseAvroFileDimensionRows(dimension, "src/test/resources/avroFilesTesting/sampleData.avro", "src/test/resources/avroFilesTesting/invalidSchema.avsc")

        then:
        IllegalArgumentException exception = thrown(IllegalArgumentException)
        exception.message == "`fields` is a required JSON field in the avro schema"
    }
}
