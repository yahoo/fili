// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.serializers

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy

import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonSerialize

import org.joda.time.DateTime

import spock.lang.Specification

class DimensionToNameSerializerSpec extends Specification {

    // Corner case.
    // NOTE: Dimension serialization is properly tested in GroupByQuerySpec.groovy
    def "allows implementing classes to override default serialization"() {
        setup:
        ObjectMapper mapper = new ObjectMappersSuite().getMapper()

        expect:
        // NOTE: The fact that this doesn't show an exception demonstrates that it does not hit the DimensionToNameSerializer as well
        mapper.writeValueAsString(new DummyDimension()).contains("woot")
    }

    @JsonSerialize
    static class DummyDimension implements Dimension {
        @JsonValue
        String example() {
            return "woot"
        }

        @Override
        void setLastUpdated(DateTime lastUpdated) {

        }

        @Override
        String getApiName() {
            return "abc"
        }

        @Override
        String getDescription() {
            return null
        }

        @Override
        DateTime getLastUpdated() {
            return null
        }

        @Override
        LinkedHashSet<DimensionField> getDimensionFields() {
            return null
        }

        @Override
        LinkedHashSet<DimensionField> getDefaultDimensionFields() {
            return null
        }

        @Override
        DimensionField getFieldByName(String name) {
            return null
        }

        @Override
        SearchProvider getSearchProvider() {
            return null
        }

        @Override
        void addDimensionRow(DimensionRow dimensionRow) {

        }

        @Override
        void addAllDimensionRows(Set<DimensionRow> dimensionRows) {

        }

        @Override
        DimensionRow findDimensionRowByKeyValue(String value) {
            return null
        }

        @Override
        DimensionField getKey() {
            return null
        }

        @Override
        DimensionRow parseDimensionRow(Map<String, String> fieldNameValueMap) {
            return null
        }

        @Override
        DimensionRow createEmptyDimensionRow(String keyFieldValue) {
            return null
        }

        @Override
        String getCategory() {
            return null
        }

        @Override
        String getLongName() {
            return null
        }

        @Override
        int getCardinality() {
            return 0
        }

        @Override
        boolean isAggregatable() {
            return false
        }

        @Override
        StorageStrategy getStorageStrategy() {
            return null
        }
    }
}
