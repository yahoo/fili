// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.serializers

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.dimension.SearchProvider

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
        ObjectMapper mapper = new ObjectMapper();

        expect:
        // NOTE: The fact that this doesn't show an exception demonstrates that it does not hit the DimensionToNameSerializer as well
        mapper.writeValueAsString(new DummyDimension()).contains("woot")
    }

    @JsonSerialize
    public static class DummyDimension implements Dimension {
        @JsonValue
        public String example() {
            return "woot";
        }

        @Override
        public void setLastUpdated(DateTime lastUpdated) {

        }

        @Override
        public String getApiName() {
            return "abc";
        }

        @Override
        public String getDescription() {
            return null;
        }

        @Override
        public DateTime getLastUpdated() {
            return null;
        }

        @Override
        public LinkedHashSet<DimensionField> getDimensionFields() {
            return null;
        }

        @Override
        public LinkedHashSet<DimensionField> getDefaultDimensionFields() {
            return null;
        }

        @Override
        public DimensionField getFieldByName(String name) {
            return null;
        }

        @Override
        public SearchProvider getSearchProvider() {
            return null;
        }

        @Override
        public void addDimensionRow(DimensionRow dimensionRow) {

        }

        @Override
        public void addAllDimensionRows(Set<DimensionRow> dimensionRows) {

        }

        @Override
        public DimensionRow findDimensionRowByKeyValue(String value) {
            return null;
        }

        @Override
        public DimensionField getKey() {
            return null;
        }

        @Override
        public DimensionRow parseDimensionRow(Map<String, String> fieldNameValueMap) {
            return null;
        }

        @Override
        public DimensionRow createEmptyDimensionRow(String keyFieldValue) {
            return null;
        }

        @Override
        public String getCategory() {
            return null;
        }

        @Override
        public String getLongName() {
            return null;
        }

        @Override
        public int getCardinality() {
            return 0;
        }

        @Override
        public boolean isAggregatable() {
            return false;
        }
    }
}
