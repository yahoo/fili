// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import static com.yahoo.bard.webservice.data.Columns.DIMENSIONS
import static com.yahoo.bard.webservice.data.Columns.METRICS

import com.yahoo.bard.webservice.data.Columns
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import com.google.common.collect.RangeSet

import org.joda.time.DateTime

class DataSourceMetadataSpec extends BaseDataSourceMetadataSpec {
    def "test construct healthy datasource metadata (interval version)"() {
        setup:
        DataSourceMetadata metadata = new DataSourceMetadata(tableName, [:], segments)
        Map<Columns, Map<String, SimplifiedIntervalList>> intervalLists = DataSourceMetadata.getIntervalLists(metadata)

        expect:
        metadata.getName() == tableName
        metadata.getProperties() == [:]
        metadata.getSegments() == segments
        intervalLists.size() == 2
        intervalLists.get(DIMENSIONS).keySet().sort() == dimensions123.sort()
        intervalLists.get(DIMENSIONS).values() as List == [
                [interval12] as SimplifiedIntervalList,
                [interval12] as SimplifiedIntervalList,
                [interval12] as SimplifiedIntervalList
        ]
        intervalLists.get(METRICS).keySet().sort() == metrics123.sort()
        intervalLists.get(METRICS).values() as List == [
                [interval12] as SimplifiedIntervalList,
                [interval12] as SimplifiedIntervalList,
                [interval12] as SimplifiedIntervalList
        ]
    }

    def "test construct healthy datasource metadata (rangeSet version)"() {
        setup:
        DataSourceMetadata metadata = new DataSourceMetadata(tableName, [:], segments)
        Map<Columns, Map<String, RangeSet<DateTime>>> rangeLists = DataSourceMetadata.getRangeLists(metadata)

        expect:
        metadata.getName() == tableName
        metadata.getProperties() == [:]
        metadata.getSegments() == segments
        rangeLists.size() == 2
        rangeLists.get(DIMENSIONS).keySet().sort() == dimensions123.sort()
        rangeLists.get(DIMENSIONS).values() as List == [
                rangeSet12,
                rangeSet12,
                rangeSet12
        ]
        rangeLists.get(METRICS).keySet().sort() == metrics123.sort()
        rangeLists.get(METRICS).values() as List == [
                rangeSet12,
                rangeSet12,
                rangeSet12
        ]
    }
}
