// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.table.PhysicalTableDictionary

import org.joda.time.DateTime

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicReference

class DataSourceMetadataServiceSpec extends BaseDataSourceMetadataSpec {

    def "test metadata service updates segment availability for physical tables"() {
        setup:
        JerseyTestBinder jtb = new JerseyTestBinder()
        PhysicalTableDictionary tableDict = jtb.configurationLoader.getPhysicalTableDictionary()
        DataSourceMetadataService metadataService = new DataSourceMetadataService()

        DataSourceMetadata metadata = new DataSourceMetadata(tableName, [:], segments)

        when:
        metadataService.update(tableDict.get(tableName), metadata)

        then:
        metadataService.allSegments.get(tableDict.get(tableName)) instanceof AtomicReference

        and:
        metadataService.allSegments.get(tableDict.get(tableName)).get().values()*.keySet() as List ==
        [
                [segment1.getIdentifier(), segment2.getIdentifier()] as Set,
                [segment3.getIdentifier(), segment4.getIdentifier()] as Set
        ] as List

        cleanup:
        jtb.tearDown()
    }
}
