// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.table.PhysicalTableDictionary

import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Collectors

class DataSourceMetadataServiceSpec extends BaseDataSourceMetadataSpec {

    def "test metadata service updates segment availability for physical tables and access methods behave correctly"() {
        setup:
        JerseyTestBinder jtb = new JerseyTestBinder()
        PhysicalTableDictionary tableDict = jtb.configurationLoader.getPhysicalTableDictionary()
        DataSourceMetadataService metadataService = new DataSourceMetadataService()
        DataSourceMetadata metadata = new DataSourceMetadata(tableName, [:], segments)
        TableName currentTableName = tableDict.get(tableName).getTableName()

        when:
        metadataService.update(tableDict.get(tableName), metadata)

        then:
        metadataService.allSegmentsByTime.get(currentTableName) instanceof AtomicReference
        metadataService.allSegmentsByColumn.get(currentTableName) instanceof AtomicReference

        and:
        metadataService.getTableSegments(Collections.singleton(currentTableName)).stream()
                .map({it.values()})
                .flatMap({it.stream()})
                .map({it.values()})
                .collect(Collectors.toList()).toString()  == [[segment1.getIdentifier(), segment2.getIdentifier()],
                                                              [segment3.getIdentifier(), segment4.getIdentifier()]].toString()

        and: "all the intervals by column in metadata service are simplified to interval12"
        [[interval12] as Set].containsAll(metadataService.getAvailableIntervalsByTable(currentTableName).values())

        cleanup:
        jtb.tearDown()
    }

    def "accessing availability by column throws exception if the table does not exist in datasource metadata service"() {
        setup:
        DataSourceMetadataService metadataService = new DataSourceMetadataService()

        when:
        metadataService.getAvailableIntervalsByTable(TableName.of("InvalidTable"))

        then:
        IllegalStateException e = thrown()
        e.message == 'Trying to access InvalidTable physical table datasource that is not available in metadata service'

    }
}
