// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.metadata.BaseDataSourceMetadataSpec
import com.yahoo.bard.webservice.metadata.DataSourceMetadata
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.metadata.SegmentInfo
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.endpoints.SlicesServlet

import org.joda.time.DateTime

import spock.lang.Shared
import spock.lang.Unroll

import javax.ws.rs.core.UriBuilder
import javax.ws.rs.core.UriInfo

class SlicesApiRequestImplSpec extends BaseDataSourceMetadataSpec {

    @Shared
    PhysicalTableDictionary fullDictionary
    @Shared
    PhysicalTableDictionary emptyDictionary

    JerseyTestBinder jtb
    UriInfo uriInfo
    UriBuilder builder
    String baseUri
    DataSourceMetadataService dataSourceMetadataService

    @Override
    def childSetupSpec() {
        tableName = generateTableName()
        segments = generateSegments()

        emptyDictionary = new PhysicalTableDictionary()
    }

    def setup() {
        uriInfo = Mock(UriInfo)
        builder = Mock(UriBuilder)
        baseUri = "http://localhost:9998/v1/slices/"
        dataSourceMetadataService = new DataSourceMetadataService()

        jtb = new JerseyTestBinder(SlicesServlet.class)
        fullDictionary = jtb.configurationLoader.physicalTableDictionary
        builder.path(_) >> builder
        builder.path(_, _) >> builder

        DataSourceMetadata dataSourceMetadata = new DataSourceMetadata(tableName, [:], segments.values().toList())
        dataSourceMetadataService.update(fullDictionary.get(tableName).dataSourceNames[0], dataSourceMetadata)
    }

    def cleanup() {
        jtb.tearDown()
    }

    def "check api request construction for the top level endpoint (all slices)"() {
        setup:
        builder.build(_) >> { List<List<String>> args ->
            new URI(baseUri + args[0][0])
        }
        uriInfo.getBaseUriBuilder() >> builder

        Set<Map<String, String>> expected = fullDictionary.collect {
            [
                "name": it.key,
                "timeGrain": it.value.schema.timeGrain.name.toLowerCase(Locale.ENGLISH),
                "uri": baseUri + it.key
            ] as LinkedHashMap
        }

        when:
        SlicesApiRequestImpl apiRequest = new SlicesApiRequestImpl(
                null,
                null,
                "",
                "",
                fullDictionary,
                dataSourceMetadataService,
                uriInfo
        )

        then:
        apiRequest.slices == expected
    }

    def "check api request construction for a given table name"() {
        setup:
        String name = "all_pets"
        PhysicalTable table = fullDictionary.get(name)
        String uri = baseUri.replaceAll("/slices/.*", "") + "/dimensions/"

        builder.build(_) >> { List<List<String>> args ->
            new URI(uri + args[0][0])
        }

        uriInfo.getBaseUriBuilder() >> builder
        uriInfo.getRequestUriBuilder() >> builder

        Set<Map<String, Object>> dimensionsResult = [] as LinkedHashSet
        Set<Map<String, Object>> metricsResult = [] as LinkedHashSet

        table.allAvailableIntervals.each {
            Map<String, Object> row = [:] as LinkedHashMap
            row["intervals"] = it.value
            row["name"] = it.key.name
            if (it.key instanceof DimensionColumn) {
                row["uri"] = uri + it.key.name
                row["factName"] = table.getPhysicalColumnName(it.key.name)
                dimensionsResult.add(row)
            } else {
                metricsResult.add(row)
            }
        }
        Set<SortedMap<DateTime, Map<String, SegmentInfo>>> sliceMetadata = dataSourceMetadataService.getSegments(
                table.dataSourceNames
        )

        Map<String, Object> expected = [
            "name": name,
            "timeGrain": fullDictionary.get(name).schema.timeGrain.name,
            "timeZone": "UTC",
            "dimensions": dimensionsResult,
            "metrics": metricsResult,
            // This test compares generated metadata against itself, meaning that the contents of this generation are
            // not under test.
            "segmentInfo": SlicesApiRequestImpl.generateSegmentMetadataView(sliceMetadata)
        ] as LinkedHashMap

        when:
        SlicesApiRequestImpl apiRequest = new SlicesApiRequestImpl(
                name,
                null,
                "",
                "",
                fullDictionary,
                dataSourceMetadataService,
                uriInfo
        )

        then:
        apiRequest.getSlice() == expected
    }

    @Unroll
    def "api request construction throws #exception.simpleName because #reason"() {
        setup:
        builder.build(_) >> { List<List<String>> args ->
            new URI(baseUri + args[0][0])
        }

        uriInfo.getBaseUriBuilder() >> builder

        when:
        new SlicesApiRequestImpl(
                name,
                null,
                "",
                "",
                dictionary,
                dataSourceMetadataService,
                uriInfo
        )

        then:
        Exception e = thrown(exception)
        e.getMessage().matches(reason)

        where:
        name         | dictionary      | exception              | reason
        "all_pets"   | emptyDictionary | BadApiRequestException | ".*Physical Table Dictionary is empty.*"
        "all_beasts" | fullDictionary  | BadApiRequestException | ".*Slice name.*does not exist.*"
    }
}
