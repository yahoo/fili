// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.metadata.BaseDataSourceMetadataSpec
import com.yahoo.bard.webservice.metadata.DataSourceMetadata
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.metadata.SegmentInfo
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.table.Table
import com.yahoo.bard.webservice.web.endpoints.SlicesServlet

import org.joda.time.DateTime

import spock.lang.Shared
import spock.lang.Unroll

import javax.ws.rs.core.UriBuilder
import javax.ws.rs.core.UriInfo

class SlicesApiRequestSpec extends BaseDataSourceMetadataSpec {

    JerseyTestBinder jtb
    UriInfo uriInfo = Mock(UriInfo)
    UriBuilder builder = Mock(UriBuilder)
    String baseUri = "http://localhost:9998/v1/slices/"
    DataSourceMetadataService dataSourceMetadataService = new DataSourceMetadataService();

    @Shared
    PhysicalTableDictionary fullDictionary

    @Shared
    PhysicalTableDictionary emptyDictionary = new PhysicalTableDictionary()

    def setup() {
        jtb = new JerseyTestBinder(SlicesServlet.class)
        fullDictionary = jtb.configurationLoader.physicalTableDictionary
        uriInfo.getBaseUriBuilder() >> builder
        builder.path(_) >> builder
        builder.path(_, _) >> builder

        DataSourceMetadata dataSourceMetadata = new DataSourceMetadata("all_pets", [:], segments);
        dataSourceMetadataService.update(fullDictionary.get("all_pets"), dataSourceMetadata);
    }

    def cleanup() {
        jtb.tearDown()
    }

    def "check api request construction for the top level endpoint (all slices)"() {
        setup:
        builder.build(_) >> { List<List<String>> args ->
            new URI(baseUri+args[0][0])
        }

        Set<Map<String, String>> expected = fullDictionary.collect {
            Map<String, String> res = new LinkedHashMap<>()
            res.put("name", it.getKey())
            res.put("timeGrain", it.getValue().getTimeGrain().getName().toLowerCase(Locale.ENGLISH))
            res.put("uri", baseUri + it.getKey())
            res
        }

        when:
        SlicesApiRequest apiRequest = new SlicesApiRequest(
                null,
                null,
                "",
                "",
                fullDictionary,
                dataSourceMetadataService,
                uriInfo
        )

        then:
        apiRequest.getSlices() == expected
    }

    def "check api request construction for a given table name"() {
        setup:
        String name = "all_pets"
        Table table = fullDictionary.get(name);
        String uri = baseUri.replaceAll("/slices/.*", "") + "/dimensions/"

        builder.build(_) >> { List<List<String>> args ->
            new URI(uri+args[0][0])
        }

        Set<Map<String, Object>> dimensionsResult = new LinkedHashSet<>()
        Set<Map<String, Object>> metricsResult = new LinkedHashSet<>()

        table.getAvailableIntervals().each {
            Map<String, Object> row = new LinkedHashMap<>()
            row.put("intervals", it.getValue())

            Column key = it.getKey()
            if (key instanceof DimensionColumn) {
                String apiName = ((DimensionColumn) key).getDimension().getApiName()
                row.put("name", apiName)
                row.put("uri", uri + apiName)
                dimensionsResult.add(row)
            } else {
                row.put("name", key.getName())
                metricsResult.add(row)
            }
        }
        Set<SortedMap<DateTime, Map<String, SegmentInfo>>> sliceMetadata = dataSourceMetadataService.getTableSegments(
                Collections.singleton(table)
        );

        Map<String, Object> expected = new LinkedHashMap<>()
        expected.put("name", name);
        expected.put("timeGrain", fullDictionary.get(name).getTimeGrain().getName());
        expected.put("timeZone", "UTC");
        expected.put("dimensions", dimensionsResult);
        expected.put("metrics", metricsResult);
        // This test compares generated metadata against itself, meaning that the contents of this generation are not
        // under test.
        expected.put("segmentInfo", SlicesApiRequest.generateSegmentMetadataView(sliceMetadata))

        when:
        SlicesApiRequest apiRequest = new SlicesApiRequest(
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
            new URI(baseUri+args[0][0])
        }

        when:
        new SlicesApiRequest(
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
