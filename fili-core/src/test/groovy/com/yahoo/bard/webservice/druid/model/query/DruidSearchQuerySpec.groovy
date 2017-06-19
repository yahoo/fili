// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStore
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider
import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter
import com.yahoo.bard.webservice.druid.model.orderby.DefaultSearchSortDirection
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.TableTestUtils
import com.yahoo.bard.webservice.util.GroovyTestUtils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import org.joda.time.DateTimeZone
import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification

class DruidSearchQuerySpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    @Shared
    DateTimeZone currentTZ

    def setupSpec() {
        currentTZ = DateTimeZone.getDefault()
        DateTimeZone.setDefault(DateTimeZone.forTimeZone(TimeZone.getTimeZone("America/Chicago")))
    }

    def shutdownSpec() {
        DateTimeZone.setDefault(currentTZ)
    }

    DruidSearchQuery defaultQuery(Map vars) {
        vars.queryType = DefaultQueryType.SEARCH
        vars.dataSource = vars.dataSource ?: new TableDataSource(
                TableTestUtils.buildTable(
                        "table_name",
                        DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                        [] as Set,
                        [:],
                        Mock(DataSourceMetadataService)
                )
        )
        vars.granularity = vars.granularity ?: DAY
        vars.filter = vars.filter ?: null
        vars.intervals = vars.intervals ?: []
        vars.searchDimensions = vars.searchDimensions ?: []
        vars.query = vars.query ?: null
        vars.sort = vars.sort ?: null
        vars.limit = vars.limit ?: 50000
        QueryContext initial = new QueryContext(Collections.<QueryContext.Param, Object> emptyMap(), null)
                .withValue(QueryContext.Param.QUERY_ID,"dummy100")
        QueryContext context = vars.context != null ?
                new QueryContext(initial, vars.context as Map).withValue(QueryContext.Param.QUERY_ID, "dummy100") :
                initial

        new DruidSearchQuery(
                vars.dataSource,
                vars.granularity,
                vars.filter,
                vars.intervals,
                vars.searchDimensions,
                vars.query,
                vars.sort,
                vars.limit,
                context,
                false
        )
    }

    def stringQuery(Map vars) {
        vars.queryType = vars.queryType ?: "search"
        vars.dataSource = vars.dataSource ?: '{"type":"table","name":"table_name"}'
        vars.granularity = vars.granularity ?: '{"type":"period","period":"P1D"}'
        vars.filter = vars.filter ? /"filter": $vars.filter,/ : ""
        vars.context = vars.context ?
                /{"queryId":"dummy100",$vars.context}/ :
                /{"queryId": "dummy100"}/
        vars.intervals = vars.intervals ?: "[]"
        vars.searchDimensions = vars.searchDimensions ?: "[]"
        vars.query = vars.query ?: ""
        vars.sort = vars.sort ? /"sort":"$vars.sort",/ : ""
        vars.limit = vars.limit ?: 50000

        ("""{
                "queryType":"$vars.queryType",
                "dataSource":$vars.dataSource,
                "granularity":$vars.granularity,
                $vars.filter
                "intervals":$vars.intervals,
                "searchDimensions":$vars.searchDimensions,
                "query":$vars.query,
                "limit": $vars.limit,
                $vars.sort
                "context":$vars.context
            }""").replaceAll(/\s/, "")
    }

    def "check DruidSearchQuery with RegexSearchQuerySpec serialization"() {
        setup:
        DruidSearchQuery query = defaultQuery(query: new RegexSearchQuerySpec(".*"))
        Map vars = [:]
        vars['query'] =
             """{
                    "pattern": ".*",
                    "type": "regex"
                }"""
        String expectedQuery = stringQuery(vars)

        when: "We create and serialize a RegexSearchQuerySpec"
        String queryStr = MAPPER.writeValueAsString(query)

        then: "The serialized JSON is what we expect"
        GroovyTestUtils.compareJson(queryStr, expectedQuery)

    }

    def "check DruidSearchQuery with FragmentSearchQuerySpec serialization"() {
        setup:
        Dimension dim = new KeyValueStoreDimension(
                "a",
                "blah",
                new LinkedHashSet<DimensionField>(),
                new MapStore(),
                new NoOpSearchProvider(5)
        )
        Dimension dim2 = new KeyValueStoreDimension(
                "b",
                "blah blah",
                new LinkedHashSet<DimensionField>(),
                new MapStore(),
                new NoOpSearchProvider(5)
        )

        List<String> fragments = ["a", "b"]
        DruidSearchQuery query = defaultQuery(
                query: new FragmentSearchQuerySpec(fragments),
                filter: new SelectorFilter(dim, "23"),
                intervals: [new Interval(0, 60000)],
                searchDimensions: [dim, dim2],
                sort: DefaultSearchSortDirection.STRLEN
        )

        Map vars = [:]
        vars['query'] = """
                {
                    "type": "fragment",
                    "values": [
                        "a",
                        "b"
                    ]
                }
        """

        vars['filter'] = """
                {
                    "dimension": "a",
                    "type": "selector",
                    "value": "23"
                }
        """

        vars['intervals'] = """["1969-12-31T18:00:00.000-06:00/1969-12-31T18:01:00.000-06:00"]"""
        vars['searchDimensions'] = """["a", "b"]"""
        vars['sort'] = "STRLEN"

        String expectedQuery = stringQuery(vars)

        when: "We create and serialize a FragmentSearchQuerySpec"
        String queryStr = MAPPER.writeValueAsString(query)

        then: "The serialized JSON is what we expect"
        GroovyTestUtils.compareJson(queryStr, expectedQuery)
    }

    def "check DruidSearchQuery with InsensitiveContainsSearchQuerySpec serialization"() {
        setup:
        DruidSearchQuery query = defaultQuery(query: new InsensitiveContainsSearchQuerySpec("abc"))
        Map vars = [:]
        vars['query'] =
             """{
                    "type": "insensitive_contains",
                    "value": "abc"
                }"""
        String expectedQuery = stringQuery(vars)

        when: "We create and serialize a InsensitiveContainsSearchQuerySpec"
        String queryStr = MAPPER.writeValueAsString(query)

        then: "The serialized JSON is what we expect"
        GroovyTestUtils.compareJson(queryStr, expectedQuery)
    }
}
