// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequestImpl
import com.yahoo.bard.webservice.web.endpoints.TablesServlet
import com.yahoo.bard.webservice.web.filters.ApiFilters
import com.yahoo.bard.webservice.web.util.PaginationParameters

import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class TablesApiRequestImplSpec extends Specification {

    JerseyTestBinder jtb

    @Shared
    LogicalTableDictionary fullDictionary

    @Shared
    LogicalTableDictionary emptyDictionary = new LogicalTableDictionary()

    TablesServlet tablesServlet = Mock(TablesServlet)

    def setup() {
        jtb = new JerseyTestBinder(TablesServlet.class)
        fullDictionary = jtb.configurationLoader.logicalTableDictionary
        tablesServlet.getGranularityParser() >> new StandardGranularityParser()

    }

    def cleanup() {
        jtb.tearDown()
    }

    def "check api request construction for the top level endpoint (all tables)"() {
        setup:
        tablesServlet.getLogicalTableDictionary() >> fullDictionary

        when:
        TablesApiRequestImpl tablesApiRequestImpl = new TablesApiRequestImpl(
                null,  // tableName
                null,  // granularity
                null,  // format
                "",  // per page
                "",  // page
                tablesServlet
        )

        then:
        tablesApiRequestImpl.getTables() as Set == fullDictionary.values() as Set
    }

    def "check api request construction for a given table name"() {
        setup:
        String name = "pets"
        tablesServlet.getLogicalTableDictionary() >> fullDictionary


        when:
        TablesApiRequestImpl tablesApiRequestImpl = new TablesApiRequestImpl(
                name,
                null,  // granularity
                null,  // format
                "",  // perPage
                "",  // page
                tablesServlet
        )

        then:
        tablesApiRequestImpl.getTables() == fullDictionary.values().findAll({ it.getName() == name }) as Set
    }

    def "check api request construction for a given table name and a given granularity"() {
        setup:
        TableGroup tg = Mock(TableGroup)
        tg.getApiMetricNames() >> ([] as Set)
        tg.getDimensions() >> ([] as Set)
        LogicalTable table = new LogicalTable("pets", DAY, tg, new MetricDictionary())
        String name = "pets"
        tablesServlet.getLogicalTableDictionary() >> fullDictionary

        when:
        TablesApiRequestImpl tablesApiRequestImpl = new TablesApiRequestImpl(
                name,
                "day",
                null,  // format
                "",  // perPage
                "", // page
                tablesServlet
        )

        then:
        tablesApiRequestImpl.getTable() == table
    }

    @Unroll
    def "api request construction throws #exception.simpleName because #reason"() {
        tablesServlet.getLogicalTableDictionary() >> dictionary

        when:
        new TablesApiRequestImpl(
                name,
                grain,
                null,  // format
                "",  // perPage
                "",  // page
                tablesServlet
        )

        then:
        Exception e = thrown(exception)
        e.getMessage().matches(reason)

        where:
        name     | grain     | dictionary      | exception              | reason
        "pets"   | "day"     | emptyDictionary | BadApiRequestException | ".*Logical Table Dictionary is empty.*"
        "beasts" | "day"     | fullDictionary  | BadApiRequestException | ".*Table name.*does not exist.*"
        "pets"   | "century" | fullDictionary  | BadApiRequestException | ".*not a valid granularity.*"
        "pets"   | "hour"    | fullDictionary  | BadApiRequestException | "Invalid pair of granularity .* and table.*"
    }

    def "test request api filters and logical tables filters are properly merged"() {
        setup:
        Dimension dim1 = Mock()
        Dimension dim2 = Mock()
        Dimension dim3 = Mock()

        ApiFilter r_dim1_filter1 = Mock()
        ApiFilter r_dim2_filter1 = Mock()
        ApiFilter t1_dim2_filter1 = r_dim2_filter1
        ApiFilter t1_dim2_filter2 = Mock()
        ApiFilter t2_dim3_filter1 = Mock()

        ApiFilters requestFilters = new ApiFilters(
                [
                        (dim1) : [r_dim1_filter1] as Set,
                        (dim2) : [r_dim2_filter1] as Set
                ] as Map
        )

        ApiFilters tableFilters_1 = new ApiFilters(
                [
                        (dim2) : [t1_dim2_filter1, t1_dim2_filter2] as Set,
                ] as Map
        )
        LogicalTable t1 = Mock(LogicalTable) {getFilters() >> tableFilters_1}

        ApiFilters tableFilters_2 = new ApiFilters(
                [
                        (dim3) : [t2_dim3_filter1] as Set,
                ] as Map
        )
        LogicalTable t2 = Mock(LogicalTable) {getFilters() >> tableFilters_2}

        expect:
        TablesApiRequestImpl tablesApiRequestImpl = new TablesApiRequestImpl(
                null, // ResponseFormatType
                null, // downloadFilename
                null, // paginationParameters,
                [t1, t2] as LinkedHashSet,
                LogicalTable table,
                Granularity granularity,
                LinkedHashSet<Dimension> dimensions,
                LinkedHashSet<LogicalMetric> metrics,
                List<Interval> intervals,
                ApiFilters filters
        )
    }
}
