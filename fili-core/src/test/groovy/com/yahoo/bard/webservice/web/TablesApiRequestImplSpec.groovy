// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequestImpl
import com.yahoo.bard.webservice.web.apirequest.binders.FilterBinders
import com.yahoo.bard.webservice.web.endpoints.TablesServlet
import com.yahoo.bard.webservice.web.filters.ApiFilters

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.PathSegment

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
    def "test pojo constructor doesn't crash on #desc"() {
        when:
        new TablesApiRequestImpl(
                null, // ResponseFormatType
                null, // downloadFilename
                pagination, // Optional<PaginationParameters>
                tables, // LinkedHashSet<LogicalTable> tables
                null, // LogicalTable
                null, // Granularity
                dimensions, // LinkedHashSet<Dimension> dimensions
                metrics, // LinkedHashSet<LogicalMetric> metrics
                intervals, // List<Interval> intervals
                filters, // ApiFilters filters
        )

        then:
        noExceptionThrown()

        where:
        pagination << [null, Optional.empty()]
        filters << [null, new ApiFilters()]
        tables << [null, new LinkedHashSet<>()]
        dimensions << [null, new LinkedHashSet<>()]
        metrics << [null, new LinkedHashSet<>()]
        intervals << [null, []]
        desc << ["all inputs are null", "relevant inputs are non null but empty"]
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
        // prep dimensions and dimension dictionary
        DimensionField dim1Field = Mock()
        Dimension dim1 = Mock() {
            getApiName() >> "dim1"
            getFieldByName(_ as String) >> { _ -> dim1Field}
        }
        DimensionField dim2Field = Mock()
        Dimension dim2 = Mock() {
            getApiName() >> "dim2"
            getFieldByName(_ as String) >> { _ -> dim2Field}
        }
        Dimension dim3 = Mock() { getApiName() >> "dim3" }

        DimensionDictionary dimensionDictionary = new DimensionDictionary([dim1, dim2, dim3] as Set)

        // prep logical tables
        LogicalTable t1 = Mock(LogicalTable) {
            getName() >> "table1"
            getGranularity() >> DAY
            getDimensions() >> [dim1, dim2, dim3]
            getLogicalMetrics() >> []
        }

        LogicalTable t2 = Mock(LogicalTable) {
            getName() >> "table2"
            getGranularity() >> DAY
        }
        LogicalTableDictionary logicalDictionary = new LogicalTableDictionary()
        logicalDictionary.put(new TableIdentifier(t1), t1)
        logicalDictionary.put(new TableIdentifier(t2), t2)

        // Prep tables servlet (bard config resources)
        tablesServlet.getDimensionDictionary() >> dimensionDictionary
        tablesServlet.getLogicalTableDictionary() >> logicalDictionary
        tablesServlet.getMetricDictionary() >> new MetricDictionary()

        // Setup api filters that will be tested
        ApiFilter r_dim1_filter1 = FilterBinders.getInstance().generateApiFilter("dim1|unused-in[val]", dimensionDictionary)
        ApiFilter r_dim2_filter1 = FilterBinders.getInstance().generateApiFilter("dim2|unused-in[val]", dimensionDictionary)
        ApiFilter t1_dim2_filter1 = r_dim2_filter1
        ApiFilter t1_dim2_filter2 = Mock()
        ApiFilter t1_dim3_filter1 = Mock()
        ApiFilter t2_dim3_filter1 = Mock()

        Optional<ApiFilters> tableFilters_1 = Optional.of(new ApiFilters(
                [
                        (dim2) : [t1_dim2_filter1, t1_dim2_filter2] as Set,
                        (dim3) : [t1_dim3_filter1] as Set,
                ] as Map
        ))
        t1.getFilters() >> tableFilters_1

        // note: these should never be used. They are defined here to ensure we are not accidentally adding filters from logical tables that are NOT part of the query
        Optional<ApiFilters> tableFilters_2 = Optional.of(new ApiFilters(
                [
                        (dim3) : [t2_dim3_filter1] as Set,
                ] as Map
        ))
        t2.getFilters() >> tableFilters_2


        when: "create TARI with basic constructor"
        TablesApiRequestImpl tablesApiRequestImpl = new TablesApiRequestImpl(
                "table1",  // tableName
                "day",  // granularity
                null,  // format
                "",  // per page
                "",  // page
                tablesServlet
        )

        then: "TARI api filters are the filters on the target logical table"
        tablesApiRequestImpl.getApiFilters() == new ApiFilters(
                [
                        (dim2) : [t1_dim2_filter1, t1_dim2_filter2] as Set,
                        (dim3) : [t1_dim3_filter1] as Set,
                ] as Map
        )

        when: "create TARI using complex constructor"
        tablesApiRequestImpl = new TablesApiRequestImpl(
                "table1",  // tableName
                "day",  // granularity
                null,  // format
                "",  // downloadFilename
                "",  // per page
                "",  // page
                tablesServlet,
                [
                        Mock(PathSegment) {getPath() >> "dim1"},
                        Mock(PathSegment) {getPath() >> "dim2"},
                        Mock(PathSegment) {getPath() >> "dim3"},
                ] as List, // dimensions
                "", // metrics
                "current/P1D", // intervals
                "dim1|unused-in[val],dim2|unused-in[val]", // filters
                "UTC" // time zone id
        )

        then: "TARI filters are a union of target table filters and api request"
        tablesApiRequestImpl.getApiFilters() == new ApiFilters(
                [
                        (dim1) : [r_dim1_filter1] as Set,
                        (dim2) : [t1_dim2_filter1, t1_dim2_filter2, r_dim2_filter1] as Set,
                        (dim3) : [t1_dim3_filter1] as Set,
                ] as Map
        )
    }
}
