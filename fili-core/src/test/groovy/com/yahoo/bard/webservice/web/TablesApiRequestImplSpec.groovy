// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequestImpl
import com.yahoo.bard.webservice.web.endpoints.TablesServlet

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
                null,
                null,
                null,
                "",
                "",
                null,
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
                null,
                null,
                "",
                "",
                null,
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
                null,
                "",
                "",
                null,
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
                null,
                "",
                "",
                null,
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
}
