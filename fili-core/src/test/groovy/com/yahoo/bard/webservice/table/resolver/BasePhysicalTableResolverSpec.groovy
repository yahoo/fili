// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver

import com.google.common.collect.Sets
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.web.DataApiRequest

import org.joda.time.DateTimeZone
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.function.BinaryOperator
/**
 *  Tests for methods in BasePhysicalResolverSpec
 */
class BasePhysicalTableResolverSpec extends Specification {

    List<PhysicalTable> tables
    @Shared PhysicalTableMatcher matchAllButTablesNamedOne, matchThree, matchAll
    @Shared BinaryOperator<PhysicalTable> pickFirst
    @Shared BinaryOperator<PhysicalTable> pickLast
    @Shared PhysicalTable one = Mock(PhysicalTable)
    @Shared PhysicalTable two = Mock(PhysicalTable)
    @Shared PhysicalTable three = Mock(PhysicalTable)

    @Shared NoMatchFoundException noMatchNotAny = new NoMatchFoundException("notAny")
    @Shared NoMatchFoundException noMatchNotOne = new NoMatchFoundException("notOne")
    @Shared NoMatchFoundException noMatchThree = new NoMatchFoundException("three")

    DataApiRequest request = Mock(DataApiRequest)
    TemplateDruidQuery query = Mock(TemplateDruidQuery)

    BasePhysicalTableResolver physicalTableResolver = new BasePhysicalTableResolver() {

        List<PhysicalTableMatcher> matchers
        BinaryOperator<PhysicalTable> betterTable

        @Override
        List<PhysicalTableMatcher> getMatchers(DataApiRequest apiRequest, TemplateDruidQuery query) {
            return matchers
        }

        @Override
        BinaryOperator<PhysicalTable> getBetterTableOperator(DataApiRequest apiRequest, TemplateDruidQuery query) {
            return betterTable
        }
    }


    def setupSpec() {
        one.getName() >> "one"
        one.toString() >> "one"
        two.getName() >> "two"
        two.toString() >> "two"
        three.getName() >> "three"
        three.toString() >> "three"

        pickFirst = { PhysicalTable table1, PhysicalTable table2 -> table1 }
        pickLast  = { PhysicalTable table1, PhysicalTable table2 -> table2 }
        matchAll = new PhysicalTableMatcher() {
            @Override
            boolean test(PhysicalTable table) {
                return true
            }

            @Override
            NoMatchFoundException noneFoundException() {
                return noMatchNotAny
            }

            @Override
            String toString() {
                return "Match All"
            }
        }

        matchAllButTablesNamedOne = new PhysicalTableMatcher() {
            @Override
            boolean test(PhysicalTable table) {
                return table.getName() != "one"
            }

            @Override
            NoMatchFoundException noneFoundException() {
                return noMatchNotOne
            }

            @Override
            String toString() {
                return "Match Not One"
            }
        }

        matchThree = new PhysicalTableMatcher() {
            @Override
            boolean test(PhysicalTable table) {
                return table.getName() == "three"
            }

            @Override
            NoMatchFoundException noneFoundException() {
                return noMatchThree
            }

            @Override
            String toString() {
                return "Match Three"
            }
        }
    }

    def setup() {
        request.getGranularity() >> AllGranularity.INSTANCE
        query.getInnermostQuery() >> query
        query.getDimensions() >> []
        query.getMetricDimensions() >> ([] as Set)
        query.getDependentFieldNames() >> ([] as Set)
        request.getFilterDimensions() >> []
        request.getDimensions() >> ([] as Set)

        LogicalTable logical = Mock(LogicalTable.class)
        TableGroup group = Mock(TableGroup.class)
        logical.getTableGroup() >> group
        group.getPhysicalTables() >> Sets.newHashSet(new PhysicalTable("table_name", DefaultTimeGrain.DAY.buildZonedTimeGrain(DateTimeZone.UTC), [:]))
        request.getTable() >> logical
    }
    @Unroll
    def "Test matchers with no empties to #expected"() {
        setup:
        physicalTableResolver.matchers = matchers as List

        expect:
        physicalTableResolver.filter( [one, two, three] as List, request, query) ==  expected as Set

        where:
        matchers                                | expected
        [matchAllButTablesNamedOne]             | [two, three]
        [matchThree]                            | [three]
        [matchThree, matchAllButTablesNamedOne] | [three]
        [matchAllButTablesNamedOne, matchThree] | [three]
    }

    @Unroll
    def "Test matchers with throw no match exception with #matchers and #supply"() {
        setup:
        physicalTableResolver.matchers = matchers as List

        when:
        physicalTableResolver.filter( supply as List, request, query)

        then:
        thrown(NoMatchFoundException)

        where:
        matchers                                | supply       | noMatch
        [matchAllButTablesNamedOne]             | [one]        | noMatchNotOne
        [matchAllButTablesNamedOne, matchThree] | [one]        | noMatchNotOne
        [matchAllButTablesNamedOne, matchThree] | [two]        | noMatchThree
        [matchThree]                            | [one, two]   | noMatchThree
        [matchThree, matchAllButTablesNamedOne] | [one]        | noMatchThree
    }

    @Unroll
    def "Test resolve with #matchers and #better into #expected"() {
        setup:
        physicalTableResolver.matchers = matchers
        physicalTableResolver.betterTable = better

        expect:
        physicalTableResolver.resolve([one, two, three], request, query) == expected

        where:
        matchers                                | better    | expected
        [matchAllButTablesNamedOne]             | pickFirst | two
        [matchAll]                              | pickFirst | one
        [matchAll]                              | pickLast  | three
        [matchThree, matchAllButTablesNamedOne] | pickFirst | three
        [matchAll]                              | pickLast  | three
    }
}
