// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver

import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableSchema

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.function.BinaryOperator

/**
 *  Tests for methods in BasePhysicalResolverSpec
 */
class BasePhysicalTableResolverSpec extends Specification {

    @Shared PhysicalTable one, two, three
    @Shared PhysicalTableMatcher matchAllButTablesNamedOne, matchThree, matchAll
    @Shared BinaryOperator<PhysicalTable> pickFirst, pickLast
    @Shared NoMatchFoundException noMatchNotAny, noMatchNotOne, noMatchThree

    BasePhysicalTableResolver physicalTableResolver
    QueryPlanningConstraint dataSourceConstraint

    def setupSpec() {
        PhysicalTableSchema commonSchema = Mock(PhysicalTableSchema)
        one = Mock(PhysicalTable)
        two = Mock(PhysicalTable)
        three = Mock(PhysicalTable)

        one.getName() >> "one"
        one.getSchema() >> commonSchema
        two.getName() >> "two"
        two.getSchema() >> commonSchema
        three.getName() >> "three"
        three.getSchema() >> commonSchema

        pickFirst = { PhysicalTable table1, PhysicalTable table2 -> table1 } as BinaryOperator<PhysicalTable>
        pickLast  = { PhysicalTable table1, PhysicalTable table2 -> table2 } as BinaryOperator<PhysicalTable>

        noMatchNotAny = new NoMatchFoundException("No Match Found: notAny")
        noMatchNotOne = new NoMatchFoundException("No Match Found: notOne")
        noMatchThree = new NoMatchFoundException("No Match Found: three")

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
        dataSourceConstraint = Mock(QueryPlanningConstraint)

        physicalTableResolver = new BasePhysicalTableResolver() {

            List<PhysicalTableMatcher> matchers
            BinaryOperator<PhysicalTable> betterTable

            @Override
            List<PhysicalTableMatcher> getMatchers(QueryPlanningConstraint requestConstraint) {
                return matchers
            }

            @Override
            BinaryOperator<PhysicalTable> getBetterTableOperator(QueryPlanningConstraint requestConstraint) {
                return betterTable
            }
        }
    }

    @Unroll
    def "Test matchers with no empties to #expected"() {
        setup:
        physicalTableResolver.matchers = matchers

        expect:
        physicalTableResolver.filter([one, two, three], dataSourceConstraint) ==  expected.toSet()

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
        physicalTableResolver.matchers = matchers

        when:
        physicalTableResolver.filter( supply, dataSourceConstraint)

        then:
        NoMatchFoundException noMatchFoundException = thrown()
        noMatchFoundException.message.startsWith('No Match Found: ')


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
        physicalTableResolver.resolve([one, two, three], dataSourceConstraint) == expected

        where:
        matchers                                | better    | expected
        [matchAllButTablesNamedOne]             | pickFirst | two
        [matchAll]                              | pickFirst | one
        [matchAll]                              | pickLast  | three
        [matchThree, matchAllButTablesNamedOne] | pickFirst | three
        [matchAll]                              | pickLast  | three
    }
}
