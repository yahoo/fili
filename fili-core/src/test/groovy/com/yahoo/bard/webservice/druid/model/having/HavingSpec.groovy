// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.having

import com.yahoo.bard.webservice.druid.model.having.Having.DefaultHavingType

import spock.lang.Specification


class HavingSpec extends Specification {

    def "Make numeric having"() {
        when:
        String aggregation = "metricName"
        NumericHaving numericHaving = new NumericHaving(DefaultHavingType.EQUAL_TO, aggregation, 5.0)

        then:
        numericHaving.aggregation == aggregation
        numericHaving.type == DefaultHavingType.EQUAL_TO
        numericHaving.value == 5.0
    }

    def "Make NOT having"() {
        when:
        Having having = Mock(Having)
        NotHaving notHaving = new NotHaving(having)

        then:
        notHaving.type == DefaultHavingType.NOT
        notHaving.having == having
    }

    def "Make AND having"() {
        when:
        Having having1 = Mock(Having)
        Having having2 = Mock(Having)
        AndHaving andHaving = new AndHaving([having1, having2])

        then:
        andHaving.type == DefaultHavingType.AND
        andHaving.havings == [having1, having2]
    }

    def "From list to AND having"() {
        Having having1 = Mock(Having)
        Having having2 = Mock(Having)
        AndHaving andHaving = new AndHaving([having1, having2])

        when:
        AndHaving having3 = Mock(AndHaving)
        AndHaving having4 = Mock(AndHaving)
        AndHaving andHaving2 = andHaving.withHavings([having3, having4])

        then:
        andHaving2.havings == [having3, having4]
    }

    def "Make OR having"() {
        when:
        Having having1 = Mock(Having)
        Having having2 = Mock(Having)
        OrHaving orHaving = new OrHaving([having1, having2])

        then:
        orHaving.type == DefaultHavingType.OR
        orHaving.havings == [having1, having2]
    }

    def "From list to OR having"() {
        Having having1 = Mock(Having)
        Having having2 = Mock(Having)
        OrHaving orHaving = new OrHaving([having1, having2])

        when:
        OrHaving having3 = Mock(OrHaving)
        OrHaving having4 = Mock(OrHaving)
        OrHaving orHaving2 = orHaving.withHavings([having3, having4])

        then:
        orHaving2.havings == [having3, having4]
    }
}
