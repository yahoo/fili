// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
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

    def "Make OR from AND having"() {
        Having having1 = Mock(Having)
        Having having2 = Mock(Having)
        AndHaving andHaving = new AndHaving([having1, having2])

        when:
        OrHaving orHaving = andHaving.asOrHaving()

        then:
        orHaving.type == DefaultHavingType.OR
        orHaving.havings == [having1, having2]
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

    def "Add one to AND having"() {
        Having having1 = Mock(Having)
        Having having2 = Mock(Having)
        AndHaving andHaving = new AndHaving([having1, having2])

        when:
        Having having3 = Mock(Having)
        AndHaving andHaving3 = andHaving.plusHaving(having3)

        then:
        andHaving3.havings as Set == [having1, having2, having3] as Set
    }

    def "Add list to AND having"() {
        Having having1 = Mock(Having)
        Having having2 = Mock(Having)
        AndHaving andHaving = new AndHaving([having1, having2])

        when:
        AndHaving having3 = Mock(AndHaving)
        AndHaving having4 = Mock(AndHaving)
        AndHaving andHaving4 = andHaving.plusHavings([having3, having4])

        then:
        andHaving4.havings as Set == [ having1, having2, having3, having4] as Set
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

    def "Make AND from OR having"() {
        Having having1 = Mock(Having)
        Having having2 = Mock(Having)
        OrHaving orHaving = new OrHaving([having1, having2])

        when:
        AndHaving andHaving = orHaving.asAndHaving()

        then:
        andHaving.type == DefaultHavingType.AND
        andHaving.havings == [having1, having2]
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

    def "Add one to OR having"() {
        Having having1 = Mock(Having)
        Having having2 = Mock(Having)
        OrHaving orHaving = new OrHaving([having1, having2])

        when:
        OrHaving having3 = Mock(OrHaving)
        OrHaving orHaving3 = orHaving.plusHaving(having3)

        then:
        orHaving3.havings as Set == [having1, having2, having3] as Set
    }

    def "Add list to OR having"() {
        Having having1 = Mock(Having)
        Having having2 = Mock(Having)
        OrHaving orHaving = new OrHaving([having1, having2])

        when:
        OrHaving having3 = Mock(OrHaving)
        OrHaving having4 = Mock(OrHaving)
        OrHaving orHaving4 = orHaving.plusHavings([having3, having4])

        then:
        orHaving4.havings as Set == [ having1, having2, having3, having4] as Set
    }
}
