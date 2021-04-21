// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.virtualcolumns

import com.yahoo.bard.webservice.druid.model.VirtualColumnType
import com.yahoo.bard.webservice.druid.model.DefaultVirtualColumnType

import spock.lang.Specification

class ExpressionVirtualColumnSpec extends Specification {

    String name
    String expression
    String outputType
    VirtualColumnType type

    def setup() {
        name = "fooPage"
        expression = "concat('foo' + page)"
        outputType = "STRING"
        type = DefaultVirtualColumnType.EXPRESSION
    }

    def "Constructor creation"() {
        when:
        ExpressionVirtualColumn expressionVirtualColumn = new ExpressionVirtualColumn(name, expression, outputType)

        then:
        expressionVirtualColumn.name == name
        expressionVirtualColumn.expression == expression
        expressionVirtualColumn.outputType == outputType
        expressionVirtualColumn.type == type
    }

    def "Constructor throws error with null name"() {
        when:
        String nullName
        ExpressionVirtualColumn expressionVirtualColumn = new ExpressionVirtualColumn(nullName, expression, outputType)

        then: "ExpressionVirtualColumn name cannot be null"
        thrown(IllegalArgumentException)
    }

    def "Virtual column creation with name"() {
        setup:
        ExpressionVirtualColumn expressionVirtualColumn = new ExpressionVirtualColumn(name, expression, outputType)
        String newName = "barPage"

        when:
        ExpressionVirtualColumn newExpressionVirtualColumn = expressionVirtualColumn.withName(newName)

        then:
        newExpressionVirtualColumn.name == newName
        newExpressionVirtualColumn.expression == expression
        newExpressionVirtualColumn.outputType == outputType
        newExpressionVirtualColumn.type == type
    }

    def "Virtual column creation with expression"() {
        setup:
        ExpressionVirtualColumn expressionVirtualColumn = new ExpressionVirtualColumn(name, expression, outputType)
        String newExpression = "concat('bar' + page)"

        when:
        ExpressionVirtualColumn newExpressionVirtualColumn = expressionVirtualColumn.withExpression(newExpression)

        then:
        newExpressionVirtualColumn.name == name
        newExpressionVirtualColumn.expression == newExpression
        newExpressionVirtualColumn.outputType == outputType
        newExpressionVirtualColumn.type == type
    }

    def "Virtual column creation with output type"() {
        setup:
        ExpressionVirtualColumn expressionVirtualColumn = new ExpressionVirtualColumn(name, expression, outputType)
        String newOutputType = "LONG"

        when:
        ExpressionVirtualColumn newExpressionVirtualColumn = expressionVirtualColumn.withOutputType(newOutputType)

        then:
        newExpressionVirtualColumn.name == name
        newExpressionVirtualColumn.expression == expression
        newExpressionVirtualColumn.outputType == newOutputType
        newExpressionVirtualColumn.type == type
    }
}
