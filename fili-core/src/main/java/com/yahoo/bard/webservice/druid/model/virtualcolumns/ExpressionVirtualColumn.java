// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.virtualcolumns;

import com.yahoo.bard.webservice.druid.model.VirtualColumnType;
import com.yahoo.bard.webservice.druid.model.DefaultVirtualColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

/**
 * VirtualColumn to provide queryable column views created from a set of columns during a query.
 */
public class ExpressionVirtualColumn implements VirtualColumn {

    private static final Logger LOG = LoggerFactory.getLogger(ExpressionVirtualColumn.class);

    private final String name;
    private final String expression;
    private final String outputType;

    /**
     * Constructor.
     *
     * @param name  Name of the expression virtual column
     * @param expression  Expression string for the virtual column
     * @param outputType Output type of the virtual column
     */
    public ExpressionVirtualColumn(@NotNull String name, String expression, String outputType) {
        // Check for null name
        if (name == null) {
            String message = "ExpressionVirtualColumn name cannot be null";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
        this.name = name;
        this.expression = expression;
        this.outputType = outputType;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public VirtualColumnType getType() {
        return DefaultVirtualColumnType.EXPRESSION;
    }

    @Override
    public String getExpression() {
        return expression;
    }

    @Override
    public String getOutputType() {
        return outputType;
    }

    @Override
    public ExpressionVirtualColumn withName(String name) {
        return new ExpressionVirtualColumn(name, getExpression(), getOutputType());
    }

    public ExpressionVirtualColumn withExpression(String expression) {
        return new ExpressionVirtualColumn(getName(), expression, getOutputType());
    }

    public ExpressionVirtualColumn withOutputType(String outputType) {
        return new ExpressionVirtualColumn(getName(), getExpression(), outputType);
    }
}
