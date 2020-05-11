// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.orderBy.antlr;

import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.web.sorts.SortsBaseListener;
import com.yahoo.bard.webservice.web.sorts.SortsParser;

import java.util.ArrayList;
import java.util.List;

public class SortsListListener extends SortsBaseListener {

    private List<OrderByColumn> results = new ArrayList<>();

    private Exception error = null;

    /**
     *  Constructor.
     */
    public SortsListListener() {
    }

    @Override
    public void exitSortsComponent(final SortsParser.SortsComponentContext ctx) {
        String columnName = ctx.metric().getText();
        String orderValue = ctx.orderingValue() != null ? ctx.orderingValue().getText() : null;
        OrderByColumn sort = new OrderByColumn(columnName, orderValue);
        results.add(sort);
    }

    public List<OrderByColumn> getResults() {
        return results;
    }
}
