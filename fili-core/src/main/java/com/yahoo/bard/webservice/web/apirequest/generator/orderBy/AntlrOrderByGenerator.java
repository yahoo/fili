// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.orderBy;

import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.web.apirequest.generator.orderBy.antlr.ProtocolAntlrSortParser;

import java.util.List;

/**
 * OrderByGenerator that uses SortsListener to parse the api request string.
 */
public class AntlrOrderByGenerator extends DefaultOrderByGenerator {

    private final ProtocolAntlrSortParser parser = new ProtocolAntlrSortParser();

    protected List<OrderByColumn> parseOrderByColumns(String sortsRequest) {
        return parser.apply(sortsRequest);
    }
}
