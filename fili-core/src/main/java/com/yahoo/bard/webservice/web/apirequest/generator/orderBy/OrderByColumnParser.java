// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.orderBy;

import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadOrderByException;

import java.util.List;

public interface OrderByColumnParser {
    List<OrderByColumn> apply(
            String orderByQuery
    ) throws BadOrderByException;
}
