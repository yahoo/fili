// Copyright 2022 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.table.LogicalTable;

/**
 * An interface for ApiRequests that contain a table. Useful
 * if you want to apply security filters based on table, and you
 * want those filters to apply to any requests (such as those
 * against both the data and tables endpoints) that involve
 * a table.
 */
public interface ApiRequestWithTable extends ApiRequest {

    /**
     * The logical table for this request.
     *
     * @return A logical table
     */
    LogicalTable getTable();
}
