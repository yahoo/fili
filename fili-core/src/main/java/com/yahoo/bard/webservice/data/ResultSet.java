// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import java.util.ArrayList;
import java.util.List;

/**
 * ResultSet.
 */
public class ResultSet extends ArrayList<Result> {

    private final ResultSetSchema schema;

    /**
     * Constructor.
     *
     * @param schema  The associated schema
     * @param results  The list of results
     */
    public ResultSet(ResultSetSchema schema, List<Result> results) {
        super(results);
        this.schema = schema;
    }

    /**
     * Getter for ResultSet schema.
     *
     * @return The schema associated with this result set
     */
    public ResultSetSchema getSchema() {
        return this.schema;
    }

    @Override
    public String toString() {
        return "Schema: " + schema;
    }
}
