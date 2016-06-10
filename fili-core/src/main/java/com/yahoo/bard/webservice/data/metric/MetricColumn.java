// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.Schema;

/**
 * Metric
 */
public class MetricColumn extends Column {
    /**
     * Constructor
     *
     * @param name  The column name
     */
    protected MetricColumn(String name) {
        super(name);
    }

    /**
     * Method to create a MetricColumn tied to a schema
     *
     * @param schema - The schema for this column to be added
     * @param name - The name for this metric column
     * @return the new column
     */
    public static MetricColumn addNewMetricColumn(Schema schema, String name) {
        MetricColumn col = new MetricColumn(name);
        schema.addColumn(col);
        return col;
    }

    @Override
    public String toString() {
        return "{metric:'" + getName() + "'}";
    }
}
