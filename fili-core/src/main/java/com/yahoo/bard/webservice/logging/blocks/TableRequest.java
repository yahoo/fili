// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * Main log of a request served by the TablesServlet.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class TableRequest implements LogInfo {
    protected final String resource = "tables";
    protected final String table;
    protected final String grain;

    public TableRequest(String table, String grain) {
        this.table = table;
        this.grain = grain;
    }
}
