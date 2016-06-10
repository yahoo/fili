// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import java.util.HashMap;

public class TableView extends HashMap<String, Object> {

    public TableView () {
        super();
    }

    public TableView (String key, Object value) {
        this.put(key, value);
    }
}
