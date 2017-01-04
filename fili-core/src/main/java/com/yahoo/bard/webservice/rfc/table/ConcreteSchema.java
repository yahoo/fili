// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.rfc.table;

import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.Column;

import java.util.LinkedHashSet;
import java.util.Set;

public class ConcreteSchema extends LinkedHashSet<Column> implements Schema {

    Granularity granularity;

    public ConcreteSchema(Granularity granularity, Set<Column> columns) {
        this.granularity = granularity;
        addAll(columns);
    }

    @Override
    public Granularity getGranularity() {
        return granularity;
    }
}
