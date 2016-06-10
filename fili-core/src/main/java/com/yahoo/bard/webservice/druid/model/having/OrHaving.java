// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.having;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Having clause model for logical OR applied to a set of druid having expressions
 */
public class OrHaving extends MultiClauseHaving {

    public OrHaving(List<Having> havings) {
        super(DefaultHavingType.OR, havings);
    }

    @Override
    public OrHaving withHavings(List<Having> havings) {
        return new OrHaving(havings);
    }

    @Override
    public MultiClauseHaving plusHaving(Having having) {
        return new OrHaving(Stream.concat(getHavings().stream(), Stream.of(having)).collect(Collectors.toList()));
    }

    @Override
    public MultiClauseHaving plusHavings(List<Having> havings) {
        return new OrHaving(Stream.concat(havings.stream(), getHavings().stream()).collect(Collectors.toList()));
    }

    public AndHaving asAndHaving() {
        return new AndHaving(getHavings());
    }
}
