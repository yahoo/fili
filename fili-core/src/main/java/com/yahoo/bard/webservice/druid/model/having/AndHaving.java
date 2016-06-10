// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.having;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Having clause model for logical AND applied to a set of druid having expressions
 */
public class AndHaving extends MultiClauseHaving {

    public AndHaving(List<Having> havings) {
        super(DefaultHavingType.AND, havings);
    }

    @Override
    public AndHaving withHavings(List<Having> havings) {
        return new AndHaving(havings);
    }

    @Override
    public MultiClauseHaving plusHaving(Having having) {
        return new AndHaving(Stream.concat(getHavings().stream(), Stream.of(having)).collect(Collectors.toList()));
    }

    @Override
    public MultiClauseHaving plusHavings(List<Having> havings) {
        return new AndHaving(Stream.concat(havings.stream(), getHavings().stream()).collect(Collectors.toList()));
    }

    public OrHaving asOrHaving() {
        return new OrHaving(getHavings());
    }
}
