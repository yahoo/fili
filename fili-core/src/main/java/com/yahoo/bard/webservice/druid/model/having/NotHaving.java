// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.having;

import com.fasterxml.jackson.annotation.JsonGetter;

/**
 * Having clause model for logical NOT applied to a  having expressions.
 */
public class NotHaving extends Having {

    private final Having having;

    /**
     * Constructor.
     *
     * @param having  Having to negate
     */
    public NotHaving(Having having) {
        super(DefaultHavingType.NOT);
        this.having = having;
    }

    @JsonGetter("havingSpec")
    public Having getHaving() {
        return having;
    }
}
