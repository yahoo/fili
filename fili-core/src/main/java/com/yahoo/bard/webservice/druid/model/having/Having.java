// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.having;

import com.yahoo.bard.webservice.druid.serializers.HasDruidNameSerializer;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * A model for Druid having clauses.
 */
public abstract class Having {

    private final HavingType type;

    /**
     * Constructor.
     *
     * @param type  Type of the Having
     */
    protected Having(HavingType type) {
        this.type = type;
    }

    @JsonSerialize(using = HasDruidNameSerializer.class)
    public HavingType getType() {
        return type;
    }

    /**
     * Allowed values for Having expressions in druid.
     */
    public enum DefaultHavingType implements HavingType {
        AND("and"),
        OR("or"),
        NOT("not"),
        EQUAL_TO("equalTo"),
        LESS_THAN("lessThan"),
        GREATER_THAN("greaterThan");

        private final String druidName;

        /**
         * Constructor.
         *
         * @param druidName  Druid name for the Having
         */
        DefaultHavingType(String druidName) {
            this.druidName = druidName;
        }

        @Override
        public String getDruidName() {
            return druidName;
        }
    }
}
