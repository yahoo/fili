// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.builders

import com.yahoo.bard.webservice.druid.model.having.AndHaving
import com.yahoo.bard.webservice.druid.model.having.Having
import com.yahoo.bard.webservice.druid.model.having.NotHaving
import com.yahoo.bard.webservice.druid.model.having.NumericHaving
import com.yahoo.bard.webservice.druid.model.having.OrHaving

public class Havings {
    public static AndHaving and(Having... havings) {
        return new AndHaving(Arrays.asList(havings))
    }


    public static OrHaving or(Having... havings) {
        return new OrHaving(Arrays.asList(havings))
    }


    public static NotHaving not(Having havings) {
        return new NotHaving(havings)
    }

    public static NumericHaving equals(String name, Number value) {
        return new NumericHaving(Having.DefaultHavingType.EQUAL_TO, name, value);
    }

    public static NumericHaving gt(String name, Number value) {
        return new NumericHaving(Having.DefaultHavingType.GREATER_THAN, name, value);
    }

    public static NumericHaving lt(String name, Number value) {
        return new NumericHaving(Having.DefaultHavingType.LESS_THAN, name, value);
    }
}
