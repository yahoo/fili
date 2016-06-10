// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * TimeMacros
 * <p>
 * Time macros are used as substitute for the actual date values
 */

public enum TimeMacros {

    CURRENT("current", new CurrentMacroCalculation()),
    NEXT("next", new NextMacroCalculation());

    private final String macroName;
    private final MacroCalculationStrategies calculation;

    private static final Map<String, TimeMacros> NAMES_TO_VALUES = Arrays.stream(TimeMacros.values())
            .collect(Collectors.toMap(TimeMacros::name, Function.identity()));

    TimeMacros(String macroName, MacroCalculationStrategies calculation) {
        this.macroName = macroName;
        this.calculation = calculation;
    }

    public String getName() {
        return this.macroName;
    }

    public String toString() {
        return getName().toUpperCase(Locale.ENGLISH);
    }

    public static TimeMacros forName(String name) {
        return NAMES_TO_VALUES.get(name.toUpperCase(Locale.ENGLISH));
    }

    public DateTime getDateTime(DateTime dateTime, TimeGrain timeGrain) {
        return calculation.getDateTime(dateTime, timeGrain);
    }
}
