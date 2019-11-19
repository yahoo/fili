// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * TimeMacro.
 * <p>
 * Time macros are used as substitute for the actual date values
 */
public enum TimeMacro {

    CURRENT("current", new CurrentMacroCalculation()),
    NEXT("next", new NextMacroCalculation());

    private final String macroName;
    private final MacroCalculationStrategies calculation;

    private static final Map<String, TimeMacro> NAMES_TO_VALUES = Arrays.stream(TimeMacro.values())
            .collect(Collectors.toMap(TimeMacro::name, Function.identity()));

    /**
     * Constructor.
     *
     * @param macroName  Name of the macro
     * @param calculation  Calculation for the macro
     */
    TimeMacro(String macroName, MacroCalculationStrategies calculation) {
        this.macroName = macroName;
        this.calculation = calculation;
    }

    public String getName() {
        return macroName;
    }

    @Override
    public String toString() {
        return getName().toUpperCase(Locale.ENGLISH);
    }

    /**
     * Get the time macro for the given name.
     *
     * @param name  Name to find the time macro for
     *
     * @return the time macro that matches
     */
    public static TimeMacro forName(String name) {
        return NAMES_TO_VALUES.get(name.toUpperCase(Locale.ENGLISH));
    }

    /**
     * Calculate the macro-adjusted DateTime under the TimeGrain.
     *
     * @param dateTime  DateTime to adjust
     * @param timeGrain  TimeGrain to adjust the DateTime for
     *
     * @return the macro-adjusted DateTime
     */
    public DateTime getDateTime(DateTime dateTime, TimeGrain timeGrain) {
        return calculation.getDateTime(dateTime, timeGrain);
    }
}
