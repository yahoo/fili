// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.time;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import org.joda.time.DateTime;

import java.util.Locale;

/**
 * DefaultTimeMacro.
 * <p>
 * Time macros are used as substitute for the actual date values
 */
public enum DefaultTimeMacro implements TimeMacro {

    CURRENT("current", new CurrentMacroCalculation()),
    NEXT("next", new NextMacroCalculation());

    private final String macroName;
    private final MacroCalculationStrategies calculation;

    /**
     * Constructor.
     *
     * @param macroName  Name of the macro
     * @param calculation  Calculation for the macro
     */
    DefaultTimeMacro(String macroName, MacroCalculationStrategies calculation) {
        this.macroName = macroName;
        this.calculation = calculation;
    }

    @Override
    public String getName() {
        return macroName;
    }

    @Override
    public String toString() {
        return getName().toUpperCase(Locale.ENGLISH);
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
