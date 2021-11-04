// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.time;

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.Granularity;

import org.joda.time.DateTime;

import java.util.Locale;

/**
 * DefaultTimeMacro.
 * <p>
 * Time macros are used as substitute for the actual date values
 */
public enum DefaultTimeMacro implements TimeMacro {

    NEXT_DAY("nextDay", new BoundGrainMacroCalculation(DefaultTimeGrain.DAY)),
    NEXT_WEEK("nextWeek", new BoundGrainMacroCalculation(DefaultTimeGrain.WEEK)),
    NEXT_MONTH("nextMonth", new BoundGrainMacroCalculation(DefaultTimeGrain.MONTH)),
    NEXT_QUARTER("nextQuarter", new BoundGrainMacroCalculation(DefaultTimeGrain.QUARTER)),
    NEXT_YEAR("nextYear", new BoundGrainMacroCalculation(DefaultTimeGrain.YEAR)),
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

    @Override
    public DateTime getDateTime(DateTime dateTime, Granularity granularity) {
        return calculation.getDateTime(dateTime, granularity);
    }
}
