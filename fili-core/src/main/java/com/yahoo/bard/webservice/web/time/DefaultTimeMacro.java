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

    CURRENT_DAY("currentDay", new BoundGrainMacroCalculation(DefaultTimeGrain.DAY)),
    CURRENT_WEEK("currentWeek", new BoundGrainMacroCalculation(DefaultTimeGrain.WEEK)),
    CURRENT_MONTH("currentMonth", new BoundGrainMacroCalculation(DefaultTimeGrain.MONTH)),
    CURRENT_QUARTER("currentQuarter", new BoundGrainMacroCalculation(DefaultTimeGrain.QUARTER)),
    CURRENT_YEAR("currentYear", new BoundGrainMacroCalculation(DefaultTimeGrain.YEAR)),

    NEXT_DAY("nextDay", new BoundGrainMacroNextCalculation(DefaultTimeGrain.DAY)),
    NEXT_WEEK("nextWeek", new BoundGrainMacroNextCalculation(DefaultTimeGrain.WEEK)),
    NEXT_MONTH("nextMonth", new BoundGrainMacroNextCalculation(DefaultTimeGrain.MONTH)),
    NEXT_QUARTER("nextQuarter", new BoundGrainMacroNextCalculation(DefaultTimeGrain.QUARTER)),
    NEXT_YEAR("nextYear", new BoundGrainMacroNextCalculation(DefaultTimeGrain.YEAR)),

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
