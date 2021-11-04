// Copyright 2021 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.time;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * DefaultTimeMacro.
 * <p>
 * Time macros are used as substitute for the actual date values
 */

public interface TimeMacro {

    Map<String, ? extends TimeMacro> NAMES_TO_VALUES = Arrays.stream(DefaultTimeMacro.values())
            .collect(Collectors.toMap(macro -> macro.getName().toUpperCase(Locale.US), Function.identity()));

    /**
     * Get the time macro for the given name.
     *
     * @param name  Name to find the time macro for
     *
     * @return the time macro that matches
     */
    static TimeMacro forName(String name) {
        return NAMES_TO_VALUES.get(name.toUpperCase(Locale.ENGLISH));
    }
    /**
     * The string name of the macro as expressed in the interval request.
     *
     * @return  The name of the macro in the request.
     */
    String getName();

    /**
     * Calculate the macro-adjusted DateTime under the TimeGrain.
     *
     * @param dateTime  DateTime to adjust
     * @param timeGrain  TimeGrain to adjust the DateTime for
     *
     * @return the macro-adjusted DateTime
     */
    DateTime getDateTime(DateTime dateTime, TimeGrain timeGrain);

}
