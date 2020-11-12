// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol.protocols;

import com.yahoo.bard.webservice.data.metric.protocol.Protocol;

import java.util.Collection;

/**
 * Define and implement a Reaggregation Protocol.
 */
public class ReaggregationProtocol {
    public static final String REAGGREGATION_CONTRACT_NAME = "reaggregation";
    public static final String REAGG_CORE_PARAMETER = "reagg";

    public static final Protocol INSTANCE = new Protocol(
            REAGGREGATION_CONTRACT_NAME,
            REAGG_CORE_PARAMETER,
            new TimeAverageMetricTransformer()
    );

    public static final Collection<String> acceptedValues() {
        return TimeAverageMetricTransformer.acceptedValues();
    }

    /**
     * Private constructor to keep singleton.
     */
    private ReaggregationProtocol() {

    }
}
