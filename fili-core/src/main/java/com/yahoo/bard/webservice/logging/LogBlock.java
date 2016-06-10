// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.logging;

import com.fasterxml.jackson.annotation.JsonAnyGetter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents the information that is written in a single log line per http request.
 * Information is progressively appended to this context according to the handling path of the request.
 * Finally, the LogBlock is exported as JSON string and written to the appropriate output via the logging framework.
 */
public class LogBlock {

    public final String uuid;
    private final Map<String, LogInfo> body;

    /**
     * Given a universally unique identifier as string construct a logging context.
     *
     * @param uuid  A string identifier that is meant to be unique across the logfile.
     */
    public LogBlock(String uuid) {
        this.uuid = uuid;
        this.body = new LinkedHashMap<>();
    }

    /**
     * Append a part of logging information defined in this {@link LogBlock}.
     *
     * @param phase  A {@link LogInfo} object including the information to be logged.
     */
    public void add(LogInfo phase) {
        body.put(phase.getName(), phase);
    }

    /**
     * Add an entry of given class in this {@link LogBlock} that holds no information.
     * It is meant to be used only to define the order of {@link LogInfo} parts inside a {@link LogBlock}.
     *
     * @param phaseClass  A string to identify the class that will hold the logs passed as arguments.
     */
    protected void add(Class<? extends LogInfo> phaseClass) {
        body.put(phaseClass.getSimpleName(), null);
    }

    @JsonAnyGetter
    public Map<String, LogInfo> any() {
        return body;
    }
}
