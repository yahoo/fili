// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;
import com.yahoo.bard.webservice.util.CacheLastObserver;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.io.EOFException;
import java.util.Optional;

import javax.ws.rs.core.Response.StatusType;

/**
 * Common information for every request that is saved when the logging of a request is finalized.
 * Epilogue takes an Observer that listens for the length of the output stream that was successfully sent to the client.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NON_PRIVATE)
public class Epilogue implements LogInfo {
    protected static final long LENGTH_UNKNOWN = -1;

    protected final String status;
    protected final int code;
    protected final String logMessage;

    private final CacheLastObserver<Long> responseLengthObserver;

    /**
     * Builds the block containing the common information for every request that is saved when the logging of a
     * request is finalized.
     *
     * @param logMessage  The message to log
     * @param response  The status of the response
     * @param responseLengthObserver  An Observer that receives the length of the response streamed back to the client
     * once streaming is complete
     */
    public Epilogue(String logMessage, StatusType response, CacheLastObserver<Long> responseLengthObserver) {
        this.status = response.getReasonPhrase();
        this.code = response.getStatusCode();
        this.logMessage = logMessage;
        this.responseLengthObserver = responseLengthObserver;
    }

    public long getResponseLength() {
        return responseLengthObserver.getLastMessageReceived().orElse(LENGTH_UNKNOWN);
    }

    /**
     * The connection between Bard and the client is considered to be closed prematurely if the connection is closed
     * before Bard finishes streaming the results back to the client.
     * <p>
     * When this happens, an {@link EOFException} is thrown. So as an approximation (it is theoretically possible for
     * an {@link EOFException} to be thrown for other reasons), we consider the connection to be closed prematurely
     * if an {@link EOFException} is generated while sending the response back to the client.
     *
     * @return True if the connection to the client closed before Bard could finish sending the result back to the
     * client
     */
    public boolean isConnectionClosedPrematurely() {
        Optional<Throwable> maybeError = responseLengthObserver.getError();
        return maybeError.isPresent() && maybeError.get() instanceof EOFException;
    }
}
