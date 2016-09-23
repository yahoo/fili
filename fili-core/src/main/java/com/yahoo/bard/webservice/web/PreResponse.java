// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext;

import java.util.Objects;

/**
 * PreReponse is an encapsulation of ResultSet and ResponseContext. ResultSet and ResponseContext can be extracted from
 * it to build a Response
 */
public class PreResponse {
    private final ResultSet resultSet;
    private final ResponseContext responseContext;

    /**
     * Build PreResponse using the given ResultSet and ResponseContext.
     *
     * @param resultSet  ResultSet associated with a response
     * @param responseContext  ResponseContext associated with a response
     */
    public PreResponse(ResultSet resultSet, ResponseContext responseContext) {
        this.resultSet = resultSet;
        this.responseContext = responseContext;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public ResponseContext getResponseContext() {
        return responseContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        PreResponse that = (PreResponse) o;
        return
                Objects.equals(resultSet, that.resultSet) &&
                Objects.equals(responseContext, that.responseContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultSet, responseContext);
    }
}
