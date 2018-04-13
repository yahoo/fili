// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;


import java.io.IOException;
import java.io.OutputStream;

/**
 * The interface for objects that write fully-processed ResultSets back to the user. This allows customers to fully
 * customize how they choose to serialize the results from Fili based on the request.
 */
@FunctionalInterface
public interface ResponseWriter {
    /**
     * Serializes the ResultSet (pulled from the ResponseData) and any desired metadata and adds it to the specified
     * output stream.
     *
     * @param request  ApiRequest object with all the associated info in it
     * @param responseData  Data object containing all the result information
     * @param os  OutputStream
     *
     * @throws IOException if a problem is encountered writing to the OutputStream
     */
    void write(ApiRequest request, ResponseData responseData, OutputStream os) throws IOException;
}
