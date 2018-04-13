// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;


import java.util.Optional;

/**
 * Typically, a Fili program's response writing infrastructure will consist of a collection of unrelated
 * ResponseWriters each responsible for serializing the ResultSet into a specific format, and a single
 * ResponseWriter that chooses the correct one based on the current state of the DataApiRequest.
 * This interface allows customers provide a clean interface to the logic that makes that selection. Its
 * sole purpose is to take a `DataApiRequest` and return the `ResponseWriter` that should be used
 * to write the response.
 */
@FunctionalInterface
public interface ResponseWriterSelector {
    /**
     * Select ResponseWriter given certain type of format from DataApiRequest.
     *
     * @param request  ApiRequest object with all the associated info in it
     *
     * @return  Writer of given format type
     */
    Optional<ResponseWriter> select(ApiRequest request);
}
