// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

/**
 * A type that an api requested response can be bound to.
 */

public interface ResponseFormatType {

    /**
     * Does this Response Format accept this api response format string.
     *
     * @param responseFormatValue  The api string for the requested format
     *
     * @return  True if this format type can accept this value.
     */
    boolean accepts(String responseFormatValue);

    /**
     * Does this response format accept this reponse format.
     * (Replaces 'equals' to allow a more general format to accept more specific ones)
     *
     * @param formatType  The format type object requested by the user.
     *
     * @return  True if this format type is compatible with another..
     */

    boolean accepts(ResponseFormatType formatType);
}
