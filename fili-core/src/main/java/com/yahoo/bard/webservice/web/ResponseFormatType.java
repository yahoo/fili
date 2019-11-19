// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import javax.ws.rs.core.MediaType;

/**
 * A type that an api requested response can be bound to.
 */

public interface ResponseFormatType {

    String TEXT_FILE_EXTENSION = ".txt";
    String CSV_CONTENT_TYPE = "text/csv";
    String CHARSET_UTF8 = "utf-8";

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

    /**
     * Provides the file extension for the response format type, if the response is downloaded as a file.
     *
     * @return the file extension. includes the '.'
     */
    default String getFileExtension() {
        return TEXT_FILE_EXTENSION;
    }

    /**
     * Provides the value for the content type header.
     *
     * @return the content type
     */
    default String getContentType() {
        return MediaType.TEXT_PLAIN;
    }

    /**
     * Provides the charset for the content type header.
     *
     * @return the charset
     */
    default String getCharset() {
        return CHARSET_UTF8;
    }
}
