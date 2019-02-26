// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import java.util.Locale;
import java.util.Objects;

import javax.ws.rs.core.MediaType;

/**
 * Standard reponse formats.
 */
public enum DefaultResponseFormatType implements ResponseFormatType {
    JSON(MediaType.APPLICATION_JSON),
    CSV(ResponseFormatType.CSV_CONTENT_TYPE),
    DEBUG(MediaType.APPLICATION_JSON, ".json"),
    JSONAPI(MediaType.APPLICATION_JSON, ".json");

    private String fileExtension;
    private String contentType;

    /**
     * Constructor.
     *
     * @param contentType  content type value used to build a Content-Type header. e.g. text/csv or application/json
     */
    DefaultResponseFormatType(String contentType) {
        this.fileExtension = "." + name().toLowerCase(Locale.ENGLISH);
        this.contentType = contentType;
    }

    /**
     * Constructor.
     *
     * @param contentType  content type value used to build a Content-Type header. e.g. text/csv or application/json
     * @param fileExtension  file extension that a response in this format would be downloaded as. e.g. .csv or .json
     */
    DefaultResponseFormatType(String contentType, String fileExtension) {
        this.contentType = contentType;
        this.fileExtension = fileExtension;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public boolean accepts(String responseFormatValue) {
        return Objects.equals(toString(), responseFormatValue);
    }

    @Override
    public boolean accepts(ResponseFormatType formatType) {
        return formatType.accepts(this.toString());
    }

    @Override
    public String getFileExtension() {
        return fileExtension;
    }

    @Override
    public String getContentType() {
        return contentType;
    }
}
