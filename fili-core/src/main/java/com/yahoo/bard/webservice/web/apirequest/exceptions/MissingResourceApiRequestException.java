// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.exceptions;

public class MissingResourceApiRequestException extends BadApiRequestException {

    public MissingResourceApiRequestException(String message) {
        super(message);
    }
}
