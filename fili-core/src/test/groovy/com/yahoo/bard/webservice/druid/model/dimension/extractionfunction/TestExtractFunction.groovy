// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction

import com.fasterxml.jackson.annotation.JsonValue

class TestExtractFunction extends ExtractionFunction {

    public static final TestExtractionFunctionType TYPE = new TestExtractionFunctionType();

    public TestExtractFunction() {
        super(TYPE)
    }

    public static class TestExtractionFunctionType implements ExtractionFunctionType {

        @JsonValue
        String toJson() {
            return "test"
        }
    }
}
