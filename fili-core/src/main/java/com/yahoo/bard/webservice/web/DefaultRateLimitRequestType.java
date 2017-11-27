// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

/**
 * Type of outstanding request.
 */
public enum DefaultRateLimitRequestType implements RateLimitRequestType {
     USER {
         @Override
         public String getType() {
             return "USER";
         }
     },
    UI {
        @Override
        public String getType() {
            return "UI";
        }
    },
    BYPASS {
        @Override
        public String getType() {
            return "BYPASS";
        }
    }
}
