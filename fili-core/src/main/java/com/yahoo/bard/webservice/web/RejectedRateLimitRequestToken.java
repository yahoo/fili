package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.web.RateLimitRequestToken;

import java.io.IOException;

public class RejectedRateLimitRequestToken implements RateLimitRequestToken {
    @Override
    public boolean isBound() {
        return false;
    }

    @Override
    public boolean bind() {
        return false;
    }

    @Override
    public void unBind() {
        // do nothing
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
