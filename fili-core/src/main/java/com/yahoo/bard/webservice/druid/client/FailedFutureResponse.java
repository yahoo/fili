package com.yahoo.bard.webservice.druid.client;

import org.asynchttpclient.Response;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by kevin on 4/21/2017.
 */
public class FailedFutureResponse implements Future<Response> {
    private String reason;

    public FailedFutureResponse(String reason) {
        this.reason = reason;
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public Response get() throws InterruptedException, ExecutionException {
        throw new ExecutionException(new RuntimeException(reason));
    }

    @Override
    public Response get(final long timeout, final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }
}
