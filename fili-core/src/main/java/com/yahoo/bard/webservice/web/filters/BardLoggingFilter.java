// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;

import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.logging.blocks.Epilogue;
import com.yahoo.bard.webservice.logging.blocks.Preface;
import com.yahoo.bard.webservice.util.CacheLastObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.subjects.ReplaySubject;
import rx.subjects.Subject;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Priority;
import javax.inject.Singleton;
import javax.validation.constraints.NotNull;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.client.ClientResponseContext;
import javax.ws.rs.client.ClientResponseFilter;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;

/**
 * Logs request/response in this format.
 *
 * <pre><code>
    Request: GET, length=-1
    {@literal http://localhost:9998/test/data?metrics=pageViews&dateTime=2014-06-11%2F2014-06-12}
    {@literal > user-agent=Jersey/2.9 (HttpUrlConnection 1.7.0_55)}
    {@literal > host=localhost:9998}
    {@literal > accept=text/html, image/gif, image/jpeg, *; q=.2,}
    {@literal > connection=keep-alive}
    {@literal < Content-Type=application/json}
    Response: 200, OK, 1020.253 ms
    length=2
* </code></pre>
*/
@PreMatching
@Singleton
@Priority(1)
public class BardLoggingFilter implements ContainerRequestFilter, ContainerResponseFilter, ClientRequestFilter,
    ClientResponseFilter, WriterInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(BardLoggingFilter.class);

    private static final String PROPERTY_NANOS = BardLoggingFilter.class.getName() + ".nanos";
    private static final String PROPERTY_REQ_LEN = BardLoggingFilter.class.getName() + ".reqlen";
    private static final String PROPERTY_OUTPUT_STREAM = BardLoggingFilter.class.getName() + ".ostream";
    private static final Pattern VALID_REQUEST_ID = Pattern.compile("[a-zA-Z0-9+/=\\-_]+");

    public static final double MILLISECONDS_PER_NANOSECOND = 1000000.0;
    public static final String TOTAL_TIMER = "TotalTime";
    public static final String CLIENT_TOTAL_TIMER = "ClientRequestTotalTime";
    public static final String X_REQUEST_ID_HEADER = "x-request-id";

    /**
     * Intercept the Container request to add length of request and a start timestamp.
     *
     * @param request  Request to intercept
     *
     * @throws IOException if there's a problem processing the request
     */
    @Override
    public void filter(ContainerRequestContext request) throws IOException {

        appendRequestId(request.getHeaders().getFirst(X_REQUEST_ID_HEADER));
        RequestLog.startTiming(TOTAL_TIMER);
        try (TimedPhase timer = RequestLog.startTiming(this)) {
            RequestLog.record(new Preface(request));

            // sets PROPERTY_REQ_LEN if content-length not defined
            lengthOfRequestEntity(request);

            // store start time to later calculate elapsed time
            request.setProperty(PROPERTY_NANOS, System.nanoTime());
        }
    }

    /**
     * Intercept the Container request/response and log.
     * <p>
     * If output is streamed, also hook the output stream.
     *
     * @param request  Request to intercept
     * @param response  Response to intercept
     *
     * @throws IOException if there's a problem processing the request or response
     */
    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response)
        throws IOException {
        appendRequestId(request.getHeaders().getFirst(X_REQUEST_ID_HEADER));
        RequestLog.startTiming(this);
        StringBuilder debugMsgBuilder = new StringBuilder();

        debugMsgBuilder.append("\tRequest: ").append(request.getMethod());
        debugMsgBuilder.append("\tlength=").append(request.getProperty(PROPERTY_REQ_LEN)).append("\t");
        debugMsgBuilder.append(renderUri(request.getUriInfo().getRequestUri())).append("\t");

        debugMsgBuilder.append("Response: ").append(response.getStatus()).append("\t");
        debugMsgBuilder.append(response.getStatusInfo()).append("\t");

        Long requestStartTime = (Long) request.getProperty(PROPERTY_NANOS);
        if (requestStartTime != null) {
            debugMsgBuilder.append((System.nanoTime() - requestStartTime) / MILLISECONDS_PER_NANOSECOND);
        }
        debugMsgBuilder.append(" ms\t");

        if (request.getSecurityContext().getUserPrincipal() != null) {
            String user = request.getSecurityContext().getUserPrincipal().getName();
            debugMsgBuilder.append("User=").append(user).append("\t");
        }

        appendStringHeaders(debugMsgBuilder, "> ", request.getHeaders().entrySet());
        appendObjectHeaders(debugMsgBuilder, "< ", response.getHeaders().entrySet());

        Response.StatusType status = response.getStatusInfo();
        String msg = "Successful request";
        if (status.getFamily() != SUCCESSFUL) {
            msg = response.hasEntity() ? response.getEntity().toString() : "Request without entity failed";
        }
        CacheLastObserver<Long> responseLengthObserver = new CacheLastObserver<>();
        RequestLog.record(new Epilogue(msg, status, responseLengthObserver));

        // if response is not yet finalized, we must intercept the output stream
        if (response.getLength() == -1 && response.hasEntity()) {
            //Pass the length of the response stream to the epilogue as soon as we know what that length is.
            Subject<Long, Long> lengthBroadcaster = ReplaySubject.create();
            lengthBroadcaster.subscribe(responseLengthObserver);
            OutputStream stream = new LengthOfOutputStream(response.getEntityStream(), lengthBroadcaster);

            response.setEntityStream(stream);
            request.setProperty(PROPERTY_OUTPUT_STREAM, stream);
        } else {
            debugMsgBuilder.append("length=").append(response.getLength()).append("\t");
            Observable.just((long) response.getLength()).subscribe(responseLengthObserver);
            LOG.debug(debugMsgBuilder.toString());
            RequestLog.stopTiming(this);
            RequestLog.stopTiming(TOTAL_TIMER);
            RequestLog.log();
        }
    }

    /**
     * Intercept Client request and add start timestamp.
     *
     * @param request  Request to intercept
     *
     * @throws IOException if there's a problem processing the request
     */
    @Override
    public void filter(ClientRequestContext request) throws IOException {
        appendRequestId(request.getStringHeaders().getFirst(X_REQUEST_ID_HEADER));
        RequestLog.startTiming(CLIENT_TOTAL_TIMER);
        request.setProperty(PROPERTY_NANOS, System.nanoTime());
    }

    /* Intercept Client request/response and log
     * @see ClientResponseFilter#filter(ClientRequestContext, ClientResponseContext)
     */
    @Override
    public void filter(ClientRequestContext request, ClientResponseContext response) throws IOException {
        StringBuilder debugMsgBuilder = new StringBuilder();

        debugMsgBuilder.append("ClientRequest: ").append(request.getMethod()).append("\t");
        debugMsgBuilder.append(request.getUri().toASCIIString()).append("\t");

        appendObjectHeaders(debugMsgBuilder, "> ", request.getHeaders().entrySet());
        appendStringHeaders(debugMsgBuilder, "< ", response.getHeaders().entrySet());

        debugMsgBuilder.append(response.getStatusInfo()).append(", length=").append(response.getLength()).append(" ");

        Long requestStartTime = (Long) request.getProperty(PROPERTY_NANOS);
        if (requestStartTime != null) {
            debugMsgBuilder
                    .append((System.nanoTime() - requestStartTime) / MILLISECONDS_PER_NANOSECOND)
                    .append(" ms\t");
        }

        LOG.debug(debugMsgBuilder.toString());
        RequestLog.stopTiming(CLIENT_TOTAL_TIMER);
    }

    /**
     * Add a request id to pass to druid. Only added if x-request-id follows the format specified at:
     * https://devcenter.heroku.com/articles/http-request-id
     *
     * @param requestId  Request id to add as queryId prefix to druid
     */
    private void appendRequestId(String requestId) {
        if (isNotValidRequestId(requestId)) {
            return; // Ignore according to https://devcenter.heroku.com/articles/http-request-id
        }
        RequestLog.addIdPrefix(requestId);
    }

    /**
     * Validate whether or not a string is acceptable as an x-request-id header argument.
     *
     * @param requestId  Request id to validate
     * @return True if the request id is not valid, false otherwise
     */
    private boolean isNotValidRequestId(String requestId) {
        return requestId == null || requestId.isEmpty() || requestId.length() > 200
               || !VALID_REQUEST_ID.matcher(requestId).matches();
    }

    /**
     * Render the URI as a string.
     *
     * @param uri The URI to render
     *
     * @return A String representation of the URI
     */
    protected String renderUri(URI uri) {
        return uri.toASCIIString();
    }

    /**
     * Add the entries (assumed to be headers) to the given StringBuilder, prefixing their keys.
     *
     * @param builder  StringBuilder to append to
     * @param prefix  Prefix to put in front of the keys
     * @param entries  Entries to add
     */
    private void appendObjectHeaders(StringBuilder builder, String prefix, Set<Entry<String, List<Object>>> entries) {
        for (Entry<String, List<Object>> e : entries) {
            appendHeader(builder, prefix, e.getKey(), e.getValue());
        }
    }

    /**
     * Add the entries (assumed to be headers) to the given StringBuilder, prefixing their keys.
     *
     * @param builder  StringBuilder to append to
     * @param prefix  Prefix to put in front of the keys
     * @param entries  Entries to add
     */
    private void appendStringHeaders(StringBuilder builder, String prefix, Set<Entry<String, List<String>>> entries) {
        for (Entry<String, List<String>> e : entries) {
            appendHeader(builder, prefix, e.getKey(), e.getValue());
        }
    }

    /**
     * Append the given values to the StringBuilder in a key-value way, prefixing the key and tab-separating the values.
     *
     * @param builder  StringBuilder to build upon
     * @param prefix  Prefix to put in front of the key
     * @param name  Name of the key
     * @param values  Values to tab-separate
     */
    private void appendHeader(StringBuilder builder, String prefix, String name, List<?> values) {
        String value = values.size() == 1 ? values.get(0).toString() : values.toString();

        builder.append(prefix).append(name).append("=").append(value).append("\t");
    }

    /**
     * This allows us to compute the length of the output stream when stream is closed.
     *
     * @param writerInterceptorContext  Interceptor to use for intercepting the entity being written
     *
     * @throws IOException if there's a problem processing the request
     */
    @Override
    public void aroundWriteTo(WriterInterceptorContext writerInterceptorContext) throws IOException {
        LengthOfOutputStream stream = (LengthOfOutputStream) writerInterceptorContext
                .getProperty(PROPERTY_OUTPUT_STREAM);
        try {
            writerInterceptorContext.proceed();
            if (stream != null) {
                emitSuccess(stream);
            }
        } catch (EOFException e) {
            if (stream != null) {
                emitError(stream, e);
            }
            LOG.warn("Connection to client closed prematurely.", e);
        } catch (Error | RuntimeException | IOException e) {
            if (stream != null) {
                emitError(stream, e);
            }
            // catch all and rethrow after logging
            LOG.error("Error encountered while streaming response back to the client.", e);
            throw e;
        } finally {
            try {
                RequestLog.stopTiming(this);
                RequestLog.stopTiming(TOTAL_TIMER);
                RequestLog.log();
            } catch (Exception e) {
                LOG.error("Error finalizing the BardLoggingFilter output stream wrapper.", e);
            }
        }
    }

    /**
     * Handle publishing the length and completed to the Subject.
     *
     * @param stream  Stream to get the length from
     */
    private void emitSuccess(@NotNull LengthOfOutputStream stream) {
        Subject<Long, Long> lengthBroadcaster = stream.getLengthBroadcaster();
        lengthBroadcaster.onNext(stream.getResponseLength());
        lengthBroadcaster.onCompleted();
    }

    /**
     * Handle publishing the length and an error to the Subject.
     *
     * @param stream  Stream to get the length from
     * @param t  Error that was encountered (to be published)
     */
    private void emitError(LengthOfOutputStream stream, Throwable t) {
        Subject<Long, Long> lengthBroadcaster = stream.getLengthBroadcaster();
        lengthBroadcaster.onNext(stream.getResponseLength());
        lengthBroadcaster.onError(t);
    }

    // maximum input buffer we can hold before lengthOfInputStream returns -1
    static final int MAX_ENTITY_SIZE = 65535;

    /**
     * If content-length not defined, finds length of request stream and sets PROPERTY_REQ_LEN.
     *
     * @param request  the request
     * @throws IOException if reading the entity stream from the request throws an IOException
     */
    private void lengthOfRequestEntity(ContainerRequestContext request) throws IOException {
        // Scan entity stream if unknown length
        if (request.getLength() == -1 && request.hasEntity()) {
            InputStream istream = request.getEntityStream();
            if (!istream.markSupported()) {
                istream = new BufferedInputStream(istream);
            }
            istream.mark(MAX_ENTITY_SIZE + 1);
            byte[] entity = new byte[MAX_ENTITY_SIZE + 1];
            int entitySize = istream.read(entity);
            if (entitySize > MAX_ENTITY_SIZE) {
                request.setProperty(PROPERTY_REQ_LEN, -1);
            } else {
                request.setProperty(PROPERTY_REQ_LEN, entitySize);
            }
            istream.reset();
            request.setEntityStream(istream);
        } else {
            request.setProperty(PROPERTY_REQ_LEN, request.getLength());
        }
    }

    /**
     * Contains a Subject that fires a single message with the number of bytes the output stream has written at the
     * point where {@code fireLengthMessage} is called.
     */
    static private class LengthOfOutputStream extends OutputStream {
        private long length;
        private final OutputStream outputStream;
        private final Subject<Long, Long> lengthBroadcaster;

        /**
         * Constructor.
         *
         * @param outputStream  OutputStream to get the length of
         * @param lengthBroadcaster  Subject to publish events to (final length and completion status or error)
         */
        LengthOfOutputStream(OutputStream outputStream, Subject<Long, Long> lengthBroadcaster) {
            this.outputStream = outputStream;
            this.lengthBroadcaster = lengthBroadcaster;
            this.length = 0;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            outputStream.write(b, off, len);
            length += len;
        }

        @Override
        public void write(int i) throws IOException {
            outputStream.write(i);
            length++;
        }

        public Subject<Long, Long> getLengthBroadcaster() {
            return lengthBroadcaster;
        }

        public long getResponseLength() {
            return length;
        }

    }
}
