// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.async.jobs.stores.ApiJobStore;
import com.yahoo.bard.webservice.async.broadcastchannels.BroadcastChannel;
import com.yahoo.bard.webservice.async.jobs.payloads.JobPayloadBuilder;
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow;
import com.yahoo.bard.webservice.async.preresponses.stores.PreResponseStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.observables.ConnectableObservable;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriInfo;

/**
 * Jobs API Request. Such an API Request binds, validates, and models the parts of a request to the Jobs endpoint.
 */
public class JobsApiRequest extends ApiRequest {

    public static final String REQUEST_MAPPER_NAMESPACE = "jobsApiRequestMapper";
    private static final Logger LOG = LoggerFactory.getLogger(JobsApiRequest.class);

    private final JobPayloadBuilder jobPayloadBuilder;
    private final ApiJobStore apiJobStore;
    private final PreResponseStore preResponseStore;
    private final BroadcastChannel<String> broadcastChannel;

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param asyncAfter  How long the user is willing to wait for a synchronous request in milliseconds
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param uriInfo  The URI of the request object.
     * @param jobPayloadBuilder  The JobRowMapper to be used to map JobRow to the Job returned by the api
     * @param apiJobStore  The ApiJobStore containing Job metadata
     * @param preResponseStore  The data store responsible for storing PreResponses
     * @param broadcastChannel  Channel to notify other Bard processes (i.e. long pollers)
     * that a PreResponse is ready for retrieval
     */
    public JobsApiRequest(
            String format,
            String asyncAfter,
            @NotNull String perPage,
            @NotNull String page,
            UriInfo uriInfo,
            JobPayloadBuilder jobPayloadBuilder,
            ApiJobStore apiJobStore,
            PreResponseStore preResponseStore,
            BroadcastChannel<String> broadcastChannel
    ) {
        super(format, asyncAfter, perPage, page, uriInfo);
        this.jobPayloadBuilder = jobPayloadBuilder;
        this.apiJobStore = apiJobStore;
        this.preResponseStore = preResponseStore;
        this.broadcastChannel = broadcastChannel;
    }

    /**
     * Returns an Observable over the Map representing the job to be returned to the user.
     *
     * @param ticket  The ticket that uniquely identifies the job
     *
     * @return An Observable over the Map representing the job to be returned to the user or an Observable wrapping
     * JobNotFoundException if the Job is not available in the ApiJobStore
     */
    public Observable<Map<String, String>> getJobViewObservable(String ticket) {
        return apiJobStore.get(ticket)
                .switchIfEmpty(
                        Observable.error(new JobNotFoundException(ErrorMessageFormat.JOB_NOT_FOUND.format(ticket)))
                )
                .map(jobRow -> jobPayloadBuilder.buildPayload(jobRow, uriInfo));
    }

    /**
     * Get a PreResponse associated with a given ticket if a timeout does not occur in 'asyncAfter' milliseconds.
     * If the notification that a job has been stored in the PreResponseStore is received from the
     * BroadcastChannel before the timeout, fetch the PreResponse for the specified ticket from the PreResponseStore.
     * In case of a timeout, return an empty Observable.
     * <p>
     * BroadCastChannel is a hot observable i.e. it emits notification irrespective of whether it has any subscribers.
     * We use the replay operator so that the preResponseObservable upon connection, will begin collecting values.
     * Once a new observer subscribes to the observable, it will have all the collected values replayed to it.
     *
     * @param ticket  The ticket for which the PreResponse needs to be retrieved.
     *
     * @return An Observable wrapping a PreResponse or an empty Observable in case a timeout occurs.
     */
    public Observable<PreResponse> handleBroadcastChannelNotification(@NotNull String ticket) {
        ConnectableObservable<PreResponse> broadCastChannelPreResponseObservable = broadcastChannel.getNotifications()
                .filter(ticket::equals)
                .timeout(asyncAfter, TimeUnit.MILLISECONDS, Observable.empty())
                .flatMap(preResponseStore::get)
                .take(1)
                .replay(1);

        //connect to the broadCastChannelPreResponseObservable so that it begins collecting values
        broadCastChannelPreResponseObservable.connect();

        return broadCastChannelPreResponseObservable;
    }

    /**
     * Returns an Observable containing a stream of job payloads for all the jobs in the ApiJobStore. If, for any
     * JobRow, the mapping from JobRow to job view fails, an Observable over JobRequestFailedException is returned. If
     * the ApiJobStore is empty, we return an empty Observable.
     *
     * @return An Observable containing a stream of Maps representing the job to be returned to the user
     */
    public Observable<Map<String, String>> getJobViews() {
        return apiJobStore.getAllRows()
                .map(this::mapJobRowsToJobViews);
    }

    /**
     * Given a JobRow, map it to the Job payload to be returned to the user. If the JobRow cannot be successfully
     * mapped to a Job View, JobRequestFailedException is thrown.
     *
     * @param jobRow  The JobRow to be mapped to job payload
     *
     * @return Job payload to be returned to the user
     */
    private Map<String, String> mapJobRowsToJobViews(JobRow jobRow) {
        try {
            return jobPayloadBuilder.buildPayload(jobRow, uriInfo);
        } catch (JobRequestFailedException e) {
            String msg = ErrorMessageFormat.JOBS_RETREIVAL_FAILED.format(jobRow.getId());
            LOG.error(msg);
            throw new JobRequestFailedException(msg);
        }
    }
}
