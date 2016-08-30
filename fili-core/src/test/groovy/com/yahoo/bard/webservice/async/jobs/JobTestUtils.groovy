// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs

import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.JOB_TICKET
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.QUERY
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.STATUS
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.DATE_CREATED
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.DATE_UPDATED
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.USER_ID

import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobStatus
import com.yahoo.bard.webservice.async.jobs.jobrows.JobField
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

/**
 * A collection of utility methods and objects to make it easier to test code that relies on ApiJobStores,
 * JobRows, and JobFields.
 */
class JobTestUtils {
    static final String JOB_TICKET_DATA = "123"
    static final String QUERY_DATA = "https://host:port/v1/data/table/grain?metrics=metric"
    static final String STATUS_DATA = DefaultJobStatus.PENDING.getName()
    static final String DATE_CREATED_DATA = new DateTime(2016, 4, 29, 0, 0, DateTimeZone.UTC).toString()
    static final String DATE_UPDATED_DATA = new DateTime(2016, 4, 30, 0, 0, DateTimeZone.UTC).toString()
    static final String USER_ID_DATA = "A man with a plan"

    /**
     * Builds a job row with the specified metadata, and provides a default value for any fields in
     * {@link com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField} that are not keys in the metadata.
     *
     * @param jobData  A mapping from JobField to Strings describing overrides to the default metadata for the job
     *
     * @return A JobRow populated with the specified metadata, and defaults for all missing DefaultJobFields
     */
    static JobRow buildJobRow(Map<JobField, String> jobData = [:]) {
        new JobRow(
                JOB_TICKET,
                [
                        (JOB_TICKET): jobData.get(JOB_TICKET) ?: JOB_TICKET_DATA,
                        (QUERY): jobData.get(QUERY) ?: QUERY_DATA,
                        (STATUS): jobData.get(STATUS) ?: STATUS_DATA,
                        (DATE_CREATED): jobData.get(DATE_CREATED) ?: DATE_CREATED_DATA,
                        (DATE_UPDATED): jobData.get(DATE_UPDATED) ?: DATE_UPDATED_DATA,
                        (USER_ID): jobData.get(USER_ID) ?: USER_ID_DATA
                ]
        )
    }

    /**
     * A convenience method that builds a JobRow, using the passed in integer to differentiate the fields in the
     * metadata.
     *
     * @param id  The integer to use to differentiate the fields
     *
     * @return A JobRow with the specified integer as an id, and fields that contain the id where reasonable
     */
    static JobRow buildJobRow(int id) {
        //Invocations of toString are necessary to convert GString to String for use in the JobRow constructor.
        buildJobRow([
                (JOB_TICKET): "$id".toString(),
                (QUERY): "https://host:port/v1/data/table/grain?metrics=$id".toString(),
                (DATE_CREATED): new DateTime(id, 4, 29, 0, 0, DateTimeZone.UTC).toString(),
                (DATE_UPDATED): new DateTime(id, 4, 30, 0, 0, DateTimeZone.UTC).toString(),
                (USER_ID): "Number $id".toString()
        ])
    }
}
