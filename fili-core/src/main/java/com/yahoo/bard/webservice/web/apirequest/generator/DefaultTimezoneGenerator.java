// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_TIME_ZONE;

import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestImpl;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default generator implementation for {@link DateTimeZone}.
 */
public class DefaultTimezoneGenerator implements Generator<DateTimeZone> {

    private static final Logger LOG = LoggerFactory.getLogger(DataApiRequestImpl.class);

    @Override
    public DateTimeZone bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        return generateTimeZone(params.getTimeZone().orElse(null), resources.getSystemTimeZone());
    }

    @Override
    public void validate(
            DateTimeZone entity,
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        // no default validation
    }

    /**
     * Get the timezone for the request.
     *
     * This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
     *
     * @param timeZoneId  String of the TimeZone ID
     * @param systemTimeZone  TimeZone of the system to use if there is no timeZoneId
     *
     * @return the request's TimeZone
     */
    public static DateTimeZone generateTimeZone(String timeZoneId, DateTimeZone systemTimeZone) {
        try (TimedPhase timer = RequestLog.startTiming("generatingTimeZone")) {
            if (timeZoneId == null) {
                return systemTimeZone;
            }
            try {
                return DateTimeZone.forID(timeZoneId);
            } catch (IllegalArgumentException ignored) {
                LOG.debug(INVALID_TIME_ZONE.logFormat(timeZoneId));
                throw new BadApiRequestException(INVALID_TIME_ZONE.format(timeZoneId));
            }
        }
    }
}
