// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction;

import com.yahoo.bard.webservice.data.time.Granularity;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Objects;

/**
 * An extraction function that returns the dimension value formatted according to given format string, time zone, and
 * locale.
 * <p>
 * Visit http://druid.io/docs/0.10.1/querying/dimensionspecs.html#time-format-extraction-function for more details on
 * Time Format Extraction Function.
 */
@JsonInclude(Include.NON_NULL)
public class TimeFormatExtractionFunction extends ExtractionFunction {
    private static final Logger LOG = LoggerFactory.getLogger(TimeFormatExtractionFunction.class);

    private final String format;
    private final Locale locale;
    private final DateTimeZone timeZone;
    private final Granularity granularity;
    private final Boolean asMillis;

    /**
     * Constructor.
     * <p>
     * Druid only requires "type" parameter in dimension spec. Other fields can all be null. See
     * http://druid.io/docs/0.10.1/querying/dimensionspecs.html#time-format-extraction-function for more details.
     */
    public TimeFormatExtractionFunction() {
        super(DefaultExtractionFunctionType.TIME_FORMAT);
        format = null;
        locale = null;
        timeZone = null;
        granularity = null;
        asMillis = null;
    }

    /**
     * Constructor.
     *
     * @param format  The date time format for the resulting dimension value, in
     * {@link org.joda.time.format.DateTimeFormat Joda Time DateTimeFormat}, or null to use the default ISO8601 format.
     * @param locale  Locale (language and country) to use, given as a
     * <a href="http://www.oracle.com/technetwork/java/javase/java8locales-2095355.html#util-text" target="_blank"> IETF
     * BCP 47 language tag</a>, e.g. en-US, en-GB, fr-FR, fr-CA, etc.
     * @param timeZone  Time zone to use in
     * <a href="http://en.wikipedia.org/wiki/List_of_tz_database_time_zones" target="_blank">IANA tz database format
     * </a>, e.g. Europe/Berlin (this can possibly be different than the aggregation time-zone)
     * @param granularity  {@link Granularity} to apply before formatting.
     * @param asMillis  Boolean value, set to true to treat input strings as millis rather than ISO8601 strings.
     */
    public TimeFormatExtractionFunction(
            String format,
            Locale locale,
            DateTimeZone timeZone,
            Granularity granularity,
            Boolean asMillis
    ) {
        super(DefaultExtractionFunctionType.TIME_FORMAT);

        // validate format
        try {
            DateTimeFormat.forPattern(format);
        } catch (IllegalArgumentException exception) {
            String message = String.format("%s is an invalid format", format);
            LOG.error(message);
            throw new IllegalArgumentException(message, exception);
        }

        this.format = format;
        this.locale = locale;
        this.timeZone = timeZone;
        this.granularity = granularity;
        this.asMillis = asMillis;
    }

    /**
     * Returns format of this {@link TimeFormatExtractionFunction}.
     *
     * @return the format of this {@link TimeFormatExtractionFunction}
     */
    @JsonProperty(value = "format")
    public String getFormat() {
        return format;
    }

    /**
     * Returns {@link java.util.Locale locale} of this {@link TimeFormatExtractionFunction}.
     *
     * @return the {@link java.util.Locale locale} of this {@link TimeFormatExtractionFunction}
     */
    @JsonProperty(value = "locale")
    public Locale getLocale() {
        return locale;
    }

    /**
     * Returns {@link org.joda.time.DateTimeZone time zone} of this {@link TimeFormatExtractionFunction}.
     *
     * @return the {@link org.joda.time.DateTimeZone time zone} of this {@link TimeFormatExtractionFunction}
     */
    @JsonProperty(value = "timeZone")
    public DateTimeZone getTimeZone() {
        return timeZone;
    }

    /**
     * Returns {@link Granularity granularity} of this
     * {@link TimeFormatExtractionFunction}.
     *
     * @return the {@link Granularity granularity} of this
     * {@link TimeFormatExtractionFunction}
     */
    @JsonProperty(value = "granularity")
    public Granularity getGranularity() {
        return granularity;
    }

    /**
     * Returns whether to treat input input strings as millis(true) or ISO8601 strings(false).
     *
     * @return the whether to treat input input strings as millis(true) or ISO8601 strings(false)
     */
    @JsonProperty(value = "asMillis")
    public Boolean isAsMillis() {
        return asMillis;
    }

    /**
     * Returns a new {@link TimeFormatExtractionFunction} with a specified format.
     *
     * @param format  The date time format for the resulting dimension value, in
     * {@link org.joda.time.format.DateTimeFormat Joda Time DateTimeFormat}, or null to use the default ISO8601 format.
     *
     * @return the new {@link TimeFormatExtractionFunction} with the specified format
     */
    public TimeFormatExtractionFunction withFormat(String format) {
        return new TimeFormatExtractionFunction(format, locale, timeZone, granularity, asMillis);
    }

    /**
     * Returns a new {@link TimeFormatExtractionFunction} with a specified {@link java.util.Locale locale}.
     *
     * @param locale  Locale (language and country) to use, given as a
     * <a href="http://www.oracle.com/technetwork/java/javase/java8locales-2095355.html#util-text" target="_blank"> IETF
     * BCP 47 language tag</a>, e.g. en-US, en-GB, fr-FR, fr-CA, etc.
     *
     * @return the new {@link TimeFormatExtractionFunction} with the specified {@link java.util.Locale locale}
     */
    public TimeFormatExtractionFunction withLocale(Locale locale) {
        return new TimeFormatExtractionFunction(format, locale, timeZone, granularity, asMillis);
    }

    /**
     * Returns a new {@link TimeFormatExtractionFunction} with a specified {@link org.joda.time.DateTimeZone time zone}.
     *
     * @param timeZone  Time zone to use in
     * <a href="http://en.wikipedia.org/wiki/List_of_tz_database_time_zones" target="_blank">IANA tz database format
     * </a>, e.g. Europe/Berlin (this can possibly be different than the aggregation time-zone)
     *
     * @return the new {@link TimeFormatExtractionFunction} with the specified
     * {@link org.joda.time.DateTimeZone time zone}
     */
    public TimeFormatExtractionFunction withTimeZone(DateTimeZone timeZone) {
        return new TimeFormatExtractionFunction(format, locale, timeZone, granularity, asMillis);
    }

    /**
     * Returns a new {@link TimeFormatExtractionFunction} with a specified
     * {@link Granularity granularity}.
     *
     * @param granularity  {@link Granularity} to apply before formatting.
     *
     * @return  the new {@link TimeFormatExtractionFunction} with the specified
     * {@link Granularity granularity}
     */
    public TimeFormatExtractionFunction withGranularity(Granularity granularity) {
        return new TimeFormatExtractionFunction(format, locale, timeZone, granularity, asMillis);
    }

    /**
     * Returns a new {@link TimeFormatExtractionFunction} with a specified config on "asMillis".
     *
     * @param asMillis  Boolean value, set to true to treat input strings as millis rather than ISO8601 strings.
     *
     * @return new {@link TimeFormatExtractionFunction} with the specified config on "asMillis"
     */
    public TimeFormatExtractionFunction withAsMillis(Boolean asMillis) {
        return new TimeFormatExtractionFunction(format, locale, timeZone, granularity, asMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), format, locale, timeZone, granularity, asMillis);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || this.getClass() != other.getClass()) {
            return false;
        }
        TimeFormatExtractionFunction that = (TimeFormatExtractionFunction) other;
        return super.equals(other) &&
                Objects.equals(this.format, that.format) &&
                Objects.equals(this.locale, that.locale) &&
                Objects.equals(this.timeZone, that.timeZone) &&
                Objects.equals(this.granularity, that.granularity) &&
                Objects.equals(this.asMillis, that.asMillis);
    }
}
