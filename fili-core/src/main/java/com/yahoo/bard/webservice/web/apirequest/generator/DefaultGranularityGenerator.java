package com.yahoo.bard.webservice.web.apirequest.generator;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.UNKNOWN_GRANULARITY;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.util.GranularityParseException;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

/**
 * Dependent on Timezone
 */
public class DefaultGranularityGenerator implements Generator<Granularity> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultGranularityGenerator.class);

    /**
     * @param builder  The builder object representing the in progress {@link DataApiRequest}. Previously constructed
     *        resources are available through this object.
     * @param params  The request parameters sent by the client.
     * @param resources  Resources used to build the request, such as the
     *        {@link com.yahoo.bard.webservice.data.config.ResourceDictionaries}.
     * @throws BadApiRequestException if granularity is not present. Granularity is required in all data queries. Also
     *         is thrown if an invalid granularity is provided.
     * @throws UnsatisfiedApiRequestConstraintsException if timeZone was not built before this generator runs. Timezone
     *         CAN be empty, which implies using the default timezone. A NULL timezone indicates timezone has not been
     *         generated yet.
     * @return the generated granularity.
     */
    @Override
    public Granularity bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        String granularity = params.getGranularity()
                .orElseThrow(() -> new BadApiRequestException(UNKNOWN_GRANULARITY.logFormat("null")));

        if (builder.getTimeZone() == null) {
            throw new UnsatisfiedApiRequestConstraintsException("Attempting to generate granularity, but timezone " +
                    "has not yet been bound.");
        }

        if (builder.getTimeZone().isPresent()) {
            return generateGranularity(
                    granularity,
                    builder.getTimeZone().get(),
                    resources.getGranularityParser()
            );
        }

        return generateGranularity(granularity, resources.getGranularityParser());
    }

    @Override
    public void validate(
            Granularity entity,
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        // no default validation
    }

    /**
     * Generate a Granularity instance based on a path element.
     *
     * @param granularity  A string representation of the granularity
     * @param dateTimeZone  The time zone to use for this granularity
     * @param granularityParser  The parser for granularity
     *
     * @return A granularity instance with time zone information
     * @throws BadApiRequestException if the string matches no meaningful granularity
     */
    public static Granularity generateGranularity(
            @NotNull String granularity,
            @NotNull DateTimeZone dateTimeZone,
            @NotNull GranularityParser granularityParser
    ) throws BadApiRequestException {
        try {
            return granularityParser.parseGranularity(granularity, dateTimeZone);
        } catch (GranularityParseException e) {
            LOG.error(UNKNOWN_GRANULARITY.logFormat(granularity), granularity);
            throw new BadApiRequestException(e.getMessage());
        }
    }

    /**
     * Generate a Granularity instance based on a path element.
     *
     * @param granularity  A string representation of the granularity
     * @param granularityParser  The parser for granularity
     *
     * @return A granularity instance without time zone information
     * @throws BadApiRequestException if the string matches no meaningful granularity
     */
    public static Granularity generateGranularity(String granularity, GranularityParser granularityParser)
            throws BadApiRequestException {
        try {
            return granularityParser.parseGranularity(granularity);
        } catch (GranularityParseException e) {
            LOG.error(UNKNOWN_GRANULARITY.logFormat(granularity), granularity);
            throw new BadApiRequestException(e.getMessage(), e);
        }
    }
}
