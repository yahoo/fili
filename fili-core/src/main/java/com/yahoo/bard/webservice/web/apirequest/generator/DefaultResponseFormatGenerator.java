package com.yahoo.bard.webservice.web.apirequest.generator;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.ACCEPT_FORMAT_INVALID;

import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.DefaultResponseFormatType;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class DefaultResponseFormatGenerator implements Generator<ResponseFormatType> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultResponseFormatType.class);

    @Override
    public ResponseFormatType bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        return DefaultResponseFormatGenerator.generateResponseFormat(params.getFormat().orElse(null));
    }

    @Override
    public void validate(
            ResponseFormatType entity,
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        // no default validation
    }

    /**
     * Resolves the {@code format} request parameter into one of the response formats specified in
     * {@link DefaultResponseFormatType}. Defaults to {@link DefaultResponseFormatType#JSON} if {@code format} is null.
     *
     * @param format  The string representation of the desired {@link ResponseFormatType} to be parsed. Null values are
     *                accepted and {@link DefaultResponseFormatType#JSON} will be returned if a null value is provided.
     *
     * @throws BadApiRequestException if the response format cannot be resolved to any of the supported response
     *                                formats.
     * @return the response format.
     */
    public static ResponseFormatType generateResponseFormat(String format) {
        try {
            return format == null ?
                    DefaultResponseFormatType.JSON :
                    DefaultResponseFormatType.valueOf(format.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            LOG.error(ACCEPT_FORMAT_INVALID.logFormat(format), e);
            throw new BadApiRequestException(ACCEPT_FORMAT_INVALID.format(format));
        }
    }
}
