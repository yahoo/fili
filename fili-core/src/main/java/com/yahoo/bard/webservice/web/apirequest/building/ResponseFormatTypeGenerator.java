package com.yahoo.bard.webservice.web.apirequest.building;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.ACCEPT_FORMAT_INVALID;

import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.DefaultResponseFormatType;
import com.yahoo.bard.webservice.web.ResponseFormatType;

import java.util.Locale;

public interface ResponseFormatTypeGenerator {
    /**
     * Generates the format in which the response data is expected.
     *
     * @param format  Expects a URL format query String.
     *
     * @return Response format type (CSV or JSON).
     * @throws BadApiRequestException if the requested format is not found.
     */
    ResponseFormatType generateAcceptFormat(String format) throws BadApiRequestException;

    ResponseFormatTypeGenerator DEFAULT_RESPONSE_FORMAT_TYPE_GENERATOR = format -> {
        try {
            return format == null ?
                    DefaultResponseFormatType.JSON :
                    DefaultResponseFormatType.valueOf(format.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            throw new BadApiRequestException(ACCEPT_FORMAT_INVALID.format(format));
        }
    };
}
