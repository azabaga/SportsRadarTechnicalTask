package com.example.offer.http;

import com.example.offer.application.Offer;
import com.example.offer.application.OfferService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

@RestController
public final class OfferController {
    private static final Pattern DATE_FORMAT = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");

    private final OfferService service;

    public OfferController(OfferService service) {
        this.service = service;
    }

    @GetMapping(path = "/offer", produces = MediaType.APPLICATION_JSON_VALUE)
    public Offer getOffer(@RequestParam MultiValueMap<String, String> filters) {
        LocalDate from = parseDate(filters, "from");
        LocalDate to = parseDate(filters, "to");
        if (from != null && to != null && from.isAfter(to)) {
            throw badRequest("'from' must be on or before 'to'");
        }
        return service.getOffer(from, to);
    }

    private static LocalDate parseDate(MultiValueMap<String, String> filters, String name) {
        var values = filters.get(name);
        if (values == null) {
            return null;
        }
        if (values.size() != 1) {
            throw badRequest("'" + name + "' may be supplied only once");
        }

        String value = values.getFirst();
        String message = "'" + name + "' must be a valid UTC calendar date in YYYY-MM-DD format";
        if (!DATE_FORMAT.matcher(value).matches()) {
            throw badRequest(message);
        }
        try {
            return LocalDate.parse(value);
        } catch (DateTimeParseException invalidDate) {
            throw badRequest(message);
        }
    }

    private static ResponseStatusException badRequest(String detail) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, detail);
    }
}
