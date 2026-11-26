package io.github.azabaga.offer.http;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
final class OpenApiController {
    @GetMapping(path = "/openapi.yaml", produces = MediaType.TEXT_PLAIN_VALUE)
    Resource openApi() {
        return new ClassPathResource("static/openapi.yaml");
    }
}
