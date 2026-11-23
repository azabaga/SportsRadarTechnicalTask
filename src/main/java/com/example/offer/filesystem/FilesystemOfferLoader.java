package com.example.offer.filesystem;

import com.example.offer.json.OfferJson;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

@Component
public final class FilesystemOfferLoader {
    private final OfferJson json;

    public FilesystemOfferLoader(OfferJson json) {
        this.json = json;
    }

    public <T> List<T> read(Path path, Class<T> type, Function<T, String> identify) {
        Path source = path.toAbsolutePath().normalize();
        try (var input = Files.newInputStream(source)) {
            List<T> entries = json.readList(input, type);
            Set<String> ids = new HashSet<>();
            for (T entry : entries) {
                String id = identify.apply(entry);
                if (!ids.add(id)) {
                    throw new IllegalArgumentException("Duplicate " + type.getSimpleName() + " ID: " + id);
                }
            }
            return entries;
        } catch (IOException | JacksonException | IllegalArgumentException error) {
            throw new IllegalStateException("Cannot load initial " + type.getSimpleName()
                    + " data from " + source + ": " + error.getMessage(), error);
        }
    }
}
