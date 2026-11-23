FROM maven:3.9.12-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY src src
COPY fixtures fixtures
RUN --mount=type=cache,target=/root/.m2 mvn -B clean package

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /workspace/target/offer-service-*.jar offer.jar
COPY fixtures fixtures
USER 10001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "offer.jar"]
