# syntax=docker/dockerfile:1
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon dependencies -q > /dev/null
COPY src src
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon bootJar -x test -q

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /workspace/build/libs/app.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
