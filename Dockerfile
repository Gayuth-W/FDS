# syntax=docker/dockerfile:1

# ---------- build stage ----------
# Builds the Spring Boot fat jar with Maven + JDK 21.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# Cache dependencies first: copy only the POM, resolve, then copy sources.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q -DskipTests package

# ---------- runtime stage ----------
# Slim JRE image that just runs the jar. curl is included for the healthcheck.
FROM eclipse-temurin:21-jre
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/dfs-node.jar app.jar

# NODE_ID / PORT / CLUSTER_NODES are provided at runtime (see docker-compose.yml).
# Per-node data (blocks, checkpoints, manifests) is written under /app/data/<nodeId>.
EXPOSE 8001-8005

ENTRYPOINT ["java", "-jar", "app.jar"]
