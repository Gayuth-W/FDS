# syntax=docker/dockerfile:1

# ---------- build stage ----------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q -DskipTests package

# ---------- runtime stage ----------
FROM eclipse-temurin:21-jre
WORKDIR /app

# curl for the healthcheck; libfaketime for the (opt-in) clock-skew chaos scenario.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl libfaketime \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/dfs-node.jar app.jar
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# NODE_ID / PORT / CLUSTER_NODES are provided at runtime (see docker-compose.yml).
# Per-node data (blocks, checkpoints, manifests) is written under /app/data/<nodeId>.
EXPOSE 8001-8005

ENTRYPOINT ["/app/docker-entrypoint.sh"]
