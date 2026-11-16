
# ETAPA 1: Compilar el proyecto con Maven y Java 17
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /workspace

COPY pom.xml .
COPY src ./src

RUN mvn -B -DskipTests package

# ETAPA 2: Ejecutar MS Report con Java 17
FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

RUN mkdir -p /app/data && \
    chown -R 10001:10001 /app

COPY --from=build --chown=10001:10001 \
    /workspace/target/campuslab-ms-report-0.0.1-SNAPSHOT.jar \
    /app/app.jar

USER 10001:10001

ENV PORT=8085
ENV DB_URL=jdbc:h2:file:/app/data/reportdb
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=65.0"

EXPOSE 8085

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
