FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace

COPY pom.xml ./
COPY common common
COPY ingestion-service ingestion-service
COPY activity-service activity-service

RUN mvn -q -DskipTests -pl activity-service -am package

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /workspace/activity-service/target/*.jar app.jar
EXPOSE 8084
ENTRYPOINT ["java", "-jar", "app.jar"]
