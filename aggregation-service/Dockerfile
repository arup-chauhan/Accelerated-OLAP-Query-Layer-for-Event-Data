FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace

COPY pom.xml ./
COPY common common
COPY aggregation-service aggregation-service

RUN mvn -q -DskipTests -pl aggregation-service -am package

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /workspace/aggregation-service/target/*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
