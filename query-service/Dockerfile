FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace

COPY pom.xml ./
COPY common common
COPY query-service query-service

RUN mvn -q -DskipTests -pl query-service -am package

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /workspace/query-service/target/*.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]
