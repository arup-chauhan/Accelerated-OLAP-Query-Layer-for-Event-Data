FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace

COPY pom.xml ./
COPY common common
COPY cache-service cache-service

RUN mvn -q -DskipTests -pl cache-service -am package

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /workspace/cache-service/target/*.jar app.jar
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "app.jar"]
