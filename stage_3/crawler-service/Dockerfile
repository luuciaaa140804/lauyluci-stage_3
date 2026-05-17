FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn clean package -q

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
RUN apk add --no-cache curl
COPY --from=build /app/target/crawler-service-1.0.0.jar ./app.jar
COPY --from=build /app/target/libs ./libs

EXPOSE 8090

CMD ["java", "-jar", "app.jar"]
