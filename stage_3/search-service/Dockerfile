FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn clean package -q

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
RUN apk add --no-cache curl
COPY --from=build /app/target/search-service-1.0.0.jar ./app.jar
COPY --from=build /app/target/libs ./libs

# Puerto REST del Search Service
EXPOSE 8080
# Puerto Hazelcast
EXPOSE 5701

CMD ["java", \
     "-Xmx512m", \
     "-Dhazelcast.multicast.group=224.2.2.3", \
     "-jar", "app.jar"]
