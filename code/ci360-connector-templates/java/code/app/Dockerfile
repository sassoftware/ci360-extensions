FROM eclipse-temurin:21-jdk AS build

WORKDIR /app
COPY .mvn/ .mvn
COPY mvnw pom.xml ./
RUN ./mvnw dependency:go-offline

COPY src ./src
RUN ./mvnw package -DskipTests

FROM eclipse-temurin:21-jre
COPY --from=build /app/target/app.jar /app/app.jar

EXPOSE 8080
WORKDIR /app
CMD ["java", "-jar", "app.jar"]
