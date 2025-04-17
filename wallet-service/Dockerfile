FROM openjdk:21-jdk
COPY . .
RUN chmod +x ./mvnw
RUN ./mvnw clean package
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "target/demo_onlycrud-0.0.1-SNAPSHOT.jar"]






# FROM openjdk:21-jdk
# COPY target/demo_onlycrud-0.0.1-SNAPSHOT.jar /
# EXPOSE 8080
# ENTRYPOINT ["java", "-jar", "/demo_onlycrud-0.0.1-SNAPSHOT.jar"]