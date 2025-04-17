# Use Maven with OpenJDK 17 as the base image
FROM jelastic/maven:3.9.5-openjdk-21


# Set the working directory inside the container
WORKDIR /marketplace-service

# Copy all project files into the container
COPY . .

# Compile the project
RUN mvn compile

# Set entrypoint to run the application using Maven exec plugin
# ENTRYPOINT ["mvn", "exec:exec"]

ENTRYPOINT ["mvn", "exec:java", "-Dexec.mainClass=com.example.QuickstartApp"]