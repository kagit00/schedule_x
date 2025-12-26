# ===============================
# Stage 1: Build + optional AI-assisted test generation (JDK 17)
# ===============================
FROM maven:3.9.9-eclipse-temurin-17 AS builder
WORKDIR /app

# Copy pom.xml first for caching
COPY pom.xml ./
# Pre-resolve dependencies (best-effort, non-fatal)
RUN mvn -B dependency:resolve dependency:resolve-plugins || true

# Copy source code
COPY src ./src

# Optional: enable AI test generation during build
ARG RUN_TESTGEN=false

RUN if [ "$RUN_TESTGEN" = "true" ]; then \
      echo "Installing Diffblue Cover CLI for AI test generation..."; \
      apt-get update && apt-get install -y curl unzip && rm -rf /var/lib/apt/lists/*; \
      curl -fsSL https://get.diffblue.com/cover-cli/install.sh | sh; \
      ./cover install-maven-plugin; \
      echo "Generating tests with Diffblue Cover..."; \
      ./cover create . --batch --verbose || true; \
    else \
      echo "Skipping AI-assisted test generation"; \
    fi

# Build the final JAR (skip tests for faster build)
RUN mvn clean package -DskipTests

# Ensure the jar exists at a predictable path for the next stage
RUN ls -lah target || true

# ===============================
# Stage 2: Runtime (JDK 17)
# ===============================
FROM eclipse-temurin:17-jdk
WORKDIR /app

# Install netcat for dependency checks
RUN apt-get update && apt-get install -y netcat-openbsd curl && rm -rf /var/lib/apt/lists/*

# Copy built JAR from builder stage
COPY --from=builder /app/target/*.jar app.jar

# Expose Spring Boot app port
EXPOSE 8080

# JVM & dependency defaults (override via docker-compose)
# --- IMPORTANT FIX FOR LMDB ON JAVA 17 ---
ENV JAVA_OPTS="\
-Xms512m \
-Xmx1200m \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
"

ENV DEPENDENCIES="kafka:9092 postgres:5432 redis:6379"

# Start-up script: wait for dependencies, then run the app
CMD ["sh", "-c", "\
mkdir -p /logs; \
echo \"==== NEW RUN `date` ====\" >> /logs/schedulex.log; \
for dep in $DEPENDENCIES; do \
  host=$(echo $dep | cut -d: -f1); \
  port=$(echo $dep | cut -d: -f2); \
  until nc -z $host $port; do \
    echo waiting for $host:$port >> /logs/schedulex.log; sleep 2; \
  done; \
done; \
echo 'All dependencies are up! Starting ScheduleX...' >> /logs/schedulex.log; \
exec java $JAVA_OPTS -jar app.jar >> /logs/schedulex.log 2>&1"]

