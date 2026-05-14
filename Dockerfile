# Eclipse Temurin gives us a clean Java 17 JDK on Ubuntu Jammy
FROM eclipse-temurin:17-jdk-jammy

# Install Python 3.11 and pip
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-dev \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Make python3.11 the default python/pip
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

# Explicitly set JAVA_HOME so PySpark can find the JVM at build time and runtime.
# eclipse-temurin sets this in the shell profile but not as a Docker ENV layer,
# so RUN commands in subsequent layers may not inherit it.
ENV JAVA_HOME=/opt/java/openjdk

WORKDIR /app

# Install Python dependencies first (layer is cached unless requirements.txt changes)
# copy the repo’s requirements.txt from the build context into /app in the image.
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install -r requirements.txt runs in the image. For that to work, requirements.txt must already be in the image. So need a COPY requirements.txt . (or equivalent) before that RUN.


# Pre-bake Spark/Sedona jars into the image so container startup doesn't re-download them.
# Runs a minimal PySpark session that triggers Ivy jar resolution and caches to ~/.ivy2.
# This layer is cached unless sedona_client.py (where jar versions live) changes.
# By adding COPY src/sedona_client.py /tmp/sedona_client_ref.py, you make this layer depend on the content of sedona_client.py. When you change that file (e.g. you bump Sedona or Hadoop versions there), the COPY produces a new layer, so the next RUN is no longer cached and runs again — and re-downloads JARs with the (intended) new versions.
ENV SEDONA_PACKAGES="org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.8.1,org.datasyslab:geotools-wrapper:1.8.1-33.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

# Pre-bake Spark/Sedona jars — cache key tied to sedona_client.py where jar versions live
COPY src/sedona_client.py /tmp/sedona_client_ref.py
RUN PYSPARK_SUBMIT_ARGS="--packages ${SEDONA_PACKAGES} pyspark-shell" \
    python3 -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local').getOrCreate().stop()"

# Copy application source
COPY src/ ./src/
COPY tests/ ./tests/
COPY pytest.ini .

# Default: run unit tests (override CMD to run the agent)
CMD ["pytest", "tests/", "-v", "-m", "not integration"]
