# Runtime image for a Spark 3.5 (+ Comet) executor that runs lc_python_runner workers sandboxed.
# Build context: lc-python-runner/ with the built jars staged in build/jars/.
FROM ubuntu:24.04

ARG SPARK_VERSION=3.5.10
ARG PYARROW_VERSION=21.0.0
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless python3 python3-pip python3-seccomp \
      bubblewrap iptables util-linux procps ca-certificates netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Spark from the PySpark wheel (bundles every Spark jar and spark-submit); pyarrow for workers.
RUN pip3 install --no-cache-dir --break-system-packages \
      "pyspark==${SPARK_VERSION}" "pyarrow==${PYARROW_VERSION}" numpy

ENV SPARK_HOME=/usr/local/lib/python3.12/dist-packages/pyspark \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# The executor JVM user. uid 185 matches the apache/spark images.
RUN useradd --uid 185 --create-home --shell /bin/bash sparkuser

COPY sandbox/bin/ /opt/lcpy/bin/
COPY sandbox/etc/ /opt/lcpy/etc/
RUN python3 /opt/lcpy/bin/gen-seccomp.py /opt/lcpy/seccomp.bpf && chmod 0644 /opt/lcpy/seccomp.bpf

COPY build/jars/ /opt/lcpy/jars/
COPY registry/ /opt/lcpy/registry/
COPY e2e/ /opt/lcpy/e2e/
RUN chmod -R a+rX /opt/lcpy && chmod 0755 /opt/lcpy/bin/*

WORKDIR /home/sparkuser
ENTRYPOINT ["/opt/lcpy/bin/entrypoint.sh"]
CMD ["bash", "-lc", "sleep infinity"]
