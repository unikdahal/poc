FROM python:3.14-slim
RUN python -m pip download --no-deps --only-binary=:all: --dest /tmp/wheels pysail==0.7.3 \
    && cd /tmp/wheels \
    && printf '%s  %s\n' b9a308f8216b65c15f0e3666482ba602e962ec25408d42570a18b35f090dfdda *.whl > /wheel.sha256 \
    && sha256sum --check /wheel.sha256 \
    && python -m pip install --no-cache-dir ./*.whl pyspark-client==4.2.1 \
    && cd / \
    && rm -r /tmp/wheels
ENTRYPOINT ["python", "-m", "pysail"]
