"""Resolve the newest released versions of the reference stack and write versions.json."""

import json
import re
import sys
import urllib.request
from pathlib import Path


def get(url: str) -> str:
    with urllib.request.urlopen(url, timeout=60) as r:
        return r.read().decode()


def maven_latest(group: str, artifact: str, prefix: str = "") -> str:
    xml = get(f"https://repo1.maven.org/maven2/{group.replace('.', '/')}/{artifact}/maven-metadata.xml")
    versions = re.findall(r"<version>([^<]+)</version>", xml)
    versions = [v for v in versions if v.startswith(prefix) and re.fullmatch(r"[0-9.]+", v)]
    versions.sort(key=lambda s: [int(x) for x in s.split(".")])
    return versions[-1]


def pypi_latest(pkg: str, prefix: str) -> str:
    data = json.loads(get(f"https://pypi.org/pypi/{pkg}/json"))
    vs = [v for v in data["releases"] if v.startswith(prefix) and re.fullmatch(r"[0-9.]+", v)]
    vs.sort(key=lambda s: [int(x) for x in s.split(".")])
    return vs[-1]


def main():
    out = {
        "spark": pypi_latest("pyspark", "3.5."),
        "iceberg": maven_latest("org.apache.iceberg", "iceberg-spark-runtime-3.5_2.12"),
        "comet": maven_latest("org.apache.datafusion", "comet-spark-spark3.5_2.12"),
        "adbc": pypi_latest("adbc-driver-flightsql", ""),
    }
    for a in sys.argv[1:]:
        k, v = a.split("=", 1)
        out[k] = v
    Path("versions.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
