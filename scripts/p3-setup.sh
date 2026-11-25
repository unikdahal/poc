#!/usr/bin/env bash
set -euo pipefail
mkdir -p out
df -h / > out/disk-before.txt
python -m pip install -q --disable-pip-version-check -r scripts/p3-requirements.txt
python -m pip freeze > out/pip-freeze.txt
printf '{"spark":"3.5.10","iceberg":"1.12.0","comet":"1.2.0","adbc":"1.13.0"}\n' > versions.json
cp versions.json out/
export POLARIS_TAG=latest@sha256:bc548500f5ba8e798b7f8e107ad0b8fbd5b5c6d0c5437f56c5ca101219b796d0
export AWSCLI_TAG=2.39.6@sha256:eccdb3ec77e159d43ca6a750a70738c546883138be7f094498294a1477ec7e42
export RUSTFS_TAG=1.0.0-beta.8@sha256:fa19210ac4697c79d7ccca1ec9b0eb91aebacc6691991ffb14014bb3c67e6cc3
compose=(docker compose -f compose/docker-compose.yml)
if [[ "${P3_TASK:-}" == faults-storage ]]; then
  override="$(mktemp)"
  cat > "$override" <<'YAML'
services:
  polaris:
    extra_hosts:
      - "host.docker.internal:host-gateway"
YAML
  if [[ "${P3_VARIANT:-}" == catalog ]]; then
    cat >> "$override" <<'YAML'
    environment:
      polaris.authentication.token-broker.max-token-generation: PT2M
YAML
  fi
  compose+=(-f "$override")
fi
"${compose[@]}" up -d
for _ in {1..90}; do
  if curl -fsS http://localhost:8182/q/health >/dev/null 2>&1; then break; fi
  sleep 2
done
curl -fsS http://localhost:8182/q/health >/dev/null
python -c 'from harness.common import polaris_bootstrap, write_json; write_json("catalog-bootstrap", polaris_bootstrap())'
printf '%s' "$GH_TOKEN" | docker login ghcr.io -u "$GITHUB_ACTOR" --password-stdin
repo="ghcr.io/${GITHUB_REPOSITORY,,}/sail"
if [[ "${P3_VARIANT:-}" == newer ]]; then
  image="$repo:$P3_NEW_SHA@sha256:ceec56baad4fddc7cab653301863f0bcc783002eeec40f73ef7bea3cf0f167f4"
  expected="$P3_NEW_SHA"
else
  image="$repo:790c622e5c4d@sha256:c0257770f6faf19e1947ace80947067308af569e5338c3428adef251fe35a767"
  expected="$P3_PINNED_SHA"
fi
docker pull -q "$image"
actual=$(docker run --rm --entrypoint cat "$image" /SAIL_SHA)
[[ "$actual" == "$expected" ]]
printf 'SAIL_IMAGE=%s\nP3_SAIL_SHA=%s\n' "$image" "$actual" >> "$GITHUB_ENV"
docker image ls --digests > out/image-digests.txt
docker compose -f compose/docker-compose.yml ps > out/services.txt
df -h / > out/disk-after-setup.txt
