#!/usr/bin/env bash
set -euo pipefail

mkdir -p out
image="ghcr.io/${GITHUB_REPOSITORY,,}/sail:$P3_NEW_SHA"
printf '%s\n' "$P3_NEW_SHA" > out/upstream-sha.txt
df -h / > out/disk-before.txt
if ! docker manifest inspect "$image" >/dev/null 2>&1; then
  dockerfile=$(mktemp)
  trap 'rm -f "$dockerfile"' EXIT
  sed -e 's|FROM python:${PYTHON_VERSION}-slim|FROM python:3.14.10-slim@sha256:22eafb370920ad8b429e0aec3fff433e0669968519f5d60469d83729c138b28b|' \
      -e '/^WORKDIR \/app/i ENV CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_STRIP=symbols' \
      -e 's/cargo build -p/cargo build --locked -p/' docker/sail/Dockerfile > "$dockerfile"
  docker build --progress=plain --build-arg "SAIL_SHA=$P3_NEW_SHA" -f "$dockerfile" \
    -t "$image" docker/sail 2>&1 | tee out/build.log
  docker push "$image" | tee -a out/build.log
else
  docker pull -q "$image"
fi
actual=$(docker run --rm --entrypoint cat "$image" /SAIL_SHA)
[[ "$actual" == "$P3_NEW_SHA" ]]
digest=$(docker image inspect --format '{{index .RepoDigests 0}}' "$image")
jq -n --arg sha "$actual" --arg image "$image" --arg digest "$digest" \
  '{status:"built",sail_sha:$sha,image:$image,digest:$digest,rust:"1.99.1",python:"3.14.10",lto:"thin"}' > out/build.json
