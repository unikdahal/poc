#!/usr/bin/env bash
set -euo pipefail

task=$1
variant=$2
source_dir=$3
[[ "${GITHUB_REF:-}" == refs/heads/agent2-phase3 ]]
[[ "$task" =~ ^[a-z][a-z0-9-]*$ && "$variant" =~ ^[a-z0-9][a-z0-9-]*$ ]]
branch="results/p3-$task"
ref="refs/heads/$branch"
repo="https://github.com/$GITHUB_REPOSITORY.git"
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
mkdir -p "$source_dir"
git -C "$dir" init -q -b results
git -C "$dir" config user.name p3-results
git -C "$dir" config user.email p3-results@localhost
git -C "$dir" remote add origin "$repo"
auth=$(printf 'x-access-token:%s' "$GH_TOKEN" | base64 -w0)
export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=http.https://github.com/.extraheader
export GIT_CONFIG_VALUE_0="AUTHORIZATION: basic $auth"
unset auth

for attempt in 1 2 3 4 5; do
  if git -C "$dir" ls-remote --exit-code origin "$ref" >/dev/null; then
    git -C "$dir" fetch -q --depth=1 origin "$ref"
    git -C "$dir" reset -q --hard FETCH_HEAD
  fi
  target="$variant"
  if [[ ! -f "$source_dir/result.json" && ! -f "$source_dir/build.json" && -d "$dir/$variant" ]]; then
    target="attempts/$GITHUB_RUN_ID/$variant"
  fi
  git -C "$dir" rm -q -r --ignore-unmatch "$target"
  mkdir -p "$dir/$target/logs"
  find "$source_dir" -maxdepth 1 \( -name '*.json' -o -name '*.txt' \) -type f | while IFS= read -r file; do
    cp "$file" "$dir/$target/"
  done
  while IFS= read -r -d '' file; do
    tail -c 1000000 "$file" > "$dir/$target/logs/$(basename "$file")"
  done < <(find "$source_dir" -maxdepth 1 -name '*.log' -print0)
  printf '{"run_id":"%s","revision":"%s","task":"%s","variant":"%s","attempt":"%s"}\n' \
    "$GITHUB_RUN_ID" "$GITHUB_SHA" "$task" "$variant" "$GITHUB_RUN_ATTEMPT" > "$dir/$target/run.json"
  git -C "$dir" add -A
  if git -C "$dir" diff --quiet --cached; then exit 0; fi
  git -C "$dir" commit -qm "Record $task $variant run $GITHUB_RUN_ID"
  if git -C "$dir" push -q origin "HEAD:$ref"; then exit 0; fi
  sleep "$attempt"
done
exit 1
