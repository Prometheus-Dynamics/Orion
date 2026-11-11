#!/bin/sh
# Compiles src/orion.ts to dist/, and notes which source dist/ was built from, so that a
# test can tell when the two have drifted apart.
set -eu
cd "$(dirname "$0")"
[ -d node_modules/typescript ] || npm install --no-audit --no-fund
npx tsc -p tsconfig.json
sha256sum src/orion.ts | cut -d' ' -f1 > dist/source.sha256
