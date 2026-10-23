#!/usr/bin/env bash
# Download the Oct and Nov 2019 REES46 CSV files.
# Requires KAGGLE_USERNAME and KAGGLE_KEY. Does not print the key.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RAW_DIR="${RAW_DIR:-$ROOT/data/raw}"
DATASET="mkechinov/ecommerce-behavior-data-from-multi-category-store"

if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

if [[ -z "${KAGGLE_USERNAME:-}" || -z "${KAGGLE_KEY:-}" ]]; then
  echo "Set KAGGLE_USERNAME and KAGGLE_KEY. See .env.example." >&2
  exit 1
fi

if ! command -v kaggle >/dev/null 2>&1; then
  echo "The kaggle CLI is not on PATH. Install it with: pip install kaggle" >&2
  exit 1
fi

mkdir -p "$RAW_DIR"

download_one() {
  local file_name="$1"
  if [[ -f "$RAW_DIR/$file_name" && "${FORCE_DOWNLOAD:-0}" != "1" ]]; then
    echo "Already present: $RAW_DIR/$file_name"
    return
  fi
  kaggle datasets download -d "$DATASET" -f "$file_name" -p "$RAW_DIR" --force
  if [[ -f "$RAW_DIR/${file_name}.zip" ]]; then
    unzip -o "$RAW_DIR/${file_name}.zip" -d "$RAW_DIR"
    rm -f "$RAW_DIR/${file_name}.zip"
  fi
  if [[ ! -f "$RAW_DIR/$file_name" ]]; then
    echo "Expected $RAW_DIR/$file_name after download." >&2
    exit 1
  fi
}

download_one "2019-Oct.csv"
download_one "2019-Nov.csv"
echo "CSV files are in $RAW_DIR"
