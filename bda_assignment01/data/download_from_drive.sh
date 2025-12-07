#!/bin/bash
set -euo pipefail
ARCHIVE='labs-final-lab1-assignment-data.zip'
REMOTE='gdrive'
REMOTE_ROOT='bda-website-data'
 
has(){ command -v "$1" >/dev/null 2>&1; }
if ! has rclone; then echo "rclone not found; install and configure rclone before running this script." >&2; exit 1; fi
 
DEST_DIR="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$DEST_DIR"
 
echo "Fetching $ARCHIVE from $REMOTE:$REMOTE_ROOT"
rclone copyto "$REMOTE:$REMOTE_ROOT/$ARCHIVE" "$DEST_DIR/$ARCHIVE" --progress || { echo "Warning: failed to fetch $ARCHIVE" >&2; exit 1; }
 
echo "Unzipping $ARCHIVE into $DEST_DIR (stripping paths)"
# -j = junk paths: extract files without their stored directories
unzip -o -j "$DEST_DIR/$ARCHIVE" -d "$DEST_DIR" || { echo "Warning: unzip failed" >&2; exit 1; }
echo "Done."