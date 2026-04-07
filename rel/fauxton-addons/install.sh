#!/bin/bash
# Install custom Fauxton addons into the Fauxton source tree.
# Run this after `./configure` fetches Fauxton and before building.
#
# Usage: ./rel/fauxton-addons/install.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FAUXTON_DIR="$(dirname "$SCRIPT_DIR")/../src/fauxton"

if [ ! -d "$FAUXTON_DIR/app/addons" ]; then
    echo "Fauxton not found at $FAUXTON_DIR. Run ./configure first."
    exit 1
fi

echo "Installing auto-shard Fauxton addon..."

# Copy top-level addon files
mkdir -p "$FAUXTON_DIR/app/addons/autoshard"
cp "$SCRIPT_DIR/autoshard/"*.js "$FAUXTON_DIR/app/addons/autoshard/"

# Copy components subdirectory (controller + status/controls/tasks/reservations panels)
mkdir -p "$FAUXTON_DIR/app/addons/autoshard/components"
cp "$SCRIPT_DIR/autoshard/components/"*.js "$FAUXTON_DIR/app/addons/autoshard/components/"

# Copy SCSS asset
mkdir -p "$FAUXTON_DIR/app/addons/autoshard/assets/scss"
cp "$SCRIPT_DIR/autoshard/assets/scss/"*.scss "$FAUXTON_DIR/app/addons/autoshard/assets/scss/"

# Empty tests dir for consistency with other addons
mkdir -p "$FAUXTON_DIR/app/addons/autoshard/__tests__"

# Update load_addons.js to include our addon
cp "$SCRIPT_DIR/load_addons.js" "$FAUXTON_DIR/app/load_addons.js"

echo "Done. Auto-shard addon installed at $FAUXTON_DIR/app/addons/autoshard/"
