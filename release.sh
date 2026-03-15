#!/bin/bash
# Release script for Mac-D-Alert (Sovson Analytics)

PROJECT_DIR="/home/daniel/Mac-D-Alert"
cd "$PROJECT_DIR" || exit

echo "🚀 Starting Mac-D-Alert release process..."

# 1. Versioning
# Since Mac-D-Alert doesn't have an auto-incrementer script like Minecraft yet,
# we will use the date-patch format or manual input.
NEW_VERSION="1.2.2" # Manual set for this first run
echo "Target version: $NEW_VERSION"

# 2. Update PROJECT_BRIEF.md
sed -i "s/Status: v$current_version/Status: v$NEW_VERSION/" PROJECT_BRIEF.md

# 3. Create a bundle of the n8n workflows for the release asset
echo "📦 Packaging n8n workflows..."
ZIP_NAME="n8n_workflows_v$NEW_VERSION.zip"
zip -j "$ZIP_NAME" n8n_workflow_*.json

# 4. Commit and Push
echo "📤 Committing version update..."
git add PROJECT_BRIEF.md
git commit -m "chore: release v$NEW_VERSION"
git push origin main

# 5. Create GitHub Release
echo "🐙 Creating GitHub Release v$NEW_VERSION..."
gh release create "v$NEW_VERSION" "$ZIP_NAME" \
    --title "Mac-D-Alert v$NEW_VERSION" \
    --notes "Release of Mac-D-Alert v$NEW_VERSION. Includes the latest n8n workflow JSONs for import."

# Clean up
rm "$ZIP_NAME"

echo "🎉 Release v$NEW_VERSION is live!"
