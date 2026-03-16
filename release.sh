#!/bin/bash
# Release script for Mac-D-Alert (Sovson Analytics)

PROJECT_DIR="/home/daniel/Mac-D-Alert"
cd "$PROJECT_DIR" || exit

echo "🚀 Starting Mac-D-Alert release process..."

# 1. Versioning
# Extract version from PROJECT_BRIEF.md
current_version=$(grep "Status:" PROJECT_BRIEF.md | sed 's/.*v\([0-9.]*\).*/\1/')
echo "Current version in PROJECT_BRIEF: $current_version"

if [ -z "$current_version" ]; then
    current_version="1.2.4"
    echo "Fallback version: $current_version"
fi

# Manual set for this major robustness jump
NEW_VERSION="1.3.0"
echo "Target version: v$NEW_VERSION"

# 2. Update PROJECT_BRIEF.md
sed -i "s/v$current_version/v$NEW_VERSION/" PROJECT_BRIEF.md

# 3. Update n8n Workflow Titles (INTERNAL)
echo "📝 Updating internal workflow titles to v$NEW_VERSION..."
for f in n8n_workflow_*.json; do
    # Remove any existing version tag
    sed -i 's/ (v[0-9.]*)//g' "$f"
    # Inject new version tag
    sed -i "s/\"name\": \"Mac-D-Alert: \([^\"]*\)\"/\"name\": \"Mac-D-Alert: \1 (v$NEW_VERSION)\"/" "$f"
done

# 4. Create a bundle with versioned FILENAMES
echo "📦 Packaging versioned workflow files..."
ZIP_NAME="n8n_workflows_v$NEW_VERSION.zip"
BUILD_DIR=$(mktemp -d)

for f in n8n_workflow_*.json; do
    base_name=$(basename "$f" .json)
    cp "$f" "$BUILD_DIR/${base_name}_v${NEW_VERSION}.json"
done

cd "$BUILD_DIR" || exit
zip -j "$PROJECT_DIR/$ZIP_NAME" *.json > /dev/null
cd "$PROJECT_DIR" || exit
rm -rf "$BUILD_DIR"

# 5. Commit and Push
echo "📤 Committing updates..."
git add PROJECT_BRIEF.md n8n_workflow_*.json
git commit -m "chore: release v$NEW_VERSION"
git push origin main

# 6. Create GitHub Release
echo "🐙 Creating GitHub Release v$NEW_VERSION..."
gh release create "v$NEW_VERSION" "$ZIP_NAME" \
    --title "Mac-D-Alert v$NEW_VERSION (Robustness Update)" \
    --notes "Major robustness and self-healing update.
- **Data Auditor**: New workflow for weekly health checks and repairs.
- **Self-Healing**: Detection scripts now automatically backfill history.
- **Usage Tracking**: Monitor API spend in the dashboard.
- **Improved UI**: High-contrast legends on all stock graphs."

# Clean up
rm "$ZIP_NAME"

echo "🎉 Release v$NEW_VERSION is live!"
