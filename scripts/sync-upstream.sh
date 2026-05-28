#!/usr/bin/env bash
# Merges upstream/master into the fork's master, preserving the access-modifier
# changes this fork makes to src/Confluent.Kafka/Consumer.cs and
# src/Confluent.Kafka/Producer.cs (see the PATCHES array below).
#
# Behavior:
#   - Exits 1 on merge conflict.
#   - If any of our patches is missing post-merge, tries to re-apply it by
#     matching the original upstream pattern. If the original isn't found either,
#     upstream restructured that area and manual review is required.
#   - Never pushes. On success, prints the command to push.
#
# Run from a clean working tree on any branch. Requires: `upstream` remote set
# to confluentinc/confluent-kafka-dotnet.

set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Working tree is not clean. Commit or stash changes first." >&2
    exit 1
fi

if ! git config --get remote.upstream.url >/dev/null; then
    echo "No 'upstream' remote configured." >&2
    exit 1
fi

echo "Fetching upstream..."
git fetch upstream

echo "Switching to master..."
git checkout master

echo "Merging upstream/master..."
if ! git merge --no-edit -m "[sync] Merged upstream/master" upstream/master; then
    echo "Merge conflict. Resolve manually, then commit and rerun verification." >&2
    exit 1
fi

# Each entry: "<file>|<desired regex>|<original regex>|<replacement line>"
PATCHES=(
    "src/Confluent.Kafka/Consumer.cs|^        internal SafeKafkaHandle kafkaHandle;$|^        private SafeKafkaHandle kafkaHandle;$|        internal SafeKafkaHandle kafkaHandle;"
    "src/Confluent.Kafka/Consumer.cs|^        internal Exception handlerException = null;$|^        private Exception handlerException = null;$|        internal Exception handlerException = null;"
    "src/Confluent.Kafka/Consumer.cs|^        internal int cancellationDelayMaxMs;$|^        private int cancellationDelayMaxMs;$|        internal int cancellationDelayMaxMs;"
    "src/Confluent.Kafka/Consumer.cs|^        protected virtual int StatisticsCallback\(|^        private int StatisticsCallback\(|        protected virtual int StatisticsCallback("
    "src/Confluent.Kafka/Producer.cs|^        protected virtual void DeliveryReportCallbackImpl\(|^        private void DeliveryReportCallbackImpl\(|        protected virtual void DeliveryReportCallbackImpl("
    "src/Confluent.Kafka/Producer.cs|^        internal SafeKafkaHandle ownedKafkaHandle;$|^        private SafeKafkaHandle ownedKafkaHandle;$|        internal SafeKafkaHandle ownedKafkaHandle;"
    "src/Confluent.Kafka/Producer.cs|^        internal Exception handlerException = null;$|^        private Exception handlerException = null;$|        internal Exception handlerException = null;"
    "src/Confluent.Kafka/Producer.cs|^        protected virtual int StatisticsCallback\(|^        private int StatisticsCallback\(|        protected virtual int StatisticsCallback("
)

reapplied=0
failed=0
touched_files=()

for entry in "${PATCHES[@]}"; do
    IFS='|' read -r file want have replace <<<"$entry"

    if grep -qE "$want" "$file"; then
        continue
    fi

    echo "Missing patch in $file matching: $want"

    if ! grep -qE "$have" "$file"; then
        echo "  Original upstream pattern not found either — upstream restructured this area." >&2
        echo "  Manual intervention required." >&2
        failed=$((failed + 1))
        continue
    fi

    echo "  Found original; re-applying."
    tmp=$(mktemp)
    sed -E "s|$have|$replace|" "$file" >"$tmp" && mv "$tmp" "$file"

    if ! grep -qE "$want" "$file"; then
        echo "  Re-apply produced no match. Aborting." >&2
        exit 1
    fi

    touched_files+=("$file")
    reapplied=$((reapplied + 1))
done

if [ "$failed" -gt 0 ]; then
    echo "$failed patch(es) could not be re-applied. Review and fix manually." >&2
    exit 1
fi

if [ "$reapplied" -gt 0 ]; then
    echo "Re-applied $reapplied patch(es). Amending merge commit."
    git add "${touched_files[@]}"
    git commit --amend --no-edit
fi

echo "Sync complete. Push with: git push origin master"
