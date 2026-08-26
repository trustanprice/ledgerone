#!/usr/bin/env bash
# Writes the most recently created CloudFormation changeset for $1 (a
# stack name) to $GITHUB_STEP_SUMMARY, as a resource-level diff plus the
# exact `execute-change-set` command a human needs to run to apply it —
# this workflow's deploy role can create/describe changesets but not
# execute them (see infra/AGENTS.md and infra/ledgerone-stack.yml's
# ChangeSetPreview policy), so that command is the actual next step, not
# just informational.
#
# Whether `aws cloudformation deploy --no-execute-changeset` leaves an
# empty changeset object behind (vs. deleting it) when the template
# already matches the deployed stack hasn't been confirmed against a real
# run yet — this script treats "no changeset found" the same as "no
# changes" rather than assuming either behavior, so it degrades to a
# clear message instead of failing either way.
set -euo pipefail

STACK="$1"

CHANGESET_ARN=$(aws cloudformation list-change-sets --stack-name "$STACK" \
  --query 'sort_by(Summaries, &CreationTime)[-1].ChangeSetId' --output text 2>/dev/null || true)

{
  echo "## CloudFormation changeset preview — $STACK"
  echo
} >> "$GITHUB_STEP_SUMMARY"

if [ -z "$CHANGESET_ARN" ] || [ "$CHANGESET_ARN" = "None" ]; then
  echo "No changeset found for \`$STACK\` — either the preview step didn't run, or the template already matches the deployed stack and AWS didn't retain an empty changeset." >> "$GITHUB_STEP_SUMMARY"
  exit 0
fi

echo "Changeset: \`$CHANGESET_ARN\`" >> "$GITHUB_STEP_SUMMARY"
echo >> "$GITHUB_STEP_SUMMARY"

CHANGE_COUNT=$(aws cloudformation describe-change-set \
  --stack-name "$STACK" --change-set-name "$CHANGESET_ARN" \
  --query 'length(Changes)' --output text)

if [ "$CHANGE_COUNT" = "0" ]; then
  echo "No changes — the deployed stack already matches \`infra/ledgerone-stack.yml\`." >> "$GITHUB_STEP_SUMMARY"
  exit 0
fi

{
  echo '```'
  aws cloudformation describe-change-set \
    --stack-name "$STACK" --change-set-name "$CHANGESET_ARN" \
    --query 'Changes[].ResourceChange.{Action:Action,Resource:LogicalResourceId,Type:ResourceType,Replacement:Replacement}' \
    --output table
  echo '```'
  echo
  echo "This role cannot execute the changeset (preview-only, by design). To apply it, from an authenticated admin session:"
  echo '```'
  echo "aws cloudformation execute-change-set --stack-name $STACK --change-set-name $CHANGESET_ARN"
  echo '```'
} >> "$GITHUB_STEP_SUMMARY"
