# AGENTS.md — infra/

Fleet: [../AGENTS.md](../AGENTS.md)

## Purpose

CloudFormation for the one real (small, portfolio-scale) cloud deployment
alongside this project's default local `make all` path. Not part of the
numbered roadmap, same spirit as the credit-risk module and
`apps/walkthrough/` — built to demonstrate the pattern, not because this
project needs a cloud warehouse.

- **`ledgerone-stack.yml`** — one template, deployed twice (`ledgerone-dev`
  and `ledgerone-prod` stacks, both in `us-east-1`), parameterized by
  `Environment`. Each deploy creates:
  - An S3 bucket (`ledgerone-raw-{env}-<account-id>`) for raw Parquet
    archival — encrypted, versioned, all public access blocked, objects
    expire after 90 days (`RawDataExpirationDays` parameter).
  - An IAM role (`ledgerone-{env}-github-actions`) trusting GitHub's OIDC
    provider (`token.actions.githubusercontent.com`), scoped via the
    trust policy's `sub` condition to `repo:<org>/<repo>:environment:{env}`
    — a workflow run against the `dev` GitHub Environment cannot assume
    the `prod` role. Its permissions policy has two parts: `s3:ListBucket`/
    `GetObject`/`PutObject` scoped to just this one bucket's ARN, and a
    separate `ChangeSetPreview` statement scoped to just this one stack's
    ARN granting `cloudformation:CreateChangeSet`/`DescribeChangeSet`/
    `DescribeStacks`/`GetTemplate`/`ListChangeSets` — **no**
    `ExecuteChangeSet` and **no** `iam:*` action of any kind, deliberately
    (see below).

## The bootstrap problem, and how it was actually solved

The GitHub OIDC provider itself (`token.actions.githubusercontent.com`)
is created once per AWS account, out of band, by hand — it's not in this
template, and normally isn't recreated per project.

The stacks in this template have a real chicken-and-egg problem: a GitHub
Actions workflow can't assume `ledgerone-{env}-github-actions` to deploy
the stack that creates that same role, the first time. Both `ledgerone-dev`
and `ledgerone-prod` were bootstrapped from an authenticated admin AWS CLI
session instead — `aws cloudformation deploy --no-execute-changeset`,
a human reviewing the exact resource diff via `describe-change-set`
(confirmed: two `Add` actions, `AWS::IAM::Role` and `AWS::S3::Bucket`,
nothing else), then a manual `execute-change-set`. Never auto-applied,
never run by an agent without that human review step in between.

## Why the deploy role can preview but not apply changes

`.github/workflows/deploy-infra.yml` (dispatch-only, no `push` trigger)
creates a CloudFormation changeset for this stack and prints its
resource-level diff to the job's `$GITHUB_STEP_SUMMARY` — but the
`ledgerone-{env}-github-actions` role it assumes has only enough
permission to do that (the `ChangeSetPreview` policy above). It has no
`cloudformation:ExecuteChangeSet` and no `iam:*` action of any kind: the
workflow can show exactly what would change, but cannot make it happen.
Applying a template change still requires a human running
`execute-change-set` from an authenticated admin session — the job
summary prints the exact command against the real changeset it just
created, so that step is a copy-paste, not a lookup.

Granting this role permission to *execute* a changeset — updating the
CloudFormation stack that defines its own IAM trust policy — is a real,
self-modifying-pipeline pattern (the same one AWS CDK Pipelines uses
deliberately) — but it's also a genuine privilege-escalation surface: a
compromised workflow run could, in principle, rewrite its own trust
policy to trust something broader. That tradeoff hasn't been made yet;
preview-only was the narrower, already-decided first step. If execute
permission is granted later, the fix belongs here — a narrowly-scoped
`cloudformation:ExecuteChangeSet` + `iam:*` statement, resource-restricted
to this one stack's ARN and this one role's ARN, not a blanket grant.

## Now live: scheduled, unattended deployment — with one open gap

`dbt-build.yml` has a `schedule: cron: '0 10 * * *'` trigger (daily,
10:00 UTC, always targets `prod`) alongside `workflow_dispatch`. This
landed after both prerequisites this doc used to list as required were
actually met: a handful of clean manual dispatches across both
environments (136/136 dbt tests, verified from real run logs), and real
push-based alerting — its "Report failure" step now publishes to the
existing `ledgerone-billing-alerts` SNS topic (already has a confirmed
email subscription, from the billing-alarm work below) in addition to the
job-summary banner it already wrote, so a broken run reaches an inbox,
not just a tab nobody's watching.

**Not yet true**: the IAM permission that publish call needs
(`sns:Publish`, the `PipelineAlerting` policy statement in
`ledgerone-stack.yml`, scoped to exactly that one topic ARN via the new
`AlertingTopicArn` parameter) hasn't been applied to the live
`ledgerone-{env}-github-actions` roles — it goes through the same
human-executed changeset flow every other IAM change here does, and
wasn't run before the workflow change landed. Until it is, a scheduled
run that fails hits `AccessDenied` on the SNS publish itself, on top of
whatever broke `dbt-build` — the alerting exists in code, not yet in the
account. Apply it (repeat for `ledgerone-dev` if dev-triggered failures
should alert too — the schedule trigger itself only ever targets `prod`):

```bash
aws cloudformation deploy \
  --template-file infra/ledgerone-stack.yml \
  --stack-name ledgerone-prod \
  --parameter-overrides Environment=prod GitHubRepo=trustanprice/ledgerone \
  --capabilities CAPABILITY_NAMED_IAM \
  --no-execute-changeset

aws cloudformation describe-change-set --stack-name ledgerone-prod --change-set-name <name from output>
# review the diff (expect exactly one Add: the PipelineAlerting policy), then:
aws cloudformation execute-change-set --stack-name ledgerone-prod --change-set-name <same name>
```

`deploy-infra.yml`'s own gap — preview-only, no `ExecuteChangeSet`/`iam:*`
of any kind — is unrelated and still open; see the section above.

## Cost monitoring: now defined as CloudFormation, import not yet applied

Separate from everything above. Originally created directly via CLI
against the real account, not through CloudFormation, because it's
account-level billing configuration (`AWS/Billing` metrics only exist in
`us-east-1` and depend on the account's root user enabling "Receive
Billing Alerts," a setting IAM permissions alone can't grant regardless of
role — even AdministratorAccess hits this wall). All three are now also
defined in `ledgerone-stack.yml`, `Condition: IsProd` — `AWS/Billing`
metrics and this account's spend are account-wide, not per-environment,
so they exist once, in the `ledgerone-prod` stack only, not duplicated
into `ledgerone-dev`:

- A CloudWatch alarm, `ledgerone-billing-80-warning`, fires when
  `AWS/Billing EstimatedCharges` (currency USD) crosses **$80** — an
  early warning under the account's $100 credit cap, not at it.
- An SNS topic, `ledgerone-billing-alerts`, with a confirmed email
  subscription, is the alarm's notification target (both `ALARM` and
  `OK` transitions) — an alarm with no action attached only changes color
  in the console, it doesn't notify anyone, so this part isn't optional.
- A CloudWatch dashboard, `ledgerone-billing`, plots `EstimatedCharges`
  over time with reference lines at $80 and $100.

**Don't confuse this with the still-open pipeline-failure alerting gap**
in the "Planned: scheduled, unattended deployment" section above — that's
about a broken `dbt-build.yml` run going unnoticed, a completely
different concern from account spend. That gap is still real and still
open; this section only covers cost.

**Real, deliberate one-time cost**: setting this up required exactly one
`ce:GetCostAndUsage`-backed run of `src/export_finops_data.py` (a few
$0.01 API calls) to populate `finops_snapshot.json` with real data for
the first time — run once, by hand, not on a schedule, matching the
script's own design (see `src/AGENTS.md`). Everything else here (the
alarm, the SNS topic, the dashboard, the `AWS/Billing` metric itself) is
free — standard-resolution CloudWatch alarms and dashboards are covered
by AWS's always-free tier at this scale (1 alarm, 1 dashboard).

**Not yet applied**: these three already exist as real resources in the
account, so bringing them into the stack needs an IMPORT change set, not
a normal deploy — a plain `aws cloudformation deploy` would try to CREATE
new resources with names that already exist and fail outright.
`infra/import-billing-monitoring.json` has the resource-identifier
mapping (`AlarmName`/`TopicArn`/`DashboardName`) `--resources-to-import`
needs. Apply it:

```bash
aws cloudformation create-change-set \
  --stack-name ledgerone-prod \
  --change-set-name import-billing-monitoring \
  --change-set-type IMPORT \
  --template-body file://infra/ledgerone-stack.yml \
  --parameters ParameterKey=Environment,ParameterValue=prod ParameterKey=GitHubRepo,ParameterValue=trustanprice/ledgerone \
  --resources-to-import file://infra/import-billing-monitoring.json \
  --capabilities CAPABILITY_NAMED_IAM

aws cloudformation describe-change-set --stack-name ledgerone-prod --change-set-name import-billing-monitoring
# review -- expect exactly three Import actions, nothing else touched, then:
aws cloudformation execute-change-set --stack-name ledgerone-prod --change-set-name import-billing-monitoring
```

The import mapping's SNS `TopicArn` uses this account's ID as recorded
earlier in this project's history (`387143718798`) — not independently
re-verified against live AWS when this was written (no working AWS
session at the time), so confirm it matches the real topic's ARN before
running the import.

## Connects to

- `.github/workflows/dbt-build.yml` assumes `ledgerone-{env}-github-actions`
  to sync `data/raw/` to the environment's S3 bucket, then runs
  `dbt build --target ci_dev` (dev) or `--target prod` (prod) against
  MotherDuck — see [../dbt/AGENTS.md](../dbt/AGENTS.md) and
  `dbt/profiles.yml`'s `ci_dev`/`prod` targets.
- `.github/workflows/deploy-infra.yml` previews a redeploy of this
  template via `infra/scripts/summarize-changeset.sh` (changeset only —
  it cannot execute one); actually applying a change still goes through a
  human until the execute-permission question above is resolved.
- `dbt-build.yml` has a `schedule:` trigger (daily, targets `prod`); see
  above for the alerting permission it still needs applied.
  `deploy-infra.yml` stays `workflow_dispatch`-only — it can only preview
  a change, never apply one, so an auto-trigger would just create
  changesets no one asked to review yet.
