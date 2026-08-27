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

## Planned: scheduled, unattended deployment

Both `dbt-build.yml` and `deploy-infra.yml` are `workflow_dispatch`-only
today, on purpose — the intent is for this to eventually become a real
scheduled deployment (a `schedule:` cron trigger on `dbt-build.yml`,
running `--target prod` unattended), not to stay a manually-triggered demo
forever. That's deliberately not built yet: a handful of clean manual
dispatches is evidence the mechanics work, not evidence it's safe to run
unattended against `prod` with no one watching. `dbt-build.yml` now
writes a clear `:x: dbt-build failed` banner (environment, run link) to
the job's `$GITHUB_STEP_SUMMARY` on failure — but that's pull-based
visibility, not push-based alerting: it makes a broken run impossible to
miss *if someone opens the Actions tab*, not something that pages or
pings anyone. That's a real gap closed for a dispatch-only workflow a
human just triggered and is already watching; it's a much smaller one
closed for the eventual unattended/cron version, where by definition no
one is watching by default. Before adding the cron trigger, at minimum:
more dispatch history across both environments, real push-based alerting
(a Slack webhook, email via SES, or a CloudWatch alarm — not yet chosen
or wired up) for the case where no one is looking at the Actions tab when
a scheduled run breaks, and a real decision on whether
`deploy-infra.yml`'s deploy role gets `ExecuteChangeSet`/`iam:*` to
actually apply what it can now only preview (see above) — an unattended
pipeline that also can't fix its own infra when it drifts is a narrower
kind of "automated" than the eventual goal.

## Cost monitoring: a billing alarm, not IaC-managed

Separate from everything above and **not** defined in `ledgerone-stack.yml`
— created directly via CLI against the real account, not through
CloudFormation, because it's account-level billing configuration
(`AWS/Billing` metrics only exist in `us-east-1` and depend on the
account's root user enabling "Receive Billing Alerts," a setting IAM
permissions alone can't grant regardless of role — even
AdministratorAccess hits this wall):

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

**A known gap, stated plainly**: this alarm/topic/dashboard exist only in
the live AWS account, not as CloudFormation in this repo — meaning they
aren't reproducible by redeploying `ledgerone-stack.yml`, and a fresh
environment wouldn't get them automatically. Moving them into IaC (a new
`AWS::CloudWatch::Alarm` + `AWS::SNS::Topic` + `AWS::CloudWatch::Dashboard`
in the template, parameterized by environment) would be the correct fix,
not yet done.

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
- Neither workflow has a `schedule:` trigger yet — both are
  `workflow_dispatch`-only until dispatched-by-hand runs build enough
  confidence to trust either unattended.
