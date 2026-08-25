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
    the `prod` role. Its permissions policy is scoped to just this one
    bucket's ARN (`s3:ListBucket`/`GetObject`/`PutObject`) — **no**
    CloudFormation or IAM permissions, deliberately (see below).

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

## Why the deploy role doesn't have CloudFormation permissions

`.github/workflows/deploy-infra.yml` exists (dispatch-only, no `push`
trigger) to redeploy this stack on future template changes, but the
`ledgerone-{env}-github-actions` role it would need to assume for that
has **zero** CloudFormation or IAM permissions right now — meaning that
workflow is currently non-functional by design, not by oversight.

Granting a CI role permission to update the CloudFormation stack that
defines its own IAM trust policy is a real, self-modifying-pipeline
pattern (the same one AWS CDK Pipelines uses deliberately) — but it's
also a genuine privilege-escalation surface: a compromised workflow run
could, in principle, rewrite its own trust policy to trust something
broader. That tradeoff hasn't been made yet. If it is, the fix belongs
here — a narrowly-scoped `cloudformation:*` + `iam:*` statement, resource-
restricted to this one stack's ARN and this one role's ARN, not a
blanket grant — plus a decision about whether changeset creation should
still require a human `execute-change-set`, mirroring the bootstrap flow.

## Planned: scheduled, unattended deployment

Both `dbt-build.yml` and `deploy-infra.yml` are `workflow_dispatch`-only
today, on purpose — the intent is for this to eventually become a real
scheduled deployment (a `schedule:` cron trigger on `dbt-build.yml`,
running `--target prod` unattended), not to stay a manually-triggered demo
forever. That's deliberately not built yet: a handful of clean manual
dispatches is evidence the mechanics work, not evidence it's safe to run
unattended against `prod` with no one watching. Before adding the cron
trigger, at minimum: more dispatch history across both environments,
CloudWatch alarming or a Slack/email notification on run failure (nothing
currently alerts on a broken scheduled run), and a real decision on the
`deploy-infra.yml` CloudFormation/IAM permissions question above — an
unattended pipeline that also can't fix its own infra when it drifts is a
narrower kind of "automated" than the eventual goal.

## Connects to

- `.github/workflows/dbt-build.yml` assumes `ledgerone-{env}-github-actions`
  to sync `data/raw/` to the environment's S3 bucket, then runs
  `dbt build --target ci_dev` (dev) or `--target prod` (prod) against
  MotherDuck — see [../dbt/AGENTS.md](../dbt/AGENTS.md) and
  `dbt/profiles.yml`'s `ci_dev`/`prod` targets.
- `.github/workflows/deploy-infra.yml` would redeploy this template, once
  the permissions question above is resolved.
- Neither workflow has a `schedule:` trigger yet — both are
  `workflow_dispatch`-only until dispatched-by-hand runs build enough
  confidence to trust either unattended.
