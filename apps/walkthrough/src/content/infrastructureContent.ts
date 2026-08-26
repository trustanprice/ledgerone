// This used to be a hypothetical design writeup ("what productionizing
// this would look like"). It stopped being hypothetical: infra/ and
// .github/workflows/{deploy-infra,dbt-build}.yml in this repo are real,
// deployed, and were tested end to end via actual GitHub Actions runs
// (see the "What actually happened" section below). This content
// describes that real deployment, not a plan for one. It's still static
// content though — this site itself makes no live AWS calls; see
// AGENTS.md for why.

export const ARCHITECTURE_DIAGRAM = [
  {
    label: 'Ingestion',
    nodes: [
      { label: 'GitHub Actions runner', kicker: 'generates synthetic data, same scripts as `make generate`' },
      { label: 'S3 raw zone', kicker: 'ledgerone-raw-{env}, encrypted, 90-day lifecycle expiry', accent: true },
    ],
  },
  {
    label: 'Transform',
    nodes: [
      { label: 'MotherDuck', kicker: 'cloud-hosted DuckDB — ledgerone (prod), ledgerone_dev (dev)', accent: true },
      { label: 'Same dbt project', kicker: 'staging → marts, zero model changes for cloud' },
    ],
  },
  {
    label: 'Orchestration',
    nodes: [
      { label: 'dbt-build.yml', kicker: 'workflow_dispatch only — no cron yet, deliberately' },
      { label: 'Scheduled run', kicker: 'next step once dispatch history builds confidence' },
    ],
  },
  {
    label: 'CI/CD & auth',
    nodes: [
      { label: 'GitHub OIDC → IAM role', kicker: 'per environment, no long-lived AWS keys', accent: true },
      { label: 'deploy-infra.yml', kicker: 'preview-only: creates/describes changesets, can’t execute — see below' },
    ],
  },
  {
    label: 'Consumption',
    nodes: [
      { label: 'MotherDuck SQL access' },
      { label: 'This site’s static data export' },
    ],
  },
]

export interface InfraSection {
  title: string
  body: string
}

export const CICD_WRITEUP: InfraSection[] = [
  {
    title: 'Why MotherDuck, not a full warehouse migration',
    body: 'The whole point of this project is that dbt-duckdb works locally with zero infra. MotherDuck (a cloud-hosted DuckDB) is the smallest real step from "local file" to "shared, always-on warehouse" — same SQL, same dbt project, same DuckDB-specific features (like the recursive CTE the CECL model uses) that a Snowflake/Redshift migration would need to be rewritten for. dbt/profiles.yml has three targets now: dev (local file, unchanged), ci_dev (MotherDuck, dev database), and prod (MotherDuck, prod database) — deliberately separate MotherDuck databases, not just separate tokens against the same one.',
  },
  {
    title: 'Auth: OIDC over long-lived keys',
    body: 'Every AWS interaction from CI (writing to the S3 raw zone) goes through GitHub Actions’ OIDC provider assuming a short-lived, tightly-scoped IAM role per environment (dev/prod) — no AWS access keys ever stored as a repo secret. Each role’s trust policy restricts which repo AND which GitHub Environment can assume it (the `sub` condition matches `repo:<org>/<repo>:environment:<dev|prod>`), so a workflow run against `dev` can’t assume the `prod` role even if it wanted to.',
  },
  {
    title: 'Provisioning: CloudFormation, bootstrapped by hand',
    body: 'The S3 bucket (lifecycle policy included) and the per-environment IAM role are defined in infra/ledgerone-stack.yml, one stack per environment. The very first deploy of each stack can’t go through CI — a workflow can’t assume a role that doesn’t exist yet to create that same role — so both were bootstrapped from an authenticated admin CLI session using `--no-execute-changeset`, a human review of the exact diff, then a manual `execute-change-set`. deploy-infra.yml now runs that same preview step in CI — the deploy role can create and describe a changeset (scoped to just this one stack’s ARN) and the job prints the resource diff to its summary — but it still has no `ExecuteChangeSet` and no `iam:*` action of any kind. Letting a CI role actually apply a change to the stack that defines its own IAM trust policy is a real privilege-escalation tradeoff, left as a deliberate, undecided question rather than defaulted into; preview-only was the narrower first step.',
  },
  {
    title: 'What actually happened, first time through',
    body: 'Two real bugs, caught from actual failing runs, not review: dbt/profiles.yml’s pre-existing local dev target and the GitHub Environment named "dev" collided under one dbt --target name, silently pointing CI at a local file that doesn’t exist on a fresh runner — fixed with a separate ci_dev target. Second, ci_dev’s MotherDuck database didn’t exist yet (the account had a token but nothing named ledgerone_dev attached) — a one-time manual CREATE DATABASE in MotherDuck’s SQL notebook UI, the same category of "the account exists but the resource inside it doesn’t yet" gap that also blocked Cost Explorer below.',
  },
  {
    title: 'What still isn’t done',
    body: 'No schedule/cron trigger on dbt-build.yml yet — both environments passed a manual dispatch (136/136 dbt tests, both dev and prod, verified from real run logs and a direct S3 bucket listing), but "it worked once, dispatched by hand" and "trust it unattended on a schedule" are different bars. deploy-infra.yml being unable to actually apply a change is also deliberate, not an oversight — see above.',
  },
  {
    title: 'Planned: this becomes a live, scheduled deployment',
    body: 'Manual dispatch is the current state, not the destination — the intent is a `schedule:` cron trigger on dbt-build.yml running unattended against prod. Held back deliberately, not by oversight: a handful of clean manual runs is evidence the mechanics work, not evidence it is safe to run unwatched. dbt-build.yml now writes a clear failure banner to the job summary, but that’s pull-based visibility (someone has to open the Actions tab), not push-based alerting — closing a real gap for a hand-dispatched run, a much smaller one for an unattended cron run where no one’s watching by default. Before that trigger gets added: more dispatch history across both environments, real push-based alerting (Slack/email/CloudWatch — not yet chosen), and a resolved answer on whether deploy-infra.yml’s deploy role gets ExecuteChangeSet/iam:* to actually apply what it can now only preview — an unattended pipeline that cannot also fix its own infrastructure drift is a narrower kind of "automated" than the goal.',
  },
]
