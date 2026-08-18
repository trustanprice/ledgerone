// Static architecture documentation — not a description of anything
// actually running. See the disclaimer rendered at the top of
// InfrastructureTab. No AWS account, credentials, or IaC execution is
// implied or required to view this content; it's a diagram plus copy.

export const ARCHITECTURE_DIAGRAM = [
  {
    label: 'Ingestion',
    nodes: [
      { label: 'Source systems', kicker: 'origination / servicing' },
      { label: 'S3 raw zone', kicker: 'Parquet, partitioned by domain + date', accent: true },
    ],
  },
  {
    label: 'Transform',
    nodes: [
      { label: 'MotherDuck', kicker: 'cloud-hosted DuckDB', accent: true },
      { label: 'Same dbt project', kicker: 'staging → marts, unchanged' },
    ],
  },
  {
    label: 'Orchestration',
    nodes: [
      { label: 'Scheduled GitHub Actions', kicker: 'cron trigger, replaces `make build`' },
      { label: 'Airflow / Dagster', kicker: 'if SLA needs cross-system dependencies' },
    ],
  },
  {
    label: 'CI/CD & auth',
    nodes: [
      { label: 'Reusable GH Actions workflow', kicker: 'PR checks: dbt build --select state:modified+' },
      { label: 'OIDC → IAM role', kicker: 'no long-lived AWS keys', accent: true },
    ],
  },
  {
    label: 'Consumption',
    nodes: [
      { label: 'Analyst SQL access' },
      { label: 'BI tool / this site’s data export' },
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
    body: 'The whole point of this project is that dbt-duckdb works locally with zero infra. MotherDuck (a cloud-hosted DuckDB) is the smallest real step from "local file" to "shared, always-on warehouse" — same SQL, same dbt project, same DuckDB-specific features (like the recursive CTE the CECL model uses) that a Snowflake/Redshift migration would need to be rewritten for. A bigger warehouse becomes the right call once concurrent writers or dataset size actually demand it, not before.',
  },
  {
    title: 'Auth: OIDC over long-lived keys',
    body: 'Every AWS interaction (writing to the S3 raw zone, reading Secrets Manager for the MotherDuck token) goes through GitHub Actions’ OIDC provider assuming a short-lived, tightly-scoped IAM role per environment (dev/prod) — no AWS access keys stored as repo secrets. The trust policy on each role restricts which repo, branch, and workflow can assume it, so a compromised workflow in one repo can’t pivot into another environment’s role.',
  },
  {
    title: 'Reusable workflows, not copy-pasted YAML',
    body: 'PR checks (lint, dbt build --select state:modified+ against a CI-scoped MotherDuck database, dbt test) live in one reusable workflow called from every environment’s deploy workflow with different inputs — the same pattern this repo’s own GitHub Actions Pages deploy uses, just extended to a scheduled production run instead of a static-site build.',
  },
  {
    title: 'Provisioning: CloudFormation',
    body: 'The S3 bucket, its lifecycle policy (raw data expires/archives on a schedule), and the per-environment IAM roles would be defined in CloudFormation, versioned in the repo, deployed via the same reusable-workflow pattern above — so standing up a new environment is a stack deploy, not a set of manual console clicks.',
  },
  {
    title: 'What actually changes vs. today',
    body: 'Almost nothing in dbt/ — the models, tests, and docs are identical. `make build` becomes a scheduled GitHub Actions run pointed at a MotherDuck connection string instead of a local file path; the export script gains a step that pushes fresh JSON somewhere the deployed site can read it (or the site’s own deploy is triggered by the same pipeline). The local-dev path (this repo, as-is) keeps working unchanged for development and interview demos.',
  },
]
