import { GlassPanel } from '../components/GlassPanel'
import { FlowDiagram } from '../components/FlowDiagram'
import { FinOpsCostChart } from '../components/charts/FinOpsCostChart'
import { useFinOpsData } from '../hooks/useFinOpsData'
import { ARCHITECTURE_DIAGRAM, CICD_WRITEUP } from '../content/infrastructureContent'

export function InfrastructureTab() {
  const { data: finops, loading: finopsLoading, notFound: finopsNotFound } = useFinOpsData()

  return (
    <div className="tab-page">
      <div className="tab-header">
        <span className="eyebrow">Architecture doc</span>
        <h1>Infrastructure & Productionization</h1>
        <p className="lede">
          This stopped being hypothetical: the S3 buckets, IAM roles, and GitHub Actions workflows below are real,
          deployed, and were exercised end to end via actual CI runs — not a design writeup for something that might
          exist someday.
        </p>
      </div>

      <div className="disclaimer-banner">
        <span aria-hidden="true">ℹ</span>
        <span>
          <strong>This page describes a real deployment, but this site itself still makes no live AWS calls.</strong>{' '}
          Nothing here is fetched from AWS at runtime — no account, credentials, or infrastructure-as-code execution
          is required to view or build this site. It's a diagram and a writeup about infrastructure that exists
          elsewhere, same static-JSON pattern as the rest of this site.
        </span>
      </div>

      <section className="ref-section">
        <h2>Target architecture</h2>
        <p className="section-desc">
          Ingestion through consumption, replacing the local <code>make all</code> pipeline with a scheduled,
          cloud-hosted equivalent.
        </p>
        <GlassPanel className="ref-panel">
          <FlowDiagram columns={ARCHITECTURE_DIAGRAM} />
        </GlassPanel>
      </section>

      <section className="ref-section">
        <h2>CI/CD approach</h2>
        <p className="section-desc">
          The reasoning behind each piece — drawing on the same GitHub Actions / OIDC / CloudFormation patterns
          already in production use elsewhere, not a generic cloud reference architecture.
        </p>
        <div className="ref-grid">
          {CICD_WRITEUP.map((section) => (
            <GlassPanel className="ref-panel" key={section.title}>
              <h3>{section.title}</h3>
              <p style={{ color: 'var(--ink-secondary)', fontSize: '0.88rem', margin: 0, lineHeight: 1.6 }}>{section.body}</p>
            </GlassPanel>
          ))}
        </div>
      </section>

      <section className="ref-section">
        <h2>Real AWS cost, last 30 days</h2>
        <p className="section-desc">
          Pulled from AWS Cost Explorer against the actual account <code>infra/ledgerone-stack.yml</code> is deployed
          into — not an estimate. Exported occasionally by hand (<code>src/export_finops_data.py</code>), not on
          every build, since it's a live billing call against real infrastructure.
        </p>
        {finopsLoading && <p className="chart-status">Loading…</p>}
        {finopsNotFound && (
          <GlassPanel className="ref-panel">
            <p style={{ color: 'var(--ink-secondary)', fontSize: '0.9rem', margin: 0 }}>
              Not available yet. This account's Cost Explorer was only just enabled — AWS can take up to 24 hours
              after enabling before any usage data actually appears, so there's nothing real to show here rather than
              a fabricated number. Once <code>src/export_finops_data.py</code> runs successfully, its output gets
              committed here the same way every other dataset on this site does.
            </p>
          </GlassPanel>
        )}
        {finops && (
          <>
            <p className="section-desc" style={{ marginTop: 0 }}>
              As of {finops.as_of}, trailing {finops.window_days} days.
            </p>
            <div className="stat-grid cols-4">
              <div className="stat-tile">
                <div className="stat-label">Total (unblended)</div>
                <div className="stat-value">${finops.total_unblended_cost.toFixed(2)}</div>
                <div className="stat-sub">last {finops.window_days} days, all services</div>
              </div>
            </div>
            {finops.by_service.length > 0 ? (
              <GlassPanel className="ref-panel" style={{ height: 300, display: 'flex', flexDirection: 'column', marginTop: '1.25rem' }}>
                <FinOpsCostChart rows={finops.by_service} />
              </GlassPanel>
            ) : (
              <p className="section-desc">
                $0.00 across every service in this window — plausible for an account this new (this project's whole
                AWS footprint is estimated at well under $1/month, see <code>infra/AGENTS.md</code>), not a sign
                something's broken.
              </p>
            )}
          </>
        )}
      </section>
    </div>
  )
}
