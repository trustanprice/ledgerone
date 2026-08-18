import { GlassPanel } from '../components/GlassPanel'
import { FlowDiagram } from '../components/FlowDiagram'
import { ARCHITECTURE_DIAGRAM, CICD_WRITEUP } from '../content/infrastructureContent'

export function InfrastructureTab() {
  return (
    <div className="tab-page">
      <div className="tab-header">
        <span className="eyebrow">Architecture doc</span>
        <h1>Infrastructure & Productionization</h1>
        <p className="lede">
          What productionizing this pipeline would look like — a design writeup, not a description of anything
          currently running.
        </p>
      </div>

      <div className="disclaimer-banner">
        <span aria-hidden="true">⚠</span>
        <span>
          <strong>This is static content, not a live deployment.</strong> Nothing on this tab is connected to AWS or
          any cloud provider — no account, credentials, or infrastructure-as-code execution is required to view or
          build this site. It's a diagram and a design writeup, same as the rest of this static site.
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
    </div>
  )
}
