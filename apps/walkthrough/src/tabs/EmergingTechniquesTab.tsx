import { GlassPanel } from '../components/GlassPanel'
import { PLANNED_TECHNIQUES } from '../content/emergingTechniquesContent'

/**
 * Scaffolding for a tab that doesn't have real results yet — see
 * README.md's "Planned: a sixth tab" section. Each section names the
 * real workflow a credit-risk analyst would follow for that technique
 * (business context, steps, deliverable) so the actual analysis has a
 * scaffold to land in — the `workflow` steps are things to go DO, this
 * page doesn't do them. Same honesty standard as every other placeholder
 * this site has shipped: the Forecasting tab's backtest section said
 * "not yet available" until real numbers existed; this is that same
 * pattern, applied before the notebook work has even started.
 */
export function EmergingTechniquesTab() {
  return (
    <div className="tab-page">
      <div className="tab-header">
        <span className="eyebrow">In progress</span>
        <h1>Emerging Techniques</h1>
        <p className="lede">
          Stochastic-process methods beyond the roll-rate/vintage-curve approach used everywhere else in this
          project, plus a SAS exercise — driven by specific, real pre-start guidance ahead of a credit-risk /
          loss-forecasting analyst role. Each section below is a scaffold for real analysis work, not a result —
          the business context and workflow describe how this would actually get done at a card issuer, written
          before doing it so the scope is deliberate.
        </p>
      </div>

      <div className="disclaimer-banner">
        <span aria-hidden="true">ℹ</span>
        <span>
          <strong>Every section below is scaffolding, not content.</strong> No notebook, export script, or chart
          exists for any of these four yet. The workflow steps are a plan to execute, not a summary of what's been
          done.
        </span>
      </div>

      {PLANNED_TECHNIQUES.map((tech, i) => (
        <section className="ref-section" key={tech.id} id={tech.id}>
          <span className="eyebrow" style={{ fontSize: '0.7rem' }}>
            {i + 1} of {PLANNED_TECHNIQUES.length} · {tech.status}
          </span>
          <h2 style={{ marginTop: '0.35rem' }}>{tech.title}</h2>
          <p className="section-desc">{tech.summary}</p>

          <div className="ref-grid">
            <GlassPanel className="ref-panel">
              <h3>Why this is on the list</h3>
              <p style={{ color: 'var(--ink-secondary)', fontSize: '0.88rem', lineHeight: 1.6, margin: 0 }}>{tech.why}</p>
            </GlassPanel>
            <GlassPanel className="ref-panel">
              <h3>Business context</h3>
              <p style={{ color: 'var(--ink-secondary)', fontSize: '0.88rem', lineHeight: 1.6, margin: 0 }}>
                {tech.businessContext}
              </p>
            </GlassPanel>
          </div>

          <GlassPanel className="ref-panel" style={{ marginTop: '1.25rem' }}>
            <h3>How this would actually get done</h3>
            <ol style={{ color: 'var(--ink-secondary)', fontSize: '0.88rem', lineHeight: 1.7, paddingLeft: '1.2rem', margin: 0 }}>
              {tech.workflow.map((step, j) => (
                <li key={j} style={{ marginBottom: j === tech.workflow.length - 1 ? 0 : '0.6rem' }}>
                  {step}
                </li>
              ))}
            </ol>
          </GlassPanel>

          <div className="ref-grid" style={{ marginTop: '1.25rem' }}>
            <GlassPanel className="ref-panel">
              <h3>Deliverable</h3>
              <p style={{ color: 'var(--ink-secondary)', fontSize: '0.88rem', lineHeight: 1.6, margin: 0 }}>
                {tech.deliverable}
              </p>
            </GlassPanel>
            <GlassPanel className="ref-panel">
              <h3>Data &amp; tools</h3>
              <p style={{ color: 'var(--ink-secondary)', fontSize: '0.88rem', lineHeight: 1.6, margin: 0 }}>
                {tech.dataAndTools}
              </p>
            </GlassPanel>
          </div>

          <p className="chart-footnote" style={{ marginTop: '1rem' }}>
            Where this plugs in: {tech.wherePlugsIn}
          </p>
        </section>
      ))}
    </div>
  )
}
