import { GlassPanel } from '../components/GlassPanel'
import { PLANNED_TECHNIQUES } from '../content/emergingTechniquesContent'

/**
 * Scaffolding for a tab that doesn't have real results yet — see
 * README.md's "Planned: a sixth tab" section. Each card names what's
 * coming and why, same honesty standard as every other placeholder this
 * site has shipped (the Forecasting tab's backtest section said "not yet
 * available" until real numbers existed; this is that same pattern,
 * applied before the notebook work has even started).
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
          loss-forecasting analyst role. Nothing on this page is a result yet; each card below names exactly what's
          coming and why it's on the list, not a placeholder for a placeholder's sake.
        </p>
      </div>

      <div className="disclaimer-banner">
        <span aria-hidden="true">ℹ</span>
        <span>
          <strong>This tab is scaffolding, not content.</strong> No notebook, export script, or chart exists for any
          of the four items below yet — this page exists so the site's structure is ready before the research is
          done, not after.
        </span>
      </div>

      <section className="ref-section">
        <h2>Planned</h2>
        <p className="section-desc">
          Each one is picked to extend something this project already has real evidence for, not a generic stats
          syllabus — see the "why" line on each card.
        </p>
        <div className="ref-grid">
          {PLANNED_TECHNIQUES.map((tech) => (
            <GlassPanel className="ref-panel" key={tech.title}>
              <span className="eyebrow" style={{ fontSize: '0.7rem' }}>
                {tech.status}
              </span>
              <h3 style={{ marginTop: '0.35rem' }}>{tech.title}</h3>
              <p style={{ color: 'var(--ink-secondary)', fontSize: '0.88rem', lineHeight: 1.6, margin: '0 0 0.75rem' }}>
                {tech.summary}
              </p>
              <p style={{ color: 'var(--ink-tertiary)', fontSize: '0.82rem', lineHeight: 1.6, margin: 0 }}>
                <strong style={{ color: 'var(--ink-secondary)' }}>Why: </strong>
                {tech.why}
              </p>
            </GlassPanel>
          ))}
        </div>
      </section>
    </div>
  )
}
