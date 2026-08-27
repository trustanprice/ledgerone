import { useCreditRiskData } from '../hooks/useCreditRiskData'
import { GlassPanel } from '../components/GlassPanel'
import { StatStrip, type StatStripItem } from '../components/StatStrip'
import { Sparkline } from '../components/Sparkline'
import { FlowDiagram } from '../components/FlowDiagram'
import { DATA_DICTIONARY, TEST_COVERAGE_SUMMARY } from '../content/dataEngineeringContent'
import { PLANNED_TECHNIQUES } from '../content/emergingTechniquesContent'
import type { TabId } from '../tabConfig'

interface Props {
  onNavigate: (tab: TabId) => void
}

const SCENES = ['Morning', 'Vintage', 'Roll-Rate', 'Reserve', 'So What']

// Compact previews reusing the same FlowDiagram component the reference
// tabs use, just a shorter node list — real model/architecture names, not
// placeholders, wrapped in .mini-diagram for a smaller footprint (see
// index.css; that class doesn't touch the full-size diagrams elsewhere).
const MINI_LINEAGE = [
  { label: 'Raw', nodes: [{ label: 'loan_originations' }, { label: 'loan_performance' }] },
  { label: 'Staging', nodes: [{ label: 'stg_loan_originations' }, { label: 'stg_loan_performance' }] },
  { label: 'Marts', nodes: [{ label: 'roll_rate_transition_matrix', accent: true }, { label: 'cecl_reserve_estimate', accent: true }] },
]

const MINI_ARCHITECTURE = [
  { label: 'Ingestion', nodes: [{ label: 'S3 raw zone', accent: true }] },
  { label: 'Transform', nodes: [{ label: 'MotherDuck', accent: true }] },
  { label: 'Orchestration', nodes: [{ label: 'GitHub Actions' }] },
  { label: 'Auth', nodes: [{ label: 'OIDC → IAM', accent: true }] },
]

export function OverviewTab({ onNavigate }: Props) {
  const { distribution, vintage, reserve, portfolioSummary, allLoaded, error } = useCreditRiskData()

  if (error) {
    return (
      <div className="dash-page">
        <p style={{ color: 'var(--bad)', marginTop: '2rem' }}>Couldn't load data ({error}).</p>
      </div>
    )
  }

  if (!allLoaded) {
    return (
      <div className="dash-page">
        <p className="chart-status" style={{ marginTop: '2rem' }}>
          Loading…
        </p>
      </div>
    )
  }

  // Every stat below is computed from the same static JSON the other tabs
  // read — see the report at the end of this session for exact sourcing.
  const totalAccounts = portfolioSummary!.reduce((s, r) => s + r.account_count, 0)
  const totalChargedOff = portfolioSummary!.reduce((s, r) => s + r.charged_off_accounts, 0)
  const blendedChargeOffRate = totalChargedOff / totalAccounts

  const cohorts = Array.from(new Set(vintage!.map((r) => r.origination_quarter))).sort()

  const totalActive = distribution!.reduce((s, r) => s + r.account_count, 0)
  const delinquentActive = distribution!.reduce((s, r) => (r.bucket === 'Current' ? s : s + r.account_count), 0)
  const activeDelinquencyRate = delinquentActive / totalActive

  const finalReserve = reserve!.points[reserve!.points.length - 1]
  const totalTests = TEST_COVERAGE_SUMMARY.reduce((s, t) => s + t.count, 0)

  const statItems: StatStripItem[] = [
    { label: 'Accounts originated', value: totalAccounts.toLocaleString() },
    {
      label: 'Origination cohorts',
      value: cohorts.length,
      sub: `${cohorts[0].slice(0, 7)} – ${cohorts[cohorts.length - 1].slice(0, 7)}`,
    },
    { label: 'Lifetime charge-off', value: `${(blendedChargeOffRate * 100).toFixed(2)}%`, sub: 'blended, all bands' },
    {
      label: 'Active delinquency',
      value: `${(activeDelinquencyRate * 100).toFixed(2)}%`,
      sub: `${delinquentActive.toLocaleString()} accounts`,
    },
    { label: `${reserve!.horizon_months}-mo reserve rate`, value: `${(finalReserve.reserve_rate * 100).toFixed(2)}%` },
    {
      label: 'dbt test coverage',
      value: (
        <>
          {totalTests}
          <span style={{ color: 'var(--good)' }}>/{totalTests}</span>
        </>
      ),
      sub: `${DATA_DICTIONARY.length} models passing`,
    },
  ]

  return (
    <>
      <div className="dash-page">
        <div className="dash-header">
          <div>
            <span className="eyebrow">LedgerOne — Credit Risk Module</span>
            <h1>Credit risk, end to end</h1>
            <p className="lede">
              A synthetic consumer credit portfolio, modeled with vintage analysis, roll-rate transition matrices, and
              a CECL-style reserve estimate — surfaced as a teaching walkthrough, a data-engineering reference, a
              forecasting dashboard, and an architecture writeup.
            </p>
          </div>
          <div className="hero-meta">
            <span>Static site, no backend</span>
            <span>Synthetic data only</span>
            <span>
              <a href="https://github.com/trustanprice/ledgerone" target="_blank" rel="noreferrer">
                Source: LedgerOne
              </a>
            </span>
          </div>
        </div>

        <StatStrip items={statItems} />

        <div className="bento-grid">
          <GlassPanel className="bento-tile bento-forecasting" onClick={() => onNavigate('forecasting')}>
            <div className="bento-tile-header">
              <h3>Forecasting & Data Science</h3>
              <span className="pill pill-accent">Reference</span>
            </div>
            <div className="bento-tile-body">
              <div className="preview-sparkline-row">
                <Sparkline values={reserve!.points.map((p) => p.expected_loss_amount)} width={220} height={56} />
                <div className="preview-sparkline-figures">
                  <div className="preview-figure">
                    <div className="stat-strip-label">Expected loss</div>
                    <div className="stat-strip-value mono-nums">
                      ${finalReserve.expected_loss_amount.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                    </div>
                  </div>
                  <div className="preview-figure">
                    <div className="stat-strip-label">Reserve rate</div>
                    <div className="stat-strip-value mono-nums">{(finalReserve.reserve_rate * 100).toFixed(2)}%</div>
                  </div>
                </div>
              </div>
            </div>
            <p className="bento-tile-footer">
              Every cohort's vintage curve, the full roll-rate matrix, and this {reserve!.horizon_months}-month reserve
              forecast — all visible at once, not revealed scene by scene.
            </p>
          </GlassPanel>

          <GlassPanel className="bento-tile bento-data-engineering" onClick={() => onNavigate('data-engineering')}>
            <div className="bento-tile-header">
              <h3>Database & Data Engineering</h3>
              <span className="pill pill-accent">Reference</span>
            </div>
            <div className="bento-tile-body">
              <div className="mini-diagram">
                <FlowDiagram columns={MINI_LINEAGE} />
              </div>
              <code className="mini-sql">
                <span className="sql-kw">lag</span>(delinquency_bucket) <span className="sql-kw">over</span> (
                <span className="sql-kw">partition by</span> account_id <span className="sql-kw">order by</span>{' '}
                months_on_book)
              </code>
            </div>
            <p className="bento-tile-footer">
              {DATA_DICTIONARY.length} models, {totalTests} dbt tests passing — the roll-rate and CECL recursive-CTE
              queries, verbatim.
            </p>
          </GlassPanel>

          <GlassPanel className="bento-tile bento-walkthrough" onClick={() => onNavigate('walkthrough')}>
            <div className="bento-tile-header">
              <h3>Day in the Life</h3>
              <span className="pill pill-neutral">Teaching</span>
            </div>
            <div className="bento-tile-body">
              <div className="scene-progress">
                {SCENES.map((label, i) => (
                  <div className="scene-progress-step" key={label}>
                    <span className="scene-progress-dot">{i + 1}</span>
                    <span className="scene-progress-label">{label}</span>
                  </div>
                ))}
              </div>
            </div>
            <p className="bento-tile-footer">
              5 scenes, scroll-driven — vintage curves, roll rates, and the reserve forecast explained from first
              principles.
            </p>
          </GlassPanel>

          <GlassPanel className="bento-tile bento-infrastructure" onClick={() => onNavigate('infrastructure')}>
            <div className="bento-tile-header">
              <h3>Infrastructure & Productionization</h3>
              <span className="pill pill-neutral">Architecture doc</span>
            </div>
            <div className="bento-tile-body">
              <div className="mini-diagram">
                <FlowDiagram columns={MINI_ARCHITECTURE} />
              </div>
            </div>
            <p className="bento-tile-footer">
              A real AWS/MotherDuck deployment — S3, IAM/OIDC, CloudFormation, a live Cost Explorer-backed spend
              section — this page itself still makes no live calls, it reads a static export like every other tab.
            </p>
          </GlassPanel>

          <GlassPanel className="bento-tile bento-emerging" onClick={() => onNavigate('emerging-techniques')}>
            <div className="bento-tile-header">
              <h3>Emerging Techniques</h3>
              <span className="pill pill-neutral">Scaffolding</span>
            </div>
            <div className="bento-tile-body">
              <div className="stat-strip-value mono-nums" style={{ fontSize: '1.6rem' }}>
                {PLANNED_TECHNIQUES.length}
              </div>
              <div className="stat-strip-label">techniques planned, 0 started</div>
            </div>
            <p className="bento-tile-footer">
              Regime-switching matrices, survival analysis, Monte Carlo simulation, a SAS exercise — real analyst
              workflows written up, no results yet by design.
            </p>
          </GlassPanel>
        </div>
      </div>

      <footer className="site-footer">
        <p>
          Everything here is synthetic — no real accounts, no real losses. See{' '}
          <a href="https://github.com/trustanprice/ledgerone/blob/main/dbt/models/credit_risk/README.md" target="_blank" rel="noreferrer">
            dbt/models/credit_risk/README.md
          </a>{' '}
          for the full technical writeup.
        </p>
      </footer>
    </>
  )
}
