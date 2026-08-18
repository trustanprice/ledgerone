import { GlassPanel } from '../components/GlassPanel'
import { SqlBlock } from '../components/SqlBlock'
import { FlowDiagram } from '../components/FlowDiagram'
import { DataTable } from '../components/DataTable'
import { DATA_DICTIONARY, SQL_SNIPPETS, TEST_COVERAGE_SUMMARY, type ModelDictEntry } from '../content/dataEngineeringContent'

const LEDGER_LINEAGE = [
  { label: 'Raw sources', nodes: [{ label: 'raw.events' }, { label: 'raw.users' }, { label: 'raw.accounts' }] },
  {
    label: 'Staging',
    nodes: [{ label: 'stg_events' }, { label: 'stg_users' }, { label: 'stg_accounts' }, { label: 'stg_ledger_entries' }],
  },
  {
    label: 'Marts: core (star schema)',
    nodes: [{ label: 'dim_date' }, { label: 'dim_customer' }, { label: 'dim_account' }, { label: 'fact_transactions', accent: true }],
  },
  {
    label: 'Marts: reporting',
    nodes: [{ label: 'monthly_revenue_and_refund_rate' }, { label: 'net_payout_by_period' }, { label: '+ 3 more' }],
  },
]

const CREDIT_RISK_LINEAGE = [
  { label: 'Raw sources', nodes: [{ label: 'credit_risk.loan_originations' }, { label: 'credit_risk.loan_performance' }] },
  {
    label: 'Staging',
    nodes: [{ label: 'credit_risk__stg_loan_originations' }, { label: 'credit_risk__stg_loan_performance' }],
  },
  {
    label: 'Marts',
    nodes: [
      { label: 'credit_risk__vintage_curves' },
      { label: 'credit_risk__roll_rate_transition_matrix', accent: true },
      { label: 'credit_risk__cecl_reserve_estimate', accent: true },
      { label: 'credit_risk__portfolio_summary_by_score_band' },
    ],
  },
]

const dictColumns = [
  { key: 'model', label: 'Model', render: (r: ModelDictEntry) => <code style={{ color: 'var(--accent-strong)' }}>{r.model}</code> },
  { key: 'layer', label: 'Layer', render: (r: ModelDictEntry) => <span className="pill pill-neutral">{r.layer}</span> },
  { key: 'grain', label: 'Grain', render: (r: ModelDictEntry) => r.grain },
  { key: 'keys', label: 'Keys', render: (r: ModelDictEntry) => r.keys },
  { key: 'tests', label: 'Tests', render: (r: ModelDictEntry) => <span style={{ color: 'var(--ink-secondary)' }}>{r.tests}</span> },
]

export function DataEngineeringTab() {
  const totalTests = TEST_COVERAGE_SUMMARY.reduce((s, t) => s + t.count, 0)

  return (
    <div className="tab-page">
      <div className="tab-header">
        <span className="eyebrow">Reference</span>
        <h1>Database & Data Engineering</h1>
        <p className="lede">
          How this is actually built. Two dbt domains sharing one DuckDB warehouse and one project — the e-commerce
          ledger (Phase 1-2) and the credit-risk practice module, kept structurally and visually separate, with zero
          shared joins between them.
        </p>
      </div>

      <section className="ref-section">
        <h2>Lineage</h2>
        <p className="section-desc">
          Raw Parquet → staging (typed, 1:1) → marts. Both domains follow the same convention; only the credit-risk
          domain is covered elsewhere on this site, so the ledger domain is shown for architectural context only.
        </p>
        <div className="ref-grid">
          <GlassPanel className="ref-panel">
            <h3>E-commerce ledger domain</h3>
            <FlowDiagram columns={LEDGER_LINEAGE} />
          </GlassPanel>
          <GlassPanel className="ref-panel">
            <h3>Credit risk domain</h3>
            <FlowDiagram columns={CREDIT_RISK_LINEAGE} />
          </GlassPanel>
        </div>
      </section>

      <section className="ref-section">
        <h2>The SQL worth reading</h2>
        <p className="section-desc">
          The three models with the most interesting logic in the repo — copied from the actual dbt files, trimmed
          where noted. Full source: <code>dbt/models/credit_risk/</code>.
        </p>
        <div className="ref-grid">
          {SQL_SNIPPETS.map((snippet) => (
            <GlassPanel key={snippet.id} className="ref-panel">
              <h3>{snippet.title}</h3>
              <p style={{ color: 'var(--ink-secondary)', fontSize: '0.85rem', marginTop: 0, marginBottom: '0.9rem' }}>
                {snippet.description}
              </p>
              <SqlBlock title={snippet.title} source={snippet.source} sql={snippet.sql} />
            </GlassPanel>
          ))}
        </div>
      </section>

      <section className="ref-section">
        <h2>Data dictionary</h2>
        <p className="section-desc">Grain, keys, and test coverage for every model in the credit-risk domain.</p>
        <GlassPanel className="ref-panel">
          <DataTable columns={dictColumns} rows={DATA_DICTIONARY} getRowKey={(r) => r.model} />
        </GlassPanel>
      </section>

      <section className="ref-section">
        <h2>Test coverage</h2>
        <p className="section-desc">
          {totalTests} dbt tests across the credit-risk domain — <code>dbt build --select credit_risk</code> fails
          loudly if any of them break.
        </p>
        <div className="stat-grid cols-4">
          {TEST_COVERAGE_SUMMARY.map((t) => (
            <div className="stat-tile" key={t.type}>
              <div className="stat-label">{t.type}</div>
              <div className="stat-value">{t.count}</div>
              <div className="stat-sub">{t.note}</div>
            </div>
          ))}
        </div>
      </section>
    </div>
  )
}
