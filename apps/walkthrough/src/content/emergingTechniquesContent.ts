// Placeholder content for a tab that doesn't have real results yet — but
// a detailed placeholder, not a bare TODO. Each entry describes how a
// credit-risk analyst at a card issuer (Synchrony-scale) would actually
// approach this technique in practice: the business reason it exists,
// the real workflow (data sourcing through validation), and what the
// deliverable looks like. The point is to give the actual analysis work
// a scaffold to land in, not to do the analysis here — every `workflow`
// step is something to go DO, not something already done. See
// README.md's "Planned: a sixth tab" section for the full writeup this
// content is a placeholder for.

export interface PlannedTechnique {
  id: string
  title: string
  status: 'not started' | 'in progress'
  summary: string
  why: string
  businessContext: string
  workflow: string[]
  deliverable: string
  dataAndTools: string
  wherePlugsIn: string
}

export const PLANNED_TECHNIQUES: PlannedTechnique[] = [
  {
    id: 'regime-switching',
    title: 'Regime-switching / macro-conditioned transition matrix',
    status: 'not started',
    summary:
      'One static roll-rate matrix rolled forward forever vs. a matrix that switches with a macro state — fit separately, not blended, and compared against the same holdout.',
    why:
      'Not a hypothetical gap: the Freddie Mac backtest already found the blended matrix isn’t time-stable (30-59 DPD cure rate dropped from 53.4% to 40.9% between training and holdout — see the Forecasting tab). This is the direct next step on a finding that already exists.',
    businessContext:
      'CECL and IFRS9 both require reserves to reflect "reasonable and supportable forecasts," not just historical averages — a regulatory expectation, not a modeling preference. A single blended transition matrix implicitly assumes the future behaves like the average of the past, which is exactly the assumption the Freddie Mac backtest already showed breaking down. Model Risk Management (MRM) teams specifically probe for this: "what happens to your reserve under a recession scenario?" is a standard challenge question, and "the matrix doesn’t change" is not an acceptable answer.',
    workflow: [
      'Source macro scenario paths — in practice, often licensed from Moody’s Analytics or built by an internal economics team (base / adverse / severely adverse, matching CCAR/DFAST conventions); for this project, FRED series (unemployment rate, HPI) are the realistic public substitute.',
      'Choose a regime-definition approach: either (a) threshold-based — split historical months into "high unemployment" / "low unemployment" regimes and fit a separate transition matrix per regime, or (b) a proper Markov-switching model with latent regimes estimated from the data (more statistically rigorous, more setup).',
      'Fit the transition matrix per regime using the same LAG-based approach already in this project (credit_risk__roll_rate_transition_matrix / the notebook 03 methodology) — same technique, split by regime instead of blended.',
      'Roll forward using the scenario’s regime path (e.g. "adverse scenario = regime B for 12 months, then regime A") instead of one static matrix, the same absorbing-state roll-forward mechanic already built for CECL.',
      'Backtest against the same Freddie Mac holdout already used in notebook 03 — does the regime-aware forecast actually beat the static-matrix MAE (0.19pp) that’s already the benchmark to beat?',
      'Write up for an MRM-style audience: does conditioning on regime measurably improve the forecast, and is the improvement worth the added model complexity? A regime model that doesn’t beat the simple baseline is itself a valid, reportable finding.',
    ],
    deliverable:
      'A side-by-side comparison table (static matrix vs. regime-aware matrix) on the exact same holdout metric already established — MAE/MAPE, plus the calibration-table view — framed as a challenge to the existing backtest’s own result, not a new unrelated analysis.',
    dataAndTools:
      'Python (pandas/numpy) for prototyping, same as notebook 03. FRED for macro series (already referenced qualitatively in the backtest’s sanity check — this reuses that same public data source for real, not just as a benchmark).',
    wherePlugsIn:
      'notebooks/04_regime_switching_backtest.ipynb, following notebook 03’s exact teaching-style structure (explain the why of each step). Export + chart component only get built once real results exist.',
  },
  {
    id: 'survival-analysis',
    title: 'Survival analysis (Kaplan-Meier, Cox proportional hazards)',
    status: 'not started',
    summary:
      '“Time to first severe delinquency” as a hazard function instead of a cumulative-rate curve — a genuinely different technique family from the vintage-curve approach used everywhere else in this project.',
    why:
      'Same underlying loan-performance panel already ingested (synthetic + the real Freddie Mac data) — a different lens on data this project already has, not a new dataset to source.',
    businessContext:
      'Vintage curves (what this project already has) answer "what fraction of a cohort has reached 90+ by month X" — a cumulative, cohort-level question. Survival analysis answers a complementary, account-level question: "given an account is still current, what’s its instantaneous risk of going severely delinquent right now, and how does that risk change with tenure and covariates?" That framing is standard in lifetime-PD modeling for CECL, and is also how attrition/prepayment risk gets modeled at most issuers — the same machinery, different event definition.',
    workflow: [
      'Define the event precisely: first month an account reaches 90+ DPD (matching the SEVERE bucket already used in notebook 03). Define censoring precisely: an account still current/never-severe at the end of the observation window is right-censored, not "surviving forever."',
      'Fit Kaplan-Meier curves stratified by a segment that should matter — score band is the obvious first cut, since it already exists in credit_risk__portfolio_summary_by_score_band. Eyeball whether the curves separate the way underwriting would expect.',
      'Fit a Cox proportional hazards model with covariates (score band, origination balance, macro state at origination) to get hazard ratios — "how much does being Subprime multiply your instantaneous risk vs. Prime, holding everything else constant."',
      'Test the proportional-hazards assumption (e.g. Schoenfeld residuals) before trusting the Cox coefficients — a real methodological step, not optional, and a good one to be able to explain in an interview.',
      'Convert the fitted hazard/survival function into a cumulative "reached severe by month X" curve and plot it directly against the existing vintage curve for the same cohort — same question, two different techniques, does it agree?',
    ],
    deliverable:
      'Kaplan-Meier curves by score band, Cox hazard ratios with a plain-language read of what each covariate’s coefficient means, and one chart overlaying the survival-derived cumulative curve against the existing vintage curve for the same real cohort.',
    dataAndTools:
      'Python’s lifelines library is the standard choice (Kaplan-Meier, CoxPHFitter, proportional-hazards test all built in) — same loan-performance panel already in data/external/freddie_mac/ and the synthetic credit_risk data.',
    wherePlugsIn:
      'notebooks/05_survival_analysis.ipynb. A new SurvivalCurveChart component would be needed on the site side — the existing VintageCurvesChart doesn’t have the right shape for a hazard/survival function.',
  },
  {
    id: 'monte-carlo',
    title: 'Monte Carlo loss-distribution simulation',
    status: 'not started',
    summary:
      'The existing CECL reserve estimate is a single point forecast. Simulating many roll-forward paths (resampling transition draws instead of always taking the expected value) turns that into a distribution — a confidence band around the reserve number, not just the number.',
    why:
      'Directly extends credit_risk__cecl_reserve_estimate’s existing roll-forward mechanic — same model, run stochastically instead of deterministically.',
    businessContext:
      'credit_risk__cecl_reserve_estimate reports one number: expected loss. Finance, capital planning, and stress-testing all want more than that — a distribution tells you not just "what do we expect to lose" but "how bad could it plausibly get," which is what economic-capital and CCAR-style severely-adverse-scenario reporting actually need. This is the standard way a deterministic roll-forward model gets extended into something a capital-planning team can use without rebuilding it from scratch.',
    workflow: [
      'Start from the existing deterministic roll-forward in credit_risk__cecl_reserve_estimate.sql — understand exactly which step currently takes an expected value (a fixed transition rate) that a simulation would instead sample from.',
      'Pick an uncertainty model for each transition-matrix cell: the simplest defensible choice is a Dirichlet distribution over each from-bucket’s row (its parameters set by the observed transition counts — more observations, tighter distribution, less simulated noise), or bootstrap-resample the underlying account-month transitions directly.',
      'Simulate N roll-forward paths (e.g. 1,000–10,000) per cohort, drawing a fresh matrix from the uncertainty model each path, rolling forward the same absorbing-state mechanic already built.',
      'Aggregate across paths into a distribution of total expected loss / reserve rate — report the median, and meaningful percentiles (e.g. P95, P99) as a range alongside the existing deterministic point estimate, not replacing it.',
      'Sanity-check: does the deterministic point estimate fall near the simulated median? If not, that’s itself worth investigating — a real, explainable finding, not a bug to hide.',
    ],
    deliverable:
      'A loss-distribution histogram and a percentile table (P50/P95/P99) for the reserve estimate, compared directly against the single deterministic number already shown on the Forecasting tab today.',
    dataAndTools:
      'Python (numpy for the Dirichlet/bootstrap sampling, pandas for aggregation) — reuses the exact SQL logic already in credit_risk__cecl_reserve_estimate.sql as the deterministic baseline to compare against.',
    wherePlugsIn:
      'notebooks/06_monte_carlo_reserve.ipynb. Site side would need a distribution-shaped chart (a histogram or a fan chart) — ReserveForecastChart’s current area-chart shape doesn’t show uncertainty, so this is a case for a new component, not a reused one.',
  },
  {
    id: 'sas-exercise',
    title: 'SAS exercise: the roll-rate matrix, reimplemented',
    status: 'not started',
    summary:
      'One already-validated analysis — the roll-rate transition matrix, picked because its correct answer is already known from the dbt/SQL version — rebuilt in SAS. A single, honest artifact, not a claim that this project runs on SAS.',
    why:
      'Named explicitly as a required skill ahead of this role, and the one language in that list this project has zero exposure to (Python and SQL are everywhere else in this repo).',
    businessContext:
      'SAS is still the production language at a lot of legacy card issuers’ risk/regulatory-reporting teams — PROC LOGISTIC for scorecards, PROC LIFETEST/PROC PHREG for survival work, PROC SQL and DATA-step macro logic for the batch pipelines that actually generate regulatory numbers. Nobody expects a portfolio project to run on SAS end to end; what’s worth demonstrating is that the same technique already proven correct in SQL/dbt translates cleanly to SAS syntax and produces the identical answer.',
    workflow: [
      'Get the same loan-performance panel (the credit_risk synthetic data, or the Freddie Mac parquet) into a SAS dataset — PROC IMPORT from the parquet/CSV, or a plain DATA step if reading from CSV.',
      'Rebuild the LAG-based from-bucket/to-bucket transition logic — the same technique credit_risk__roll_rate_transition_matrix.sql already uses — via a DATA step with a BY-group LAG(), mirroring the SQL window function exactly.',
      'Aggregate into the transition matrix with PROC FREQ or PROC TABULATE (row-normalized counts, matching how the dbt model presents cell percentages).',
      'Diff the SAS output against the dbt/SQL model’s actual output, cell by cell — this is the whole point of the exercise: prove the two independently-built versions agree, not just that the SAS code runs.',
      'Write up anything about the SAS version that was awkward or required a different approach than the SQL version — that difference is itself the useful, talkable-about part of the exercise.',
    ],
    deliverable:
      'A SAS script producing the transition matrix, plus a written cell-by-cell comparison confirming numeric parity with credit_risk__roll_rate_transition_matrix — a concrete, verifiable "I can do this in SAS" artifact, not a claim.',
    dataAndTools:
      'SAS (SAS OnDemand for Academics is the free option if a licensed seat isn’t available) reading the same credit_risk synthetic parquet/CSV export already produced by this repo.',
    wherePlugsIn:
      'A standalone .sas script, not a notebook — SAS output doesn’t belong in a Python/dbt pipeline. Referenced from this tab as a downloadable/linked artifact once it exists, not embedded in the site’s data flow.',
  },
]
