// Placeholder content for a tab that doesn't have real results yet.
// Each entry names what's coming and why it's on the list — not a
// generic stats syllabus, each one ties back to something this project
// already has real evidence for. See README.md's "Planned: a sixth tab"
// section for the full writeup this content is a placeholder for.

export interface PlannedTechnique {
  title: string
  status: 'not started' | 'in progress'
  summary: string
  why: string
}

export const PLANNED_TECHNIQUES: PlannedTechnique[] = [
  {
    title: 'Regime-switching / macro-conditioned transition matrix',
    status: 'not started',
    summary:
      'One static roll-rate matrix rolled forward forever vs. a matrix that switches with a macro state — fit separately, not blended, and compared against the same holdout.',
    why:
      'Not a hypothetical gap: the Freddie Mac backtest already found the blended matrix isn’t time-stable (30-59 DPD cure rate dropped from 53.4% to 40.9% between training and holdout — see the Forecasting tab). This is the direct next step on a finding that already exists.',
  },
  {
    title: 'Survival analysis (Kaplan-Meier, Cox proportional hazards)',
    status: 'not started',
    summary:
      '“Time to first severe delinquency” as a hazard function instead of a cumulative-rate curve — a genuinely different technique family from the vintage-curve approach used everywhere else in this project.',
    why:
      'Same underlying loan-performance panel already ingested (synthetic + the real Freddie Mac data) — a different lens on data this project already has, not a new dataset to source.',
  },
  {
    title: 'Monte Carlo loss-distribution simulation',
    status: 'not started',
    summary:
      'The existing CECL reserve estimate is a single point forecast. Simulating many roll-forward paths (resampling transition draws instead of always taking the expected value) turns that into a distribution — a confidence band around the reserve number, not just the number.',
    why:
      'Directly extends credit_risk__cecl_reserve_estimate’s existing roll-forward mechanic — same model, run stochastically instead of deterministically.',
  },
  {
    title: 'SAS exercise: the roll-rate matrix, reimplemented',
    status: 'not started',
    summary:
      'One already-validated analysis — the roll-rate transition matrix, picked because its correct answer is already known from the dbt/SQL version — rebuilt in SAS. A single, honest artifact, not a claim that this project runs on SAS.',
    why:
      'Named explicitly as a required skill ahead of this role, and the one language in that list this project has zero exposure to (Python and SQL are everywhere else in this repo).',
  },
]
