// The site's one shared visual system, dark-glass edition. A fixed
// categorical palette (never reassigned/cycled) for the vintage cohort
// lines, and a single-hue sequential ramp for every magnitude-encoded
// chart (the roll-rate heatmap, bucket-distribution bars) — re-anchored
// for a dark surface: LOW magnitude is a dim tone close to the panel
// background, HIGH magnitude is the vivid accent, so the cells that
// matter most are the ones that visually pop, not the ones that
// disappear into the background the way a light-mode ramp would here.

// 12 cohorts (2022-Q1 .. 2024-Q4), fixed assignment order — a cohort keeps
// its color even if a future feature lets the reader filter others out.
// Brighter/higher-chroma than a light-mode palette would use, so each
// line stays legible against a near-black background.
export const COHORT_COLORS = [
  '#5AA9FF', // blue
  '#FF9F45', // orange
  '#5FDD9D', // green
  '#FF6B6B', // red
  '#5FE0E0', // teal
  '#F5D547', // yellow
  '#C792EA', // purple
  '#FF8FB1', // pink
  '#D2A679', // tan
  '#9AA5B1', // gray
  '#3DDC97', // emerald
  '#FFA24D', // amber
]

export function cohortColor(index: number): string {
  return COHORT_COLORS[index % COHORT_COLORS.length]
}

// Dim slate (close to panel fill) -> vivid accent cyan.
const SEQUENTIAL_LOW = { r: 0x22, g: 0x2b, b: 0x3d }
const SEQUENTIAL_HIGH = { r: 0x4f, g: 0xd8, b: 0xff }

/** Single-hue sequential ramp (dim -> vivid accent), t in [0, 1]. */
export function sequentialBlue(t: number): string {
  const clamped = Math.max(0, Math.min(1, t))
  const r = Math.round(SEQUENTIAL_LOW.r + (SEQUENTIAL_HIGH.r - SEQUENTIAL_LOW.r) * clamped)
  const g = Math.round(SEQUENTIAL_LOW.g + (SEQUENTIAL_HIGH.g - SEQUENTIAL_LOW.g) * clamped)
  const b = Math.round(SEQUENTIAL_LOW.b + (SEQUENTIAL_HIGH.b - SEQUENTIAL_LOW.b) * clamped)
  return `rgb(${r}, ${g}, ${b})`
}

/** Text color that reads on a sequentialBlue(t) fill. */
export function sequentialTextColor(t: number): string {
  return t > 0.5 ? '#071019' : '#EDF2F8'
}

// Shared Recharts chrome (axis ticks, gridlines) — every chart imports
// these instead of hardcoding hex, so a future palette change only
// touches this file.
export const CHART_CHROME = {
  grid: 'rgba(255, 255, 255, 0.08)',
  axisLine: 'rgba(255, 255, 255, 0.16)',
  tick: '#97A1B3',
}

export const COLORS = {
  accent: '#4FD8FF',
  accentStrong: '#8CE8FF',
  ink: '#F3F5F9',
  inkSecondary: '#97A1B3',
  inkTertiary: '#5C6578',
  border: 'rgba(255, 255, 255, 0.12)',
  panelBg: 'rgba(255, 255, 255, 0.05)',
  pageBg: '#05070C',
  good: '#34D399',
  bad: '#F87171',
  warn: '#FBBF24',
}
