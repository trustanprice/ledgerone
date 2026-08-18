// The site's one shared visual system: a fixed categorical palette (never
// reassigned/cycled) for the vintage cohort lines, and a single-hue
// sequential ramp for every magnitude-encoded chart (the roll-rate heatmap
// and the bucket-distribution bars) so severity always reads the same way
// across scenes.

// 12 cohorts (2022-Q1 .. 2024-Q4), fixed assignment order — a cohort keeps
// its color even if a future feature lets the reader filter others out.
export const COHORT_COLORS = [
  '#4C78A8',
  '#F58518',
  '#54A24B',
  '#E45756',
  '#72B7B2',
  '#EECA3B',
  '#B279A2',
  '#FF9DA6',
  '#9D755D',
  '#BAB0AC',
  '#1B9E77',
  '#D95F02',
]

export function cohortColor(index: number): string {
  return COHORT_COLORS[index % COHORT_COLORS.length]
}

const SEQUENTIAL_LIGHT = { r: 0xea, g: 0xf1, b: 0xfc }
const SEQUENTIAL_DARK = { r: 0x0b, g: 0x3d, b: 0x8f }

/** Single-hue sequential ramp (light -> dark blue), t in [0, 1]. */
export function sequentialBlue(t: number): string {
  const clamped = Math.max(0, Math.min(1, t))
  const r = Math.round(SEQUENTIAL_LIGHT.r + (SEQUENTIAL_DARK.r - SEQUENTIAL_LIGHT.r) * clamped)
  const g = Math.round(SEQUENTIAL_LIGHT.g + (SEQUENTIAL_DARK.g - SEQUENTIAL_LIGHT.g) * clamped)
  const b = Math.round(SEQUENTIAL_LIGHT.b + (SEQUENTIAL_DARK.b - SEQUENTIAL_LIGHT.b) * clamped)
  return `rgb(${r}, ${g}, ${b})`
}

export const COLORS = {
  accent: '#2F6FED',
  accentMuted: '#9DB8F5',
  ink: '#1A1D23',
  inkSecondary: '#5B6270',
  border: '#E4E6EB',
  panelBg: '#FFFFFF',
  pageBg: '#F7F8FA',
  warn: '#E45756',
  good: '#1B9E77',
}
