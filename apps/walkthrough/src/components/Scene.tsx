import type { ReactNode } from 'react'
import { useScrollySteps } from '../hooks/useScrollySteps'

interface SceneProps {
  id: string
  eyebrow: string
  title: string
  steps: ReactNode[]
  renderVisual: (activeStep: number) => ReactNode
}

/**
 * The one layout every scene shares: a sticky visualization panel beside a
 * scrolling column of narrative "margin note" paragraphs. As each
 * paragraph crosses the vertical center of the viewport (see
 * useScrollySteps), it's marked active and renderVisual(activeStep) decides
 * what the pinned chart should look like — the two stay locked together
 * without the reader needing to consciously connect them.
 */
export function Scene({ id, eyebrow, title, steps, renderVisual }: SceneProps) {
  const { activeStep, setStepRef } = useScrollySteps(steps.length)

  return (
    <section className="scene" id={id}>
      <div className="scene-visual-col">
        <div className="scene-visual-sticky glass-panel">
          <div className="scene-visual-header">
            <span className="eyebrow">{eyebrow}</span>
            <h2>{title}</h2>
          </div>
          <div className="scene-visual-body">{renderVisual(activeStep)}</div>
        </div>
      </div>
      <div className="scene-narrative-col">
        {steps.map((step, i) => (
          <p
            key={i}
            ref={setStepRef(i)}
            data-step-index={i}
            className={`narrative-step${i === activeStep ? ' is-active' : ''}`}
          >
            {/* narrative-step is a flex container for vertical centering;
                wrapping in one <span> keeps it a single flex item so the
                rich inline content (text + <strong>/<code>) inside still
                flows and wraps normally instead of each text run becoming
                its own flex item laid out in a row. */}
            <span>{step}</span>
          </p>
        ))}
      </div>
    </section>
  )
}
