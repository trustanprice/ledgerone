import { useCallback, useEffect, useRef, useState } from 'react'

/**
 * Drives the scrollytelling interaction: tracks which narrative "step"
 * element (a paragraph inside a scene) is currently crossing the vertical
 * center of the viewport, via IntersectionObserver with a centered
 * rootMargin — the standard technique for this pattern, and simple enough
 * that pulling in a dedicated scrollytelling library (e.g. scrollama)
 * wasn't worth it for 5 short, discrete steps per scene.
 *
 * Returns the active step index (defaults to 0) and a ref-callback factory
 * to attach to each step element.
 */
export function useScrollySteps(stepCount: number) {
  const [activeStep, setActiveStep] = useState(0)
  const elementsRef = useRef<(HTMLElement | null)[]>([])
  const visibleSteps = useRef<Set<number>>(new Set())

  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          const index = Number((entry.target as HTMLElement).dataset.stepIndex)
          if (entry.isIntersecting) visibleSteps.current.add(index)
          else visibleSteps.current.delete(index)
        }
        if (visibleSteps.current.size > 0) {
          setActiveStep(Math.min(...visibleSteps.current))
        }
      },
      { rootMargin: '-45% 0px -45% 0px', threshold: 0 },
    )

    elementsRef.current.forEach((el) => el && observer.observe(el))
    return () => observer.disconnect()
  }, [stepCount])

  const setStepRef = useCallback(
    (index: number) => (el: HTMLElement | null) => {
      elementsRef.current[index] = el
    },
    [],
  )

  return { activeStep, setStepRef }
}
