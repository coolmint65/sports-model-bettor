import { cn } from '../../lib/utils'

/**
 * SectionCard — replaces the legacy `.result-card` shell used across
 * every game-detail pane. Provides consistent header (title + optional
 * subtitle / right-side action) and body chrome on Tailwind tokens.
 *
 * Composition:
 *   <SectionCard title="Projected Outcome" subtitle="Final score model">
 *     ...body...
 *   </SectionCard>
 *
 * `bodyClassName` lets callers override the default body padding when
 * the content prefers full-bleed (e.g. tables that already pad cells).
 */
export default function SectionCard({
  title,
  subtitle,
  rightSlot,
  children,
  bodyClassName,
  className,
}) {
  return (
    <section className={cn('rounded-lg border border-border bg-card overflow-hidden', className)}>
      {(title || subtitle || rightSlot) && (
        <div className="flex items-start justify-between gap-3 border-b border-border px-5 py-3">
          <div className="min-w-0">
            {title && (
              <h3 className="text-sm font-semibold text-foreground">{title}</h3>
            )}
            {subtitle && (
              <p className="mt-0.5 text-[11px] text-muted-foreground leading-snug">
                {subtitle}
              </p>
            )}
          </div>
          {rightSlot && (
            <div className="flex-shrink-0">{rightSlot}</div>
          )}
        </div>
      )}
      <div className={cn('px-5 py-4', bodyClassName)}>{children}</div>
    </section>
  )
}
