/**
 * GameDetailShell — sport-agnostic game-detail page skeleton.
 *
 * Phase 2-cleanup restyle: Tailwind tokens. Shell now matches the
 * Tracker / Backtest page rhythm (header section spacing, dashed
 * empty state, spinner-with-label loading). Two-column layout for
 * prediction body collapses to one column below lg.
 *
 * Each sport just passes the main/sidebar render callbacks and a
 * few labels — see GameDetail / NHLGameDetail / NBAGameDetail.
 */

import SharedGameHeader from '../gameDetail/SharedGameHeader'

export default function GameDetailShell({
  game,
  sport,
  onBack,
  matchupExtras,
  loading,
  loadingLabel = 'Running model…',
  prediction,
  noPredictionMessage = 'Prediction unavailable.',
  noPredictionCommand,
  renderMain,
  renderSidebar,
  headerSlot,
}) {
  return (
    <div className="space-y-5 py-4">
      <SharedGameHeader game={game} sport={sport} onBack={onBack} matchupExtras={matchupExtras} />

      {headerSlot && (
        <div className="flex justify-end">{headerSlot}</div>
      )}

      {loading && (
        <div className="flex items-center justify-center gap-3 rounded-lg border border-border bg-card py-12 text-sm text-muted-foreground">
          <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
          {loadingLabel}
        </div>
      )}

      {prediction && (
        <div className="grid grid-cols-1 gap-5 lg:grid-cols-[2fr_1fr]">
          <div className="space-y-4 min-w-0">{renderMain(prediction)}</div>
          <div className="space-y-4 min-w-0">{renderSidebar(prediction)}</div>
        </div>
      )}

      {!loading && !prediction && (
        <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center">
          <div className="text-sm font-semibold text-foreground">{noPredictionMessage}</div>
          {noPredictionCommand && (
            <code className="mt-2 inline-block rounded bg-secondary px-2 py-1 text-[11px] font-mono text-muted-foreground">
              {noPredictionCommand}
            </code>
          )}
        </div>
      )}
    </div>
  )
}
