/**
 * GameDetailShell
 * ──────────────────────────────────────────────────────────────
 * Sport-agnostic game-detail page skeleton. Wraps the three
 * shared phases (loading, prediction, empty) around the
 * two-column prediction layout. Each sport just passes the
 * main/sidebar render callbacks and a few labels.
 *
 * Replaces the duplicated shell JSX in GameDetail.jsx,
 * NHLGameDetail.jsx, and NBAGameDetail.jsx.
 *
 * Props:
 *   game, onBack, matchupExtras -> passed straight to SharedGameHeader
 *   loading                     -> boolean
 *   loadingLabel                -> "Running model..." etc.
 *   prediction                  -> prediction payload (truthy triggers main content)
 *   noPredictionMessage         -> first line in the no-prediction fallback
 *   noPredictionCommand         -> second line (rendered in <code>) e.g. "sync_nhl.bat --full"
 *   renderMain(pred)            -> left column node (PredictionResults)
 *   renderSidebar(pred)         -> right column node (BettingPicks)
 */

import SharedGameHeader from '../gameDetail/SharedGameHeader'

export default function GameDetailShell({
  game,
  onBack,
  matchupExtras,
  loading,
  loadingLabel = 'Running model...',
  prediction,
  noPredictionMessage = 'Prediction unavailable.',
  noPredictionCommand,
  renderMain,
  renderSidebar,
}) {
  return (
    <div className="game-detail">
      <SharedGameHeader game={game} onBack={onBack} matchupExtras={matchupExtras} />

      <div className="detail-prediction">
        {loading && (
          <div className="loading">
            <div className="spinner" />
            <p>{loadingLabel}</p>
          </div>
        )}

        {prediction && (
          <div className="prediction-layout">
            <div className="prediction-main">{renderMain(prediction)}</div>
            <div className="prediction-sidebar">{renderSidebar(prediction)}</div>
          </div>
        )}

        {!loading && !prediction && (
          <div className="no-prediction">
            <p>{noPredictionMessage}</p>
            {noPredictionCommand && <code>{noPredictionCommand}</code>}
          </div>
        )}
      </div>
    </div>
  )
}
