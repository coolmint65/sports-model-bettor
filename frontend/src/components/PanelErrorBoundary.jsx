import { Component } from 'react'
// Contains a panel crash so it can never blank the whole app (2026-09-18 incident).
export default class PanelErrorBoundary extends Component {
  constructor(props) { super(props); this.state = { error: null } }
  static getDerivedStateFromError(error) { return { error } }
  componentDidCatch(error, info) { console.error('[EdgeScanner] panel crashed:', error, info) }
  render() {
    if (this.state.error) return (
      <div className="p-6 text-sm text-red-400">
        Edge Scanner hit an error and was contained (rest of the app is fine): {String(this.state.error && this.state.error.message || this.state.error)}
      </div>
    )
    return this.props.children
  }
}
