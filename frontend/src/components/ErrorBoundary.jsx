import React from 'react';

class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, info) {
    console.error('React Error Boundary caught:', error, info);
    // Report to backend for server-side logging
    try {
      const payload = JSON.stringify({
        error: error?.message || String(error),
        stack: error?.stack?.slice(0, 2000),
        componentStack: info?.componentStack?.slice(0, 2000),
        url: window.location.href,
        timestamp: new Date().toISOString(),
      });
      navigator.sendBeacon(
        '/api/client-error',
        new Blob([payload], { type: 'application/json' }),
      );
    } catch {
      // Reporting failed — don't mask the original error
    }
  }

  handleReset = () => {
    this.setState({ hasError: false, error: null });
  };

  render() {
    if (this.state.hasError) {
      return (
        <div style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          minHeight: '60vh',
          padding: '2rem',
          color: '#ccc',
          textAlign: 'center',
        }}>
          <h2 style={{ color: '#ff5252', marginBottom: '1rem' }}>Something went wrong</h2>
          <p style={{ marginBottom: '1rem', maxWidth: '500px', color: '#999' }}>
            {this.state.error?.message || 'An unexpected error occurred while rendering the page.'}
          </p>
          <button
            onClick={this.handleReset}
            style={{
              padding: '0.5rem 1.5rem',
              background: '#00ff88',
              color: '#0a0a0f',
              border: 'none',
              borderRadius: '6px',
              cursor: 'pointer',
              fontWeight: 600,
            }}
          >
            Try Again
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

export default ErrorBoundary;
