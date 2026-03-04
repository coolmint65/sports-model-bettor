import React from 'react';
import { Routes, Route } from 'react-router-dom';
import Navbar from './components/Navbar';
import Dashboard from './components/Dashboard';
import GameDetail from './components/GameDetail';
import History from './components/History';
import ErrorBoundary from './components/ErrorBoundary';
import { useWebSocket } from './hooks/useWebSocket';

function App() {
  // Single WebSocket connection for the entire app
  const { connected } = useWebSocket();

  return (
    <div className="app">
      <ErrorBoundary>
        <Navbar wsConnected={connected} />
        <main className="main-content">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/games/:id" element={<GameDetail />} />
            <Route path="/history" element={<History />} />
          </Routes>
        </main>
      </ErrorBoundary>
    </div>
  );
}

export default App;
