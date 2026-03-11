import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import Navbar from './components/Navbar';
import SportsSidebar from './components/SportsSidebar';
import SportPage from './components/SportPage';
import GameDetail from './components/GameDetail';
import ErrorBoundary from './components/ErrorBoundary';
import { useWebSocket } from './hooks/useWebSocket';

function App() {
  // Single WebSocket connection for the entire app
  const { connected } = useWebSocket();

  return (
    <div className="app">
      <ErrorBoundary>
        <Navbar wsConnected={connected} />
        <div className="app-body">
          <SportsSidebar />
          <main className="main-content">
            <Routes>
              <Route path="/" element={<Navigate to="/nhl" replace />} />
              <Route path="/:sport" element={<SportPage />} />
              <Route path="/games/:id" element={<GameDetail />} />
            </Routes>
          </main>
        </div>
      </ErrorBoundary>
    </div>
  );
}

export default App;
