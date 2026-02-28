import React, { useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { BarChart3, Home, History, RefreshCw, Zap } from 'lucide-react';
import { triggerDataSync } from '../utils/api';

function Navbar() {
  const location = useLocation();
  const [syncing, setSyncing] = useState(false);
  const [syncMessage, setSyncMessage] = useState('');

  const handleSync = async () => {
    if (syncing) return;
    setSyncing(true);
    setSyncMessage('');
    try {
      await triggerDataSync((step) => {
        setSyncMessage(step || 'Syncing...');
      });
      setSyncMessage('Sync complete!');
      setTimeout(() => setSyncMessage(''), 3000);
    } catch (err) {
      setSyncMessage('Sync failed');
      setTimeout(() => setSyncMessage(''), 3000);
    } finally {
      setSyncing(false);
    }
  };

  const isActive = (path) => location.pathname === path;

  return (
    <nav className="navbar">
      <div className="navbar-inner">
        <Link to="/" className="navbar-brand">
          <Zap className="brand-icon" size={24} />
          <span className="brand-text">Sports Betting Model</span>
        </Link>

        <div className="navbar-links">
          <Link
            to="/"
            className={`nav-link ${isActive('/') ? 'nav-link-active' : ''}`}
          >
            <Home size={18} />
            <span>Dashboard</span>
          </Link>
          <Link
            to="/history"
            className={`nav-link ${isActive('/history') ? 'nav-link-active' : ''}`}
          >
            <History size={18} />
            <span>History</span>
          </Link>
        </div>

        <div className="navbar-actions">
          {syncMessage && (
            <span
              className={`sync-message ${
                syncMessage.includes('failed') || syncMessage.includes('Failed')
                  ? 'sync-error'
                  : syncMessage.includes('complete')
                    ? 'sync-success'
                    : 'sync-progress'
              }`}
            >
              {syncMessage}
            </span>
          )}
          <button
            className={`btn btn-sync ${syncing ? 'syncing' : ''}`}
            onClick={handleSync}
            disabled={syncing}
          >
            <RefreshCw size={16} className={syncing ? 'spin' : ''} />
            <span>{syncing ? 'Syncing...' : 'Sync Data'}</span>
          </button>
        </div>
      </div>
    </nav>
  );
}

export default Navbar;
