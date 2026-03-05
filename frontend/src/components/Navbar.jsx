import { Link, useLocation } from 'react-router-dom';
import { Home, History, Zap, Wifi, WifiOff } from 'lucide-react';

function Navbar({ wsConnected }) {
  const location = useLocation();

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
          <div
            className={`ws-status ${wsConnected ? 'ws-connected' : 'ws-disconnected'}`}
            title={wsConnected ? 'Live updates active' : 'Reconnecting...'}
          >
            {wsConnected ? <Wifi size={14} /> : <WifiOff size={14} />}
            <span className="ws-status-text">
              {wsConnected ? 'Live' : 'Offline'}
            </span>
          </div>
        </div>
      </div>
    </nav>
  );
}

export default Navbar;
