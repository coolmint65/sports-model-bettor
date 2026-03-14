import { Link } from 'react-router-dom';
import { Leaf, Wifi, WifiOff } from 'lucide-react';

function Navbar({ wsConnected }) {
  return (
    <nav className="navbar">
      <div className="navbar-inner">
        <Link to="/" className="navbar-brand">
          <Leaf className="brand-icon" size={24} />
          <span className="brand-text">MintPicks</span>
          <span className="brand-version">V2</span>
        </Link>

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
