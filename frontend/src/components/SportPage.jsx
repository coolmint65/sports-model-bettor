import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { BarChart3, Clock as ClockIcon, Users } from 'lucide-react';
import Dashboard from './Dashboard';
import PlayerProps from './PlayerProps';
import History from './History';

const ALL_TABS = [
  { key: 'dashboard', label: 'Dashboard', icon: BarChart3 },
  { key: 'props', label: 'Player Props', icon: Users },
  { key: 'history', label: 'History', icon: ClockIcon },
];

// Sports that don't have player props yet
const NO_PROPS_SPORTS = new Set([]);

function SportPage() {
  const { sport } = useParams();
  const currentSport = sport || 'nhl';
  const [activeTab, setActiveTab] = useState('dashboard');

  const tabs = NO_PROPS_SPORTS.has(currentSport)
    ? ALL_TABS.filter((t) => t.key !== 'props')
    : ALL_TABS;

  // Reset to dashboard if active tab was removed (e.g. switching from NHL to NBA while on props)
  useEffect(() => {
    if (!tabs.find((t) => t.key === activeTab)) {
      setActiveTab('dashboard');
    }
  }, [currentSport, activeTab, tabs]);

  return (
    <div className="sport-page">
      <div className="sport-page-tabs">
        {tabs.map((tab) => {
          const Icon = tab.icon;
          return (
            <button
              key={tab.key}
              className={`sport-tab ${activeTab === tab.key ? 'sport-tab-active' : ''}`}
              onClick={() => setActiveTab(tab.key)}
            >
              <Icon size={16} />
              <span>{tab.label}</span>
            </button>
          );
        })}
      </div>

      <div className="sport-page-content">
        {activeTab === 'dashboard' && <Dashboard />}
        {activeTab === 'props' && <PlayerProps />}
        {activeTab === 'history' && <History />}
      </div>
    </div>
  );
}

export default SportPage;
