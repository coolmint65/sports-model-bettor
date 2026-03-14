import { useState } from 'react';
import { useParams } from 'react-router-dom';
import { BarChart3, Clock as ClockIcon, Users } from 'lucide-react';
import Dashboard from './Dashboard';
import PlayerProps from './PlayerProps';
import History from './History';

const TABS = [
  { key: 'dashboard', label: 'Dashboard', icon: BarChart3 },
  { key: 'props', label: 'Player Props', icon: Users },
  { key: 'history', label: 'History', icon: ClockIcon },
];

function SportPage() {
  const { sport } = useParams();
  const currentSport = sport || 'nhl';
  const [activeTab, setActiveTab] = useState('dashboard');

  const sportLabel = currentSport.toUpperCase();

  return (
    <div className="sport-page">
      <div className="sport-page-tabs">
        {TABS.map((tab) => {
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
