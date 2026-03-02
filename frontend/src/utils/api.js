import axios from 'axios';

const api = axios.create({
  baseURL: '/api',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

api.interceptors.response.use(
  (response) => response,
  (error) => {
    console.error('API Error:', error.response?.data || error.message);
    return Promise.reject(error);
  }
);

// Schedule endpoints
export const fetchTodaySchedule = () => api.get('/schedule/today');
export const fetchLiveGames = () => api.get('/schedule/live');
export const fetchScheduleByDate = (date) => api.get(`/schedule/${date}`);
export const fetchGameDetails = (gameId) => api.get(`/games/${gameId}`);

// Prediction endpoints
export const fetchTodayPredictions = () => api.get('/predictions/today');
export const fetchBestBets = () => api.get('/predictions/best-bets');
export const fetchPredictionHistory = () => api.get('/predictions/history');
export const fetchPredictionStats = () => api.get('/predictions/stats');
export const regeneratePredictions = () => api.post('/predictions/regenerate');

// Tracked bets endpoints
export const trackBet = (predictionId, units) =>
  api.post('/predictions/tracked', { prediction_id: predictionId, units });
export const fetchTrackedBets = () => api.get('/predictions/tracked');
export const deleteTrackedBet = (id) => api.delete(`/predictions/tracked/${id}`);
export const settleTrackedBets = () => api.post('/predictions/tracked/settle');
export const clearAllTrackedBets = () => api.delete('/predictions/tracked/all');

// Stats endpoints
export const fetchAllTeams = () => api.get('/stats/teams');

// Data management
export const startDataSync = () => api.post('/data/sync/all');
export const fetchSyncStatus = () => api.get('/data/sync/status');

export const triggerDataSync = async (onProgress) => {
  await startDataSync();
  // Poll until done
  while (true) {
    await new Promise((r) => setTimeout(r, 1500));
    const { data } = await fetchSyncStatus();
    if (onProgress) onProgress(data.step);
    if (!data.running) {
      if (data.error) throw new Error(data.error);
      // Notify all listening components to refresh their data
      window.dispatchEvent(new Event('data-synced'));
      return data;
    }
  }
};

export default api;
