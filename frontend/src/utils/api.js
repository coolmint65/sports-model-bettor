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
export const fetchGameDetails = (gameId) => api.get(`/games/${gameId}`);
export const fetchLineMovement = (gameId) => api.get(`/games/${gameId}/line-movement`);
export const fetchGameInjuries = (gameId) => api.get(`/games/${gameId}/injuries`);

// Prediction endpoints
export const regeneratePredictions = () =>
  api.post('/predictions/regenerate', null, { timeout: 120000 });

// Player props endpoints
export const fetchTodayProps = (market) =>
  api.get('/props/today', { params: market ? { market } : {} });
export const fetchGameProps = (gameId, market) =>
  api.get(`/props/game/${gameId}`, { params: market ? { market } : {} });
export const fetchTodayPropPicks = () => api.get('/props/picks/today');
export const fetchGamePropPicks = (gameId) => api.get(`/props/picks/game/${gameId}`);

// Tracked bets endpoints
export const trackBet = (predictionId) =>
  api.post('/predictions/tracked', { prediction_id: predictionId });
export const fetchTrackedBets = () => api.get('/predictions/tracked');
export const deleteTrackedBet = (id) => api.delete(`/predictions/tracked/${id}`);
export const clearAllTrackedBets = () => api.delete('/predictions/tracked/all');

export default api;
