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

// Prediction endpoints
export const regeneratePredictions = () =>
  api.post('/predictions/regenerate', null, { timeout: 120000 });

// Tracked bets endpoints
export const trackBet = (predictionId) =>
  api.post('/predictions/tracked', { prediction_id: predictionId });
export const fetchTrackedBets = () => api.get('/predictions/tracked');
export const deleteTrackedBet = (id) => api.delete(`/predictions/tracked/${id}`);
export const clearAllTrackedBets = () => api.delete('/predictions/tracked/all');

export default api;
