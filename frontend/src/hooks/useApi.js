import { useState, useEffect, useCallback } from 'react';

export function useApi(apiFunc, args = [], immediate = true) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(immediate);
  const [error, setError] = useState(null);

  const execute = useCallback(
    async (...params) => {
      setLoading(true);
      setError(null);
      try {
        const response = await apiFunc(...params);
        setData(response.data);
        return response.data;
      } catch (err) {
        const errorMessage =
          err.response?.data?.detail ||
          err.response?.data?.message ||
          err.message ||
          'An unexpected error occurred';
        setError(errorMessage);
        throw err;
      } finally {
        setLoading(false);
      }
    },
    [apiFunc]
  );

  useEffect(() => {
    if (immediate) {
      execute(...args).catch(() => {
        // Error already captured in state via setError
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [immediate, execute]);

  const refetch = useCallback(() => {
    return execute(...args);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [execute]);

  // Silent refetch: updates data without triggering the loading spinner.
  // Used for background polling and post-sync refreshes.
  const silentRefetch = useCallback(async () => {
    try {
      const response = await apiFunc(...args);
      setData(response.data);
      setError(null);
      return response.data;
    } catch (err) {
      // Don't update error state on silent fails — keep showing stale data
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiFunc]);

  return { data, loading, error, execute, refetch, silentRefetch };
}

export function useApiLazy(apiFunc) {
  return useApi(apiFunc, [], false);
}

export default useApi;
