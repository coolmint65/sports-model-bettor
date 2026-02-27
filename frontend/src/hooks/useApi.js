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
      execute(...args);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [immediate]);

  const refetch = useCallback(() => {
    return execute(...args);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [execute]);

  return { data, loading, error, execute, refetch };
}

export function useApiLazy(apiFunc) {
  return useApi(apiFunc, [], false);
}

export default useApi;
