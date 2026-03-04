import { useEffect, useRef, useState, useCallback } from 'react';

/**
 * WebSocket hook for live data updates.
 *
 * Connects to /ws/live, auto-reconnects on disconnect, and dispatches
 * custom window events so any component can listen for specific update types.
 *
 * Events dispatched:
 *   - 'ws:odds_update'      — odds changed for one or more games
 *   - 'ws:initial_state'    — full state snapshot on connect
 *   - 'ws:connected'        — WebSocket connection established
 *   - 'ws:disconnected'     — WebSocket connection lost
 */

const RECONNECT_DELAYS = [1000, 2000, 4000, 8000, 15000]; // exponential backoff
const PING_INTERVAL = 30000; // 30s keepalive

export function useWebSocket() {
  const [connected, setConnected] = useState(false);
  const wsRef = useRef(null);
  const reconnectAttemptRef = useRef(0);
  const reconnectTimerRef = useRef(null);
  const pingTimerRef = useRef(null);
  const mountedRef = useRef(true);

  const connect = useCallback(() => {
    if (!mountedRef.current) return;

    // Build WebSocket URL relative to current host
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${window.location.host}/ws/live`;

    try {
      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        if (!mountedRef.current) return;
        reconnectAttemptRef.current = 0;
        setConnected(true);
        window.dispatchEvent(new Event('ws:connected'));

        // Start keepalive pings
        pingTimerRef.current = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send('ping');
          }
        }, PING_INTERVAL);
      };

      ws.onmessage = (event) => {
        if (!mountedRef.current) return;
        try {
          const data = JSON.parse(event.data);
          if (data.type === 'pong') return;

          // Dispatch typed event so components can subscribe selectively
          window.dispatchEvent(
            new CustomEvent(`ws:${data.type}`, { detail: data })
          );
        } catch {
          // Ignore unparseable messages
        }
      };

      ws.onclose = () => {
        if (!mountedRef.current) return;
        cleanup();
        setConnected(false);
        window.dispatchEvent(new Event('ws:disconnected'));
        scheduleReconnect();
      };

      ws.onerror = () => {
        // onclose will fire after onerror, which handles reconnect
      };
    } catch {
      scheduleReconnect();
    }
  }, []);

  const cleanup = useCallback(() => {
    if (pingTimerRef.current) {
      clearInterval(pingTimerRef.current);
      pingTimerRef.current = null;
    }
  }, []);

  const scheduleReconnect = useCallback(() => {
    if (!mountedRef.current) return;
    const attempt = reconnectAttemptRef.current;
    const delay = RECONNECT_DELAYS[Math.min(attempt, RECONNECT_DELAYS.length - 1)];
    reconnectAttemptRef.current = attempt + 1;

    reconnectTimerRef.current = setTimeout(() => {
      if (mountedRef.current) {
        connect();
      }
    }, delay);
  }, [connect]);

  useEffect(() => {
    mountedRef.current = true;
    connect();

    return () => {
      mountedRef.current = false;
      cleanup();
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
      }
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [connect, cleanup]);

  return { connected };
}

/**
 * Hook to subscribe to a specific WebSocket event type.
 * Re-renders the component with the latest event data.
 */
export function useWebSocketEvent(eventType, handler) {
  const handlerRef = useRef(handler);
  handlerRef.current = handler;

  useEffect(() => {
    const listener = (event) => {
      handlerRef.current(event.detail);
    };
    window.addEventListener(`ws:${eventType}`, listener);
    return () => window.removeEventListener(`ws:${eventType}`, listener);
  }, [eventType]);
}

export default useWebSocket;
