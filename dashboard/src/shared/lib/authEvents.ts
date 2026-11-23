type Listener = () => void;

const expiredListeners = new Set<Listener>();

/** Register a handler for session expiry (401). Returns unsubscribe. */
export function onSessionExpired(listener: Listener): () => void {
  expiredListeners.add(listener);
  return () => {
    expiredListeners.delete(listener);
  };
}

export function emitSessionExpired(): void {
  for (const listener of expiredListeners) {
    listener();
  }
}
