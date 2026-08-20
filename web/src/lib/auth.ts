/**
 * Token storage and the session's identity.
 *
 * Kept deliberately small: the token lives in localStorage, every request
 * carries it, and a 401 clears it. Storing a bearer token in localStorage is
 * a known trade-off — it is readable by any script that achieves XSS on this
 * origin. The alternative is an httpOnly cookie, which requires the API to
 * set it and brings CSRF handling with it. For a dashboard behind your own
 * network this is the right cost; for a public deployment, move to cookies.
 */

export interface Principal {
  subject: string;
  display_name: string;
  email: string | null;
  roles: string[];
  max_blast: number;
  issuer: string;
  can_configure: boolean;
  can_inject_chaos: boolean;
  can_rollback: boolean;
}

const TOKEN_KEY = "kga.token";
const PRINCIPAL_KEY = "kga.principal";

type Listener = () => void;
const listeners = new Set<Listener>();

export const auth = {
  token(): string | null {
    try {
      return localStorage.getItem(TOKEN_KEY);
    } catch {
      return null;
    }
  },

  principal(): Principal | null {
    try {
      const raw = localStorage.getItem(PRINCIPAL_KEY);
      return raw ? (JSON.parse(raw) as Principal) : null;
    } catch {
      return null;
    }
  },

  set(token: string, principal: Principal) {
    localStorage.setItem(TOKEN_KEY, token);
    localStorage.setItem(PRINCIPAL_KEY, JSON.stringify(principal));
    listeners.forEach((l) => l());
  },

  clear() {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(PRINCIPAL_KEY);
    listeners.forEach((l) => l());
  },

  /** Notifies the shell so a 401 mid-session sends you back to the login. */
  subscribe(listener: Listener): () => void {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },

  headers(): Record<string, string> {
    const t = auth.token();
    return t ? { Authorization: `Bearer ${t}` } : {};
  },
};
