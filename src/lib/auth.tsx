import {
  createContext,
  useContext,
  useState,
  useCallback,
  useEffect,
  type ReactNode,
} from "react";
import { api } from "./api";
import { resetTransferClient } from "./transfer-client";

interface AuthState {
  token: string | null;
  userId: string | null;
  email: string | null;
}

interface AuthContextValue extends AuthState {
  isAuthenticated: boolean;
  login: (email: string, password: string) => Promise<void>;
  signup: (email: string, password: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

const STORAGE_KEY = "mossaic_auth";

function loadAuth(): AuthState {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      api.setToken(parsed.token);
      return parsed;
    }
  } catch {
    // ignore
  }
  return { token: null, userId: null, email: null };
}

function saveAuth(state: AuthState) {
  if (state.token) {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } else {
    localStorage.removeItem(STORAGE_KEY);
  }
  api.setToken(state.token);
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<AuthState>(loadAuth);

  useEffect(() => {
    saveAuth(state);
  }, [state]);

  const login = useCallback(async (email: string, password: string) => {
    const res = await api.login(email, password);
    setState({ token: res.token, userId: res.userId, email: res.email });
  }, []);

  const signup = useCallback(async (email: string, password: string) => {
    const res = await api.signup(email, password);
    setState({ token: res.token, userId: res.userId, email: res.email });
  }, []);

  const logout = useCallback(() => {
    // Drop the cached canonical-VFS HttpVFS client + cached VFS token.
    // The next post-login transfer rebuilds against a fresh token.
    resetTransferClient();
    setState({ token: null, userId: null, email: null });
  }, []);

  return (
    <AuthContext.Provider
      value={{
        ...state,
        isAuthenticated: !!state.token,
        login,
        signup,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
