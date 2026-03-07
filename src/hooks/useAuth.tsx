import {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
  type ReactNode,
} from "react";
import { api } from "../lib/api";

interface AuthState {
  isAuthenticated: boolean;
  isLoading: boolean;
  userId: string | null;
  email: string | null;
  token: string | null;
  login: (email: string, password: string) => Promise<void>;
  signup: (email: string, password: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthState | null>(null);

const TOKEN_KEY = "mossaic_token";
const USER_KEY = "mossaic_user";

export function AuthProvider({ children }: { children: ReactNode }) {
  const [isLoading, setIsLoading] = useState(true);
  const [token, setToken] = useState<string | null>(null);
  const [userId, setUserId] = useState<string | null>(null);
  const [email, setEmail] = useState<string | null>(null);

  // Load from localStorage on mount
  useEffect(() => {
    const savedToken = localStorage.getItem(TOKEN_KEY);
    const savedUser = localStorage.getItem(USER_KEY);
    if (savedToken && savedUser) {
      try {
        const user = JSON.parse(savedUser);
        setToken(savedToken);
        setUserId(user.userId);
        setEmail(user.email);
        api.setToken(savedToken);
      } catch {
        localStorage.removeItem(TOKEN_KEY);
        localStorage.removeItem(USER_KEY);
      }
    }
    setIsLoading(false);
  }, []);

  const saveAuth = useCallback(
    (t: string, uid: string, em: string) => {
      setToken(t);
      setUserId(uid);
      setEmail(em);
      api.setToken(t);
      localStorage.setItem(TOKEN_KEY, t);
      localStorage.setItem(USER_KEY, JSON.stringify({ userId: uid, email: em }));
    },
    []
  );

  const login = useCallback(
    async (email: string, password: string) => {
      const result = await api.login(email, password);
      saveAuth(result.token, result.userId, result.email);
    },
    [saveAuth]
  );

  const signup = useCallback(
    async (email: string, password: string) => {
      const result = await api.signup(email, password);
      saveAuth(result.token, result.userId, result.email);
    },
    [saveAuth]
  );

  const logout = useCallback(() => {
    setToken(null);
    setUserId(null);
    setEmail(null);
    api.setToken(null);
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
  }, []);

  return (
    <AuthContext.Provider
      value={{
        isAuthenticated: !!token,
        isLoading,
        userId,
        email,
        token,
        login,
        signup,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthState {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}
