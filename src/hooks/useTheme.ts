import { useState, useEffect, useCallback } from "react";

type Theme = "light" | "dark";

const THEME_KEY = "mossaic_theme";

function getSystemPreference(): Theme {
  if (typeof window === "undefined") return "dark";
  return window.matchMedia("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
}

function getStoredTheme(): Theme | null {
  try {
    const stored = localStorage.getItem(THEME_KEY);
    if (stored === "light" || stored === "dark") return stored;
  } catch {
    // ignore
  }
  return null;
}

function applyTheme(theme: Theme) {
  const root = document.documentElement;
  if (theme === "dark") {
    root.setAttribute("data-mode", "dark");
  } else {
    root.removeAttribute("data-mode");
  }
}

export function useTheme() {
  const [theme, setThemeState] = useState<Theme>(() => {
    return getStoredTheme() ?? getSystemPreference();
  });

  // Apply theme on mount and changes
  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  // Listen for system preference changes
  useEffect(() => {
    const mql = window.matchMedia("(prefers-color-scheme: dark)");
    const handler = (e: MediaQueryListEvent) => {
      if (!getStoredTheme()) {
        const newTheme = e.matches ? "dark" : "light";
        setThemeState(newTheme);
      }
    };
    mql.addEventListener("change", handler);
    return () => mql.removeEventListener("change", handler);
  }, []);

  const setTheme = useCallback((newTheme: Theme) => {
    setThemeState(newTheme);
    localStorage.setItem(THEME_KEY, newTheme);
  }, []);

  const toggleTheme = useCallback(() => {
    setTheme(theme === "dark" ? "light" : "dark");
  }, [theme, setTheme]);

  return { theme, setTheme, toggleTheme, isDark: theme === "dark" };
}
