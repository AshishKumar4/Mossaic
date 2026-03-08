import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { AuthProvider, useAuth } from "@/lib/auth";
import { ThemeProvider } from "@/lib/theme";
import { AppLayout } from "@/components/layout/app-layout";
import { AuthPage } from "@/components/auth/auth-page";
import { LandingPage } from "@/pages/landing";
import { FilesPage } from "@/pages/files";
import { GalleryPage } from "@/pages/gallery";
import { AlbumsPage } from "@/pages/albums";
import { AnalyticsPage } from "@/pages/analytics";
import { SharedAlbumPage } from "@/pages/shared-album";
import type { ReactNode } from "react";

function ProtectedRoute({ children }: { children: ReactNode }) {
  const { isAuthenticated } = useAuth();
  if (!isAuthenticated) return <Navigate to="/login" replace />;
  return <>{children}</>;
}

function PublicRoute({ children }: { children: ReactNode }) {
  const { isAuthenticated } = useAuth();
  if (isAuthenticated) return <Navigate to="/files" replace />;
  return <>{children}</>;
}

function LandingRoute() {
  const { isAuthenticated } = useAuth();
  if (isAuthenticated) return <Navigate to="/files" replace />;
  return <LandingPage />;
}

function AppRoutes() {
  return (
    <Routes>
      <Route path="/" element={<LandingRoute />} />
      <Route
        path="/login"
        element={
          <PublicRoute>
            <AuthPage />
          </PublicRoute>
        }
      />
      {/* Public shared album route (no auth required) */}
      <Route path="/shared/:token" element={<SharedAlbumPage />} />
      <Route
        element={
          <ProtectedRoute>
            <AppLayout />
          </ProtectedRoute>
        }
      >
        <Route path="/files" element={<FilesPage />} />
        <Route path="/gallery" element={<GalleryPage />} />
        <Route path="/albums" element={<AlbumsPage />} />
        <Route path="/analytics" element={<AnalyticsPage />} />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}

export function App() {
  return (
    <ThemeProvider>
      <AuthProvider>
        <BrowserRouter>
          <AppRoutes />
        </BrowserRouter>
      </AuthProvider>
    </ThemeProvider>
  );
}
