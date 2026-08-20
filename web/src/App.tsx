import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Route, Routes } from "react-router-dom";

import { useEffect, useState } from "react";
import { AppNav } from "@/components/AppNav";
import { useGuardian } from "@/hooks/useGuardian";
import { api } from "@/lib/api";
import { auth } from "@/lib/auth";
import Connections from "./pages/Connections";
import Incident from "./pages/Incident";
import Login from "./pages/Login";
import NotFound from "./pages/NotFound";
import Operations from "./pages/Operations";
import Overview from "./pages/Overview";
import Topology from "./pages/Topology";

const queryClient = new QueryClient();

/**
 * The nav needs live connection state, which comes from the same hook the
 * pages use. Keeping it in a shell inside the router means every page shares
 * one WebSocket rather than opening its own.
 */
const Shell = () => {
  const { connected, state, cluster } = useGuardian();
  return (
    <div className="min-h-screen bg-background">
      <AppNav
        connected={connected}
        pendingApprovals={state.pending_approvals.length}
        clusterLabel={cluster?.description}
        principal={auth.principal()}
        onSignOut={auth.clear}
      />
      <Routes>
        <Route path="/" element={<Overview />} />
        <Route path="/topology" element={<Topology />} />
        <Route path="/operations" element={<Operations />} />
        <Route path="/incidents/:id" element={<Incident />} />
        <Route path="/connections" element={<Connections />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
    </div>
  );
};

/**
 * Decides whether to show the app or the login screen.
 *
 * Subscribes to the token store so a 401 anywhere in the app — an expired
 * session, a revoked user — returns here rather than leaving every button
 * silently inert, which is exactly how the missing login manifested.
 */
const AuthGate = () => {
  const [mode, setMode] = useState<string | null>(null);
  const [token, setToken] = useState<string | null>(auth.token());

  useEffect(() => auth.subscribe(() => setToken(auth.token())), []);
  useEffect(() => {
    api.authConfig()
      .then((c) => setMode(c.enabled ? c.mode : "disabled"))
      .catch(() => setMode("disabled"));
  }, []);

  if (mode === null) {
    return <div className="min-h-screen bg-background" />;
  }
  if (mode !== "disabled" && !token) {
    return <Login mode={mode} />;
  }
  return <Shell />;
};

const App = () => (
  <QueryClientProvider client={queryClient}>
    <TooltipProvider>
      <Toaster />
      <Sonner />
      <BrowserRouter>
        <AuthGate />
      </BrowserRouter>
    </TooltipProvider>
  </QueryClientProvider>
);

export default App;
