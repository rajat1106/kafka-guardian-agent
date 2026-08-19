import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Route, Routes } from "react-router-dom";

import { AppNav } from "@/components/AppNav";
import { useGuardian } from "@/hooks/useGuardian";
import Connections from "./pages/Connections";
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
      />
      <Routes>
        <Route path="/" element={<Overview />} />
        <Route path="/topology" element={<Topology />} />
        <Route path="/operations" element={<Operations />} />
        <Route path="/connections" element={<Connections />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
    </div>
  );
};

const App = () => (
  <QueryClientProvider client={queryClient}>
    <TooltipProvider>
      <Toaster />
      <Sonner />
      <BrowserRouter>
        <Shell />
      </BrowserRouter>
    </TooltipProvider>
  </QueryClientProvider>
);

export default App;
