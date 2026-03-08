import { Outlet } from "react-router-dom";
import { Sidebar } from "./sidebar";
import { TooltipProvider } from "@/components/ui/tooltip";

export function AppLayout() {
  return (
    <TooltipProvider delayDuration={0}>
      <div className="flex h-screen overflow-hidden bg-background">
        <Sidebar />
        <main className="flex-1 overflow-y-auto">
          <Outlet />
        </main>
      </div>
    </TooltipProvider>
  );
}
