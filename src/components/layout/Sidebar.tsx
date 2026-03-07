import { NavLink } from "react-router-dom";
import { Files, Images, HardDrives, ChartBar } from "@phosphor-icons/react";
import { Text } from "@cloudflare/kumo";
import { cn } from "../../lib/utils";

const navItems = [
  { to: "/files", label: "Files", icon: Files },
  { to: "/gallery", label: "Gallery", icon: Images },
  { to: "/analytics", label: "Analytics", icon: ChartBar },
];

export function Sidebar() {
  return (
    <aside className="flex w-60 flex-col border-r border-kumo-line bg-kumo-base">
      {/* Logo */}
      <div className="flex h-14 items-center gap-2.5 border-b border-kumo-line px-5">
        <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-kumo-brand">
          <HardDrives size={18} weight="bold" className="text-white" />
        </div>
        <Text size="lg" bold>
          Mossaic
        </Text>
      </div>

      {/* Navigation */}
      <nav className="flex-1 space-y-1 p-3">
        {navItems.map(({ to, label, icon: Icon }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) =>
              cn(
                "flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors",
                isActive
                  ? "bg-kumo-overlay text-kumo-default"
                  : "text-kumo-strong hover:bg-kumo-overlay/50 hover:text-kumo-default"
              )
            }
          >
            <Icon size={18} weight="duotone" />
            {label}
          </NavLink>
        ))}
      </nav>

      {/* Footer */}
      <div className="border-t border-kumo-line px-5 py-4">
        <Text size="xs" variant="secondary">
          Distributed chunked storage
        </Text>
      </div>
    </aside>
  );
}
