import { NavLink, useLocation } from "react-router-dom";
import {
  Files,
  BarChart3,
  Sun,
  Moon,
  LogOut,
  PanelLeftClose,
  PanelLeftOpen,
  HardDrive,
  Image,
  BookImage,
  ChevronsUpDown,
  Settings,
  Search,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useTheme } from "@/lib/theme";
import { useAuth } from "@/lib/auth";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuLabel,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Switch } from "@/components/ui/switch";
import { useState } from "react";

const navItems = [
  { to: "/files", icon: Files, label: "Files" },
  { to: "/gallery", icon: Image, label: "Gallery" },
  { to: "/albums", icon: BookImage, label: "Albums" },
  { to: "/analytics", icon: BarChart3, label: "Analytics" },
];

export function Sidebar() {
  const { theme, toggle } = useTheme();
  const { email, logout } = useAuth();
  const [collapsed, setCollapsed] = useState(false);
  const location = useLocation();

  const initials = email
    ? email
        .split("@")[0]
        .slice(0, 2)
        .toUpperCase()
    : "??";

  const username = email ? email.split("@")[0] : "";

  return (
    <aside
      className={cn(
        "flex h-screen flex-col border-r border-white/[0.06] bg-sidebar transition-all duration-200",
        collapsed ? "w-[52px]" : "w-56"
      )}
    >
      {/* Header — logo + brand + collapse toggle */}
      <div className="flex h-14 items-center justify-between px-3">
        <div className="flex items-center gap-2 overflow-hidden">
          <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-primary">
            <HardDrive className="h-3.5 w-3.5 text-primary-foreground" />
          </div>
          {!collapsed && (
            <span className="text-sm font-semibold tracking-tight text-heading">
              Mossaic
            </span>
          )}
        </div>
        {!collapsed && (
          <Tooltip>
            <TooltipTrigger asChild>
              <button
                onClick={() => setCollapsed(true)}
                className="flex h-6 w-6 shrink-0 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-white/[0.06] hover:text-foreground"
              >
                <PanelLeftClose className="h-3.5 w-3.5" />
              </button>
            </TooltipTrigger>
            <TooltipContent side="right">Collapse sidebar</TooltipContent>
          </Tooltip>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 space-y-0.5 px-2 py-2">
        {navItems.map((item) => {
          const isActive =
            location.pathname === item.to ||
            location.pathname.startsWith(item.to + "/");
          return (
            <Tooltip key={item.to}>
              <TooltipTrigger asChild>
                <NavLink
                  to={item.to}
                  className={cn(
                    "group relative flex items-center gap-3 rounded-md px-2 py-1.5 text-[13px] font-medium transition-colors",
                    collapsed && "justify-center px-0",
                    isActive
                      ? "bg-white/[0.07] text-foreground"
                      : "text-muted-foreground hover:bg-white/[0.04] hover:text-foreground"
                  )}
                >
                  {/* Active indicator — left accent bar */}
                  {isActive && (
                    <span className="absolute left-0 top-1/2 h-4 w-[2px] -translate-y-1/2 rounded-full bg-primary" />
                  )}
                  <item.icon className="h-4 w-4 shrink-0" />
                  {!collapsed && <span>{item.label}</span>}
                </NavLink>
              </TooltipTrigger>
              {collapsed && (
                <TooltipContent side="right">{item.label}</TooltipContent>
              )}
            </Tooltip>
          );
        })}
      </nav>

      {/* Expand button when collapsed — subtle, above user section */}
      {collapsed && (
        <div className="flex justify-center px-2 pb-1">
          <Tooltip>
            <TooltipTrigger asChild>
              <button
                onClick={() => setCollapsed(false)}
                className="flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-white/[0.06] hover:text-foreground"
              >
                <PanelLeftOpen className="h-3.5 w-3.5" />
              </button>
            </TooltipTrigger>
            <TooltipContent side="right">Expand sidebar</TooltipContent>
          </Tooltip>
        </div>
      )}

      <Separator />

      {/* User section — clean single row with dropdown */}
      <div className="p-2">
        <DropdownMenu>
          <Tooltip>
            <TooltipTrigger asChild>
              <DropdownMenuTrigger asChild>
                <button
                  className={cn(
                    "flex w-full items-center gap-2.5 rounded-md p-1.5 text-left transition-colors hover:bg-white/[0.06]",
                    collapsed && "justify-center"
                  )}
                >
                  <Avatar className="h-6 w-6 shrink-0">
                    <AvatarFallback className="text-[9px] font-medium">
                      {initials}
                    </AvatarFallback>
                  </Avatar>
                  {!collapsed && (
                    <>
                      <div className="flex-1 overflow-hidden">
                        <p className="truncate text-[13px] font-medium text-foreground leading-tight">
                          {username}
                        </p>
                        <p className="truncate text-[11px] text-muted-foreground leading-tight">
                          {email}
                        </p>
                      </div>
                      <ChevronsUpDown className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                    </>
                  )}
                </button>
              </DropdownMenuTrigger>
            </TooltipTrigger>
            {collapsed && (
              <TooltipContent side="right">Account</TooltipContent>
            )}
          </Tooltip>

          <DropdownMenuContent
            side="top"
            align={collapsed ? "center" : "start"}
            sideOffset={8}
            className="w-56"
          >
            <DropdownMenuLabel className="pb-0">
              <p className="text-[13px] font-medium text-foreground">{username}</p>
              <p className="text-[11px] text-muted-foreground">{email}</p>
            </DropdownMenuLabel>

            <DropdownMenuSeparator />

            {/* Theme toggle */}
            <DropdownMenuItem
              onSelect={(e) => {
                e.preventDefault();
                toggle();
              }}
            >
              {theme === "dark" ? (
                <Sun className="h-4 w-4" />
              ) : (
                <Moon className="h-4 w-4" />
              )}
              <span className="flex-1">
                {theme === "dark" ? "Light mode" : "Dark mode"}
              </span>
              <Switch
                checked={theme === "light"}
                className="pointer-events-none scale-75"
                tabIndex={-1}
              />
            </DropdownMenuItem>

            <DropdownMenuSeparator />

            <DropdownMenuItem
              onSelect={() => logout()}
              className="text-destructive focus:text-destructive"
            >
              <LogOut className="h-4 w-4" />
              <span>Log out</span>
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </aside>
  );
}
