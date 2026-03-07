import { useLocation } from "react-router-dom";
import { SignOut, User, Moon, Sun } from "@phosphor-icons/react";
import { Button, DropdownMenu, Text, Tooltip } from "@cloudflare/kumo";
import { useAuth } from "../../hooks/useAuth";
import { useTheme } from "../../hooks/useTheme";

export function Header() {
  const { email, logout } = useAuth();
  const { isDark, toggleTheme } = useTheme();
  const location = useLocation();

  const getPageTitle = () => {
    if (location.pathname.startsWith("/gallery")) return "Gallery";
    if (location.pathname.startsWith("/analytics")) return "Analytics";
    return "Files";
  };

  return (
    <header className="flex h-14 items-center justify-between border-b border-kumo-line bg-kumo-base px-8">
      <Text variant="heading3" as="h1">
        {getPageTitle()}
      </Text>

      <div className="flex items-center gap-2">
        {/* Theme toggle */}
        <Tooltip
          content={isDark ? "Switch to light mode" : "Switch to dark mode"}
          asChild
        >
          <Button
            variant="ghost"
            shape="square"
            size="sm"
            onClick={toggleTheme}
            aria-label="Toggle theme"
          >
            {isDark ? <Sun size={18} /> : <Moon size={18} />}
          </Button>
        </Tooltip>

        {/* User menu */}
        <DropdownMenu>
          <DropdownMenu.Trigger
            render={
              <Button variant="ghost" size="sm">
                <User size={16} weight="bold" />
                <span className="hidden sm:inline">{email}</span>
              </Button>
            }
          />
          <DropdownMenu.Content>
            <DropdownMenu.Label>
              <div className="flex flex-col gap-0.5">
                <Text size="xs" variant="secondary">
                  Signed in as
                </Text>
                <Text size="sm" bold>
                  {email}
                </Text>
              </div>
            </DropdownMenu.Label>
            <DropdownMenu.Separator />
            <DropdownMenu.Item
              icon={SignOut}
              variant="danger"
              onSelect={logout}
            >
              Sign out
            </DropdownMenu.Item>
          </DropdownMenu.Content>
        </DropdownMenu>
      </div>
    </header>
  );
}
