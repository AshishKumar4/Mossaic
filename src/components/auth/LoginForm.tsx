import { useState, type FormEvent } from "react";
import { Link } from "react-router-dom";
import { SignIn, WarningCircle } from "@phosphor-icons/react";
import { Button, Input, Banner, Text } from "@cloudflare/kumo";
import { useAuth } from "../../hooks/useAuth";

export function LoginForm() {
  const { login } = useAuth();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [isLoading, setIsLoading] = useState(false);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError("");
    setIsLoading(true);
    try {
      await login(email, password);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-5">
      <Input
        label="Email"
        type="email"
        placeholder="you@example.com"
        value={email}
        onChange={(e: React.ChangeEvent<HTMLInputElement>) => setEmail(e.target.value)}
        required
        autoComplete="email"
      />
      <Input
        label="Password"
        type="password"
        placeholder="Enter your password"
        value={password}
        onChange={(e: React.ChangeEvent<HTMLInputElement>) => setPassword(e.target.value)}
        required
        autoComplete="current-password"
      />

      {error && (
        <Banner
          variant="error"
          icon={<WarningCircle weight="fill" />}
          title={error}
        />
      )}

      <Button
        type="submit"
        variant="primary"
        disabled={isLoading}
        loading={isLoading}
        className="w-full"
        icon={!isLoading ? SignIn : undefined}
      >
        {isLoading ? "Signing in..." : "Sign in"}
      </Button>

      <Text size="sm" variant="secondary" className="text-center">
        Don&apos;t have an account?{" "}
        <Link to="/register" className="text-kumo-link hover:underline">
          Sign up
        </Link>
      </Text>
    </form>
  );
}
