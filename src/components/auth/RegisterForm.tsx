import { useState, type FormEvent } from "react";
import { Link } from "react-router-dom";
import { UserPlus, WarningCircle } from "@phosphor-icons/react";
import { Button, Input, Banner, Text } from "@cloudflare/kumo";
import { useAuth } from "../../hooks/useAuth";

export function RegisterForm() {
  const { signup } = useAuth();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [error, setError] = useState("");
  const [isLoading, setIsLoading] = useState(false);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError("");

    if (password !== confirmPassword) {
      setError("Passwords do not match");
      return;
    }

    if (password.length < 8) {
      setError("Password must be at least 8 characters");
      return;
    }

    setIsLoading(true);
    try {
      await signup(email, password);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Registration failed");
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
        placeholder="At least 8 characters"
        value={password}
        onChange={(e: React.ChangeEvent<HTMLInputElement>) => setPassword(e.target.value)}
        required
        autoComplete="new-password"
      />
      <Input
        label="Confirm Password"
        type="password"
        placeholder="Repeat your password"
        value={confirmPassword}
        onChange={(e: React.ChangeEvent<HTMLInputElement>) => setConfirmPassword(e.target.value)}
        required
        autoComplete="new-password"
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
        icon={!isLoading ? UserPlus : undefined}
      >
        {isLoading ? "Creating account..." : "Create account"}
      </Button>

      <Text size="sm" variant="secondary" className="text-center">
        Already have an account?{" "}
        <Link to="/login" className="text-kumo-link hover:underline">
          Sign in
        </Link>
      </Text>
    </form>
  );
}
