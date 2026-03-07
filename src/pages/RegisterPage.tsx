import { HardDrives } from "@phosphor-icons/react";
import { Surface, Text } from "@cloudflare/kumo";
import { RegisterForm } from "../components/auth/RegisterForm";

export function RegisterPage() {
  return (
    <div className="flex min-h-screen items-center justify-center bg-kumo-elevated p-4">
      <div className="w-full max-w-sm">
        <div className="mb-8 flex flex-col items-center gap-3">
          <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-kumo-brand">
            <HardDrives size={24} weight="bold" className="text-white" />
          </div>
          <Text variant="heading2" as="h1">
            Mossaic
          </Text>
          <Text variant="secondary">Distributed file storage</Text>
        </div>

        <Surface className="rounded-xl p-6">
          <Text variant="heading3" as="h2" className="mb-6 text-center">
            Create your account
          </Text>
          <RegisterForm />
        </Surface>
      </div>
    </div>
  );
}
