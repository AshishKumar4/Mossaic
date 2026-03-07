import { useState, type FormEvent } from "react";
import { FolderPlus, X } from "@phosphor-icons/react";
import { Dialog, Button, Input, Text } from "@cloudflare/kumo";

interface CreateFolderDialogProps {
  open: boolean;
  onClose: () => void;
  onCreate: (name: string) => Promise<void>;
}

export function CreateFolderDialog({
  open,
  onClose,
  onCreate,
}: CreateFolderDialogProps) {
  const [name, setName] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;

    setError("");
    setIsLoading(true);
    try {
      await onCreate(name.trim());
      setName("");
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create folder");
    } finally {
      setIsLoading(false);
    }
  };

  if (!open) return null;

  return (
    <Dialog.Root
      open={open}
      onOpenChange={(o) => {
        if (!o) onClose();
      }}
    >
      <Dialog className="p-8">
        <div className="mb-4 flex items-start justify-between gap-4">
          <Dialog.Title className="text-xl font-semibold">
            New Folder
          </Dialog.Title>
          <Dialog.Close
            aria-label="Close"
            render={(props: React.ComponentPropsWithRef<"button">) => (
              <Button
                {...props}
                variant="secondary"
                shape="square"
                size="sm"
                icon={<X size={16} />}
                aria-label="Close"
              />
            )}
          />
        </div>
        <form onSubmit={handleSubmit} className="space-y-4">
          <Input
            label="Folder name"
            placeholder="My folder"
            value={name}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setName(e.target.value)
            }
            autoFocus
            required
          />

          {error && (
            <Text variant="error" size="sm">
              {error}
            </Text>
          )}

          <div className="flex justify-end gap-3 pt-2">
            <Dialog.Close
              render={(props: React.ComponentPropsWithRef<"button">) => (
                <Button variant="secondary" {...props}>
                  Cancel
                </Button>
              )}
            />
            <Button
              type="submit"
              variant="primary"
              disabled={isLoading || !name.trim()}
              loading={isLoading}
              icon={FolderPlus}
            >
              Create
            </Button>
          </div>
        </form>
      </Dialog>
    </Dialog.Root>
  );
}
