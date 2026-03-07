import { useRef } from "react";
import { UploadSimple } from "@phosphor-icons/react";
import { Button } from "@cloudflare/kumo";

interface UploadButtonProps {
  onUpload: (files: FileList) => void;
}

export function UploadButton({ onUpload }: UploadButtonProps) {
  const inputRef = useRef<HTMLInputElement>(null);

  return (
    <>
      <Button
        variant="primary"
        size="sm"
        icon={UploadSimple}
        onClick={() => inputRef.current?.click()}
      >
        Upload
      </Button>
      <input
        ref={inputRef}
        type="file"
        multiple
        className="hidden"
        onChange={(e) => {
          if (e.target.files && e.target.files.length > 0) {
            onUpload(e.target.files);
            e.target.value = "";
          }
        }}
      />
    </>
  );
}
