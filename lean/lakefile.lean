import Lake

open Lake DSL

package «Mossaic» where
  -- Standard Mathlib-compat options
  leanOptions := #[
    ⟨`pp.unicode.fun, true⟩,
    ⟨`autoImplicit, false⟩
  ]

require "leanprover-community" / "mathlib" @ git "v4.29.0"

@[default_target]
lean_lib «Mossaic» where
  roots := #[`Mossaic]
