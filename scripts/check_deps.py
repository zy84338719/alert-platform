#!/usr/bin/env python3
"""Package dependency lint for the Go monorepo (BASE-001).

Encodes the docs/02 §3 dependency direction as an allowlist:

    Adapter -> Application -> Domain ports
    Infrastructure implements Domain/Application ports
    Domain depends on nothing internal and stays standard-library only

Any violation exits non-zero so `make deps-lint` (and CI) turn red.
Dependency-free: standard library plus `go list` only.
"""
import json
import subprocess
import sys

MODULE = "github.com/example/alert-platform"
GEN_PREFIX = MODULE + "/gen/"
DOMAIN_PREFIX = MODULE + "/internal/domain/"

# Allowed internal import prefixes per internal layer; cmd/ and gen/ are free.
RULES = {
    DOMAIN_PREFIX: [],
    MODULE + "/internal/application/": [MODULE + "/internal/domain/", GEN_PREFIX],
    MODULE + "/internal/adapter/": [
        MODULE + "/internal/application/",
        MODULE + "/internal/domain/",
        GEN_PREFIX,
    ],
    MODULE + "/internal/infrastructure/": [
        MODULE + "/internal/application/",
        MODULE + "/internal/domain/",
        GEN_PREFIX,
    ],
}


def layer_of(import_path):
    """Return the matching internal layer prefix, or None for other packages."""
    best = None
    for prefix in RULES:
        if import_path.startswith(prefix) and (best is None or len(prefix) > len(best)):
            best = prefix
    return best


def is_stdlib(import_path):
    """Stdlib paths have no dot in the first segment (fmt, net/http);
    external module paths always carry a domain (github.com/...)."""
    return "." not in import_path.split("/")[0]


def main():
    try:
        out = subprocess.run(
            ["go", "list", "-json", "./..."],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    except subprocess.CalledProcessError as exc:
        print("go list failed:\n" + exc.stderr, file=sys.stderr)
        return 1

    decoder = json.JSONDecoder()
    packages, idx = [], 0
    while idx < len(out):
        while idx < len(out) and out[idx] in " \n\t\r":
            idx += 1
        if idx >= len(out):
            break
        obj, idx = decoder.raw_decode(out, idx)
        packages.append(obj)

    violations = []
    checked = 0
    for pkg in packages:
        path = pkg.get("ImportPath", "")
        if path.startswith(MODULE + "/cmd/") or path.startswith(GEN_PREFIX):
            continue
        layer = layer_of(path)
        if layer is None:
            continue
        checked += 1
        # Production dependency edges govern the layer rule; test files may
        # import any internal layer (integration tests construct real
        # infrastructure), but restricted EXTERNAL libs stay banned everywhere.
        imports = list(pkg.get("Imports", []))
        test_only = list(pkg.get("TestImports", [])) + list(pkg.get("XTestImports", []))
        for imp in imports + test_only:
            is_test_only = imp in test_only and imp not in imports
            if imp == "C":
                continue  # cgo pseudo-package
            if imp.startswith(MODULE):
                if is_test_only:
                    continue  # test files may construct any internal layer
                allowed = (
                    any(imp.startswith(p) for p in RULES[layer])
                    or imp.startswith(layer)  # intra-layer imports are fine
                )  # e.g. hz's router/v1 -> handler/v1 inside the adapter layer
                if not allowed:
                    violations.append(
                        f"{path}: internal import outside the {layer} allowlist: {imp}"
                    )
            elif layer == DOMAIN_PREFIX and not is_stdlib(imp):
                violations.append(
                    f"{path}: domain must stay standard-library-only; "
                    f"found external import: {imp}"
                )

    if violations:
        print("dependency rule violations:")
        for v in sorted(set(violations)):
            print("  -", v)
        return 1
    print(f"OK: dependency rules hold for {checked} internal packages.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
