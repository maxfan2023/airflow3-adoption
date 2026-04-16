#!/usr/bin/env python3
"""Import DAG modules inside the target Python environment."""

import argparse
import importlib
import os
import sys
import traceback
from pathlib import Path


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Import probe for DAG validation.")
    parser.add_argument("--import-root", required=True)
    parser.add_argument("--package-root", required=True)
    parser.add_argument("--extra-pythonpath", action="append", default=[])
    parser.add_argument("--module", action="append", default=[])
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    import_root = Path(args.import_root).expanduser().resolve()
    package_root = Path(args.package_root).expanduser().resolve()

    paths = [str(package_root), str(import_root)]
    paths.extend(str(Path(item).expanduser().resolve()) for item in args.extra_pythonpath)
    sys.path[:0] = [item for item in paths if item not in sys.path]
    os.chdir(str(import_root))
    importlib.invalidate_caches()

    for module_name in args.module:
        try:
            importlib.import_module(module_name)
        except Exception as exc:
            print(
                "Import check failed for module {0}: {1}: {2}".format(
                    module_name,
                    exc.__class__.__name__,
                    exc,
                ),
                file=sys.stderr,
            )
            traceback.print_exc(file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
