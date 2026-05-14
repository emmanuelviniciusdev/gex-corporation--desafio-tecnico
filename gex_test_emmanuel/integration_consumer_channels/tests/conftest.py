import os
import sys
import types

# Ensure the package directory (one level up from tests/) is importable
pkg_name = "integration_consumer_channels"
pkg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

if pkg_path not in sys.path:
    sys.path.insert(0, pkg_path)

# Also register a synthetic package to allow imports relative to this directory if needed
if pkg_name not in sys.modules:
    pkg = types.ModuleType(pkg_name)
    pkg.__path__ = [pkg_path]
    sys.modules[pkg_name] = pkg
