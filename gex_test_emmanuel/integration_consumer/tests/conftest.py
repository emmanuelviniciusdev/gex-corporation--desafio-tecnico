import os
import sys
import types

pkg_name = "integration_consumer"
# package directory is the parent of the tests/ folder
pkg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

if pkg_name not in sys.modules:
    pkg = types.ModuleType(pkg_name)
    pkg.__path__ = [pkg_path]
    sys.modules[pkg_name] = pkg
