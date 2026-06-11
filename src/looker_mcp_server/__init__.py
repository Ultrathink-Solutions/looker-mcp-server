"""Looker MCP Server — Full-featured MCP server for the Looker API."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("looker-mcp-server")
except PackageNotFoundError:
    # Running from a source tree without an installed distribution.
    __version__ = "0.0.0+unknown"
