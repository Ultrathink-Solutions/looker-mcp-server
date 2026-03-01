# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, use one of these methods:

1. **GitHub Security Advisories** (preferred): [Report a vulnerability](https://github.com/ultrathink-solutions/looker-mcp-server/security/advisories/new)
2. **Email**: security@ultrathinksolutions.com

### What to include

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

### What to expect

- **Acknowledgment** within 48 hours
- **Assessment** within 5 business days
- **Fix timeline** communicated once the issue is confirmed
- **Credit** in the release notes (unless you prefer to remain anonymous)

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

## Security Considerations

This server handles Looker API credentials and user tokens. Operators should:

- Never expose the server to the public internet without authentication (`LOOKER_MCP_AUTH_TOKEN`)
- Use TLS for all HTTP transport connections
- Rotate API3 credentials regularly
- Restrict tool groups to the minimum required (`--groups`)
- Monitor structured logs for authentication failures
- Use the readiness endpoint (`/readyz`) to verify connectivity before accepting traffic
