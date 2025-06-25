<!--
SPDX-License-Identifier: Apache-2.0
-->

# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in XTDB, please report it privately to the XTDB team at **[security@xtdb.com](mailto:security@xtdb.com)**.

Please **do not file public GitHub issues** for security concerns, even if you believe they are minor or low impact.

If you prefer to encrypt your message, you can use [@jarohen's Keybase profile](https://keybase.io/jarohen) to find the appropriate PGP key.

We support **coordinated disclosure** and ask that you give us an opportunity to investigate and address the issue before disclosing it publicly.

You may also report issues using the GitHub [security advisory workflow](https://docs.github.com/en/code-security/security-advisories/repository-security-advisories/about-repository-security-advisories). This allows private reporting and optional CVE publication via GitHub's interface.

## Response and Resolution Timeline

We aim to:

- Acknowledge vulnerability reports within **48 hours**
- Provide a resolution or mitigation within **90 days**, depending on severity and complexity

We will keep you informed throughout the process and notify you when a fix is available.

## Supported Versions

The following XTDB versions receive security updates:

| Version     | Status        |
|-------------|----------------|
| `2.x`       | ✅ Supported   |
| `1.24.x`    | ✅ Supported   |
| `< 1.24.0`  | ❌ Unsupported |

We recommend using the latest patch release in a supported series to receive security updates.

## Dependency Management

XTDB uses [Dependabot](https://docs.github.com/en/code-security/dependabot) to monitor upstream dependencies for known vulnerabilities. We actively maintain and upgrade critical libraries such as:

- Kotlin and Java platform dependencies
- Netty (for networking)
- Kafka client
- Cloud storage SDKs (e.g., AWS S3)

## Deployment Expectations

XTDB is designed for trusted environments. Specifically:

- XTDB nodes are expected to run **behind firewalls**
- Users are responsible for securing their **object stores** and **transaction logs**
- Authentication, access control, and network security are the responsibility of the deployment environment
