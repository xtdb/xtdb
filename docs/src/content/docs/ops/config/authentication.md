---
title: Authentication
---

XTDB provides authentication to control database access and secure connections.
Authentication rules determine which users can connect and what credentials they must provide.

## Authentication providers

XTDB supports three authentication providers:

- **Single Root User** (`!SingleRootUser`, v2.2+): a single user (`xtdb`) whose password is configured at startup.
  This is the default.
- **User List** (`!UserList`, v2.2+): a fixed set of users with pre-hashed passwords, configured at startup.
- **OpenID Connect** (`!OpenIdConnect`): integrates with external identity providers like Keycloak, Auth0, AWS Cognito or Azure Entra.

## Single root user (v2.2+)

The `!SingleRootUser` method authenticates a single user named `xtdb` against a password that's configured at startup.

The password is resolved at config-construction time:

1. The explicit `password` field on the YAML config (or Kotlin `SingleRootUser` value), if present.
2. Otherwise, the `XTDB_PASSWORD` environment variable, if set.
3. Otherwise, no password is configured.

If no password is configured, connections run under `TRUST` — no credentials required.
If a password is configured, connections require `PASSWORD` authentication as the `xtdb` user.
Any other username is rejected.

### Configuration

Resolve the password from `XTDB_PASSWORD` (the common case for containerised deployments):

``` yaml
authn: !SingleRootUser
```

Or pass the password explicitly (discouraged for production — anyone with access to the config can read it):

``` yaml
authn: !SingleRootUser
  password: !Env XTDB_PASSWORD
```

``` yaml
authn: !SingleRootUser
  password: hunter2
```

## User list (v2.2+)

The `!UserList` method authenticates against a fixed set of users configured at startup.
It suits simple deployments that want password authentication for more than one user without running an external identity provider.

The user set is static.
It's read from the configuration when the node starts, and the only way to change it is to edit the configuration and restart — there is no SQL or runtime API to add, alter, or remove users.

### Configuration

Each user maps to a pre-hashed password, tagged with the algorithm that produced it:

``` yaml
authn: !UserList
  users:
    alice: !Argon2id "$argon2id$v=19$m=65536,t=3,p=1$c29tZXNhbHRzYWx0$RdescudvJCsgt3ub+b+dWRWJTmaaJObG"
    bob: !BCrypt "$2y$12$mc.G6e7uChPgZW2NfY0XQOQ0qN6Q0o3Yv0bFv6kKQXmnq7nqQk0K"
  rules:
    # local connections are trusted
    - remoteAddress: 127.0.0.1
      method: TRUST
    # everyone else must supply a password
    - method: PASSWORD
```

If `rules` is omitted, every connection requires a password.

The configured users appear in the read-only `pg_user` view (with `passwd` redacted to `NULL`), so Postgres tooling that lists users sees them.
`usesuper` is `true` only for a user named `xtdb` — the same superuser convention as `!SingleRootUser` — and `false` for everyone else.

### Hash algorithms

Each password carries its own algorithm tag, so a node can hold a mix and you can move to a new algorithm without re-hashing the existing entries:

- `!Argon2id` — argon2id, the recommended default.
- `!BCrypt` — bcrypt, in standard modular-crypt (`$2…`) form.

### Hashing a password

Pre-hash passwords with the `hash-password` command and paste the output into the config:

``` bash
xtdb hash-password [--argon2id | --bcrypt] 'my-password'
```

`--argon2id` is the current default; pass `--bcrypt` to use bcrypt instead.
Omit the password argument to read it from stdin, which keeps the plaintext out of your shell history:

``` bash
echo 'my-password' | xtdb hash-password
```

## OpenID Connect (OIDC)

The `!OpenIdConnect` authentication method integrates with external identity providers like Keycloak, Auth0, AWS Cognito or Azure Entra.

### Basic Configuration

``` yaml
authn: !OpenIdConnect
  issuerUrl: https://your-keycloak.example.com/realms/master
  clientId: xtdb-client
  clientSecret: !Env OIDC_CLIENT_SECRET
  rules:
    - user: oidc-client
      method: CLIENT_CREDENTIALS
    - method: PASSWORD
```

For complete OIDC configuration, setup guides, and troubleshooting, see [OpenID Connect Authentication](authentication/oidc).

## Rule configuration

`!UserList` and `!OpenIdConnect` control database access through authentication rules that match users and IP addresses to determine the required authentication method.
`!SingleRootUser` doesn't use rules — its method is determined by whether a password is configured.

### Authentication Rules

Authentication rules are evaluated in order until the first match.
If no rules match, the connection is rejected.

**Rule Parameters**
: - `user` (optional): Match specific username

    - `remoteAddress` (optional): Match IP address or CIDR block (IPv4 or IPv6)
    - `method` (required): Authentication method to use

**Available Methods**
: - `TRUST`: No authentication required

    - `PASSWORD`: Require username/password validation
    - `CLIENT_CREDENTIALS`: OAuth client credentials flow (OIDC only)
    - `DEVICE_AUTH`: OAuth device authorization flow (OIDC only)

**Example Rule**
:

``` yaml
- user: admin
  remoteAddress: 127.0.0.1
  method: PASSWORD
```

This rule requires the `admin` user to provide a password when connecting from `localhost`.
