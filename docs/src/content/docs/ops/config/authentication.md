---
title: Authentication
---

XTDB provides authentication to control database access and secure connections.
Authentication rules determine which users can connect and what credentials they must provide.

## Authentication Providers

XTDB supports two authentication providers:

- **User Table** (`!UserTable`): Uses an internal user table with password-based authentication
- **OpenID Connect** (`!OpenIdConnect`): Integrates with external identity providers like Keycloak, Auth0, AWS Cognito or Azure Entra.

## User Table

The `!
UserTable` authentication method uses an internal user table with password-based authentication.

### Configuration

``` yaml
authn: !UserTable
  rules:
    # admin always requires a password
    - user: admin
      method: PASSWORD
    # We trust local connections
    - remoteAddress: 127.0.0.1
      method: TRUST
    # Everything else requires a password
    - method: PASSWORD
```

### User Management

**Default User**
: The `pg_user` table contains a default user "xtdb" with password
    "xtdb".

**Creating Users**
:

``` sql
CREATE USER alan WITH PASSWORD 'TURING'
```

**Modifying Users**
:

``` sql
ALTER USER ada WITH PASSWORD 'LOVELACE'
```

**Password Validation**
: When `PASSWORD` method is specified, credentials are validated
    against the `pg_user` table entries.

## OpenID Connect (OIDC)

The `!
OpenIdConnect` authentication method integrates with external identity providers like Keycloak, Auth0, AWS Cognito or Azure Entra.

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

## Rule Configuration

XTDB controls database access through authentication rules that match users and IP addresses to determine the required authentication method.

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
