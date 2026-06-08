---
title: Authorization
---

XTDB records which users belong to which roles, managed with standard SQL `GRANT`/`REVOKE` statements (v2.2+).

Role assignment is partly an application concern: an identity provider knows organisational groups, but XTDB knows what they mean for its tables.
For deployments whose identity provider can't carry XTDB's roles — no IdP-admin access, or role-less consumer OAuth (Google/GitHub) — XTDB holds the role→user mapping itself.

## Granting and revoking roles (v2.2+)

``` sql
GRANT analyst TO alice;
REVOKE analyst FROM alice;
```

Role and user names are SQL identifiers.
For users whose names contain characters outside the plain-identifier set — an OIDC subject, say — use a delimited identifier:

``` sql
GRANT analyst TO "google-oauth2|103547991597142817347";
```

Roles don't need to be created beforehand — granting a role brings it into existence, and both statements are idempotent (re-granting an existing membership, or revoking one that doesn't exist, is a no-op).

Two restrictions apply:

- Only a [superuser](#superusers) may grant or revoke roles.
- They run against the primary `xtdb` database — they're rejected on a connection to any other database.

## Inspecting membership

Membership is surfaced through the standard Postgres catalog views, so `psql`'s `\du` and other Postgres-introspecting tools work:

`pg_roles`

: One row per user and per granted role.
  Users carry `rolcanlogin = true`; granted roles carry `rolcanlogin = false`.

`pg_auth_members`

: One row per membership, linking a role's `oid` (`roleid`) to a user's `oid` (`member`).

``` sql
SELECT r.rolname AS role, u.rolname AS member
FROM pg_auth_members m
  JOIN pg_roles r ON r.oid = m.roleid
  JOIN pg_roles u ON u.oid = m.member;
```

These views reflect the node's current state.

## Membership history

Memberships are stored in the `xt.role_membership` table — ordinary bitemporal rows, written only by `GRANT`/`REVOKE` (direct DML on the table is rejected).
`REVOKE` closes the membership in system time rather than deleting it, so the history remains queryable for audit:

``` sql
-- who was in which role at a past point in time?
SELECT "user", role
FROM xt.role_membership
  FOR SYSTEM_TIME AS OF TIMESTAMP '2026-06-01T00:00:00Z';

-- the full grant/revoke history
SELECT "user", role, _system_from, _system_to
FROM xt.role_membership FOR ALL SYSTEM_TIME;
```

A membership granted at `T1` and revoked at `T2` shows in force for any system time in `[T1, T2)`, closed from `T2`, and absent before `T1`.

## Superusers

Granting and revoking roles requires a superuser.
The superuser convention is the `xtdb` username:

- `!SingleRootUser`: the root `xtdb` user is the superuser.
- `!UserList`: a configured user named `xtdb` is the superuser.
- `!OpenIdConnect`: no superuser is currently defined, so role membership can't be managed under OIDC yet.
