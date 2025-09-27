# Testing OAUTHBEARER in PostgreSQL 18

PostgreSQL 18 adds a new authentication mechanism using OAuth 2.0 bearer
tokens. This directory provides a token validator for the purpose of testing the
`db` library's implementation of the OAUTHBEARER authentication protocol.

The code and instructions are designed for Linux (Ubuntu 24.04)
with PostgreSQL installed from the PostgreSQL Apt Repository
(see https://www.postgresql.org/download/linux/ubuntu/).

## The Validator

The `oauth_dupe` token validator acts as follows:
- If the token is exactly the string `"valid"`, then authentication succeeds
  and the user is whoever they say they are.
- Otherwise, authentication fails.

## Setup

- Install PostgreSQL 18.

  - `apt install postgresql-18`

- Install build prerequisites.

  - `apt install postgresql-server-dev-18 libkrb5-dev`
  
- Make and install the plugin:

  - `make && sudo make install`
  
- Edit `/etc/postgresql/18/main/postgresql.conf`

  - Set `oauth_validator_libraries = 'oauth_dupe'`
  - Set `listen_address = '*'` (enables TCP, all interfaces)

- Edit `/etc/postgresql/18/main/pg_hba.conf`:

  - Add `host  all  all  0.0.0.0/0  oauth issuer=https://localhost scope=pg`

    The issuer and scope don't matter for the validator, but
    they will be reported to clients for authentication failure.

- Stop and restart `postgresql` service.

  Note: I had trouble with `systemctl restart postgresql`,
  but separate `stop` and `start` worked.
