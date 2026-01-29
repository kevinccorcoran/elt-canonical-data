# Local Environment Management (direnv)

This project uses **direnv** to manage local development environments.

The environment is defined by files in the repository, not by global shell state.

This guarantees:
- no hidden state
- no accidental use of wrong database
- reproducible local setup
- explicit dev vs staging

---

## What is direnv?

`direnv` automatically loads environment variables when you enter a directory,  
and unloads them when you leave.

The directory itself defines the environment.

No manual `export`, no global switching.

---

## Environment Files

The project contains two environment definitions:

.envrc.dev       → Local development (dev database)  
.envrc.staging   → Local staging (staging database)  

The active environment is selected via a symlink:

.envrc → .envrc.dev  (or .envrc.staging)

Only `.envrc` is read by direnv.

---

## How Switching Works

Switching environment means changing the symlink.

### Switch to DEV (local)

    ln -sf .envrc.dev .envrc
    direnv allow

### Switch to STAGING (local)

    ln -sf .envrc.staging .envrc
    direnv allow

### PROD (managed, automation only)

.envrc.prod is used only by:
- Airflow on the runtime host
- CI/CD or recovery scripts

It must never be activated on a local machine.
DB → DigitalOcean/prod

That is the only switching mechanism.

No shell functions.  
No memory.  
No global state.

---

## How to Verify

At any time:

    echo $ENV
    echo $DB_DATABASE

Should show:

dev / dev  
or  
staging / staging

---

## How to Use Day-to-Day

Just run commands normally:

    python3 script.py
    pytest
    dbt run

The database and environment are guaranteed by the directory.

---

## Security Model

Every time `.envrc` changes, direnv blocks execution and requires:

    direnv allow

This is intentional and prevents malicious code execution.

---

## Design Principle

The environment is part of the codebase.

Not part of:
- your memory
- your shell history
- your global machine state

Switching environments is a filesystem operation, not a mental one.

This makes the system:
- deterministic
- auditable
- reproducible
- impossible to accidentally misconfigure
