# Runtime Bootstrap

This document describes the one-time setup steps required when provisioning a new runtime host or after removing Docker volumes.

These steps are operational and environment-specific. They are not part of the core system architecture.

---

## Runtime Access

Runtime administration is performed directly on the runtime host.

Access requires SSH:

    ssh root@<runtime-ip>

All commands in this document are executed on the runtime host.

---

## Initial Container Startup

Start the Airflow stack:

    cd /opt/elt-runtime
    docker compose up -d

Verify containers:

    docker ps

---

## Metadata Database Initialization

Airflow requires a metadata database to exist before the webserver is usable.

Initialize the database:

    docker exec -it airflow-webserver airflow db init

This step is required:

- On first runtime provisioning  
- After removing Docker volumes  
- After resetting the metadata database  

---

## Admin User Creation

At least one administrative user must exist for UI access.

Create an admin user:

    docker exec -it airflow-webserver airflow users create \
      --username admin \
      --firstname Kevin \
      --lastname Corcoran \
      --role Admin \
      --email kevin.corcoran@hotmail.com \
      --password <password>

Credentials are not stored in the repository.

---

## Validation

Verify the webserver is reachable:

    http://<runtime-ip>:8080

Login using the created admin user.
