# Initializes dbt config directory and normalizes permissions during Heroku release phase
#!/usr/bin/env bash
mkdir -p /app/.dbt
cp /app/.dbt/profiles.yml /app/.dbt/profiles.yml
chmod 644 /app/.dbt/profiles.yml
