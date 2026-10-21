# A new Mac

Everything AlphaStream needs lives off the laptop: the code on GitHub, the settings files and the small local
databases in a weekly encrypted backup in iCloud Drive, and the data in prod. A new Mac is set up from those three
with one script. alphastream-conformance checks every week that this still holds (Laptop replaceable).

## Now, on the old Mac (once)

1. `tools/mac/backup.sh init` and choose a passphrase. Keep it in your password manager: the new Mac needs it.
2. `tools/mac/backup.sh on`: a backup each Sunday at 20:00 into `iCloud Drive/AlphaStream backups`.
   `tools/mac/backup.sh run` makes one now, `tools/mac/backup.sh check` opens it and confirms every file is inside.
3. Push every repo to GitHub. `tools/mac/new_mac.sh --check` lists what is only on this Mac.
4. Optional, an external drive: set `STAGING_DUMP_DIR` in `docker/staging-postgres/.env` to a folder on it. The
   prod copy then lives off the laptop too, and a new Mac loads staging from it in minutes, not an hour.

What the backup holds is `tools/mac/settings_files.txt` (every `.env` and `.envrc` the repos need, and
`~/.ssh/config`) plus the dev, airflow_db and board_test databases. Not staging: a new Mac copies it from prod.
Not the ssh keys: a new Mac makes its own.

## On the day, on the new Mac

1. Sign in to iCloud and let `iCloud Drive/AlphaStream backups` download.
2. Install Homebrew (https://brew.sh).
3. Make an ssh key (`ssh-keygen -t ed25519`) and add the public half to GitHub (`gh auth login`, then
   `gh ssh-key add ~/.ssh/id_ed25519.pub`). From the old Mac, or the DigitalOcean console, add it to the
   droplet's `~/.ssh/authorized_keys` so the checks and deploys reach it.
4. Clone this repo and run the script. It asks for the backup passphrase once.

   ```
   git clone git@github.com:kevinccorcoran/elt-canonical-data.git ~/repos/elt-canonical-data
   ~/repos/elt-canonical-data/tools/mac/new_mac.sh
   ```

It installs the tools (`tools/mac/Brewfile`), clones the other repos, restores the settings and databases, rebuilds
staging (from the drive's dump, else from prod outside prod's 03:00 to 12:00 chain), starts staging's Airflow, the
test report and every timer, and sets up the app's and the checks' Python. Rerun it if a step fails; it skips what
is done.

Then: the app at http://127.0.0.1:8050 (the `dash` container, which the script starts),
staging's Airflow at http://localhost:8082, the test report at http://localhost:49548.
