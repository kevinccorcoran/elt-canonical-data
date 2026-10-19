# AlphaStream dashboard - Server helpers: DB connection, env switcher, severity ring. Part of app.R, split 2026-10-19: files load in name order (see app.R).
# ─── Helper: create a DB connection ───
# One shared connection: reads the global connection_bar() inputs (no per-tab
# suffix). dbname + sslmode come from DB_ENVIRONMENTS keyed by the selected
# environment; host/port/user still come from the fields so a manual override
# survives.
# ── Connection reuse: one warm connection per process ──────────────────────
# A board render fans out to ~39 get_con() sites; opening a fresh SSL connection
# at each dominated wall-clock (measured ~0.22s warm / 3.1s cold handshake x39).
# Cache a single live connection keyed by target identity and hand it to every
# site. Safe here: R/Shiny is single-threaded so queries never truly overlap,
# every read is a fully-materialised dbGetQuery (no open cursors), and the only
# writers use self-contained dbWithTransaction blocks that leave the socket clean.
.CON_CACHE <- new.env(parent = emptyenv())
# Default per-statement cap on the shared connection. A handler that raises it
# (Forecast's heavy curve query) must set it back on exit, or every later query
# from every tab and session inherits the higher cap.
DB_STATEMENT_TIMEOUT <- "25s"

# Every bare dbDisconnect(con) in this app is a per-scope on.exit cleanup that,
# with the shared cached connection, must NOT tear the socket down mid-render.
# Route them to a no-op; get_con is the sole owner and closes stale/replaced
# connections itself via the namespaced DBI::dbDisconnect (left untouched here).
dbDisconnect <- function(conn, ...) invisible(NULL)

get_con <- function(input) {
  env <- input$db_env
  cfg <- DB_ENVIRONMENTS[[env]]
  if (is.null(cfg)) stop(sprintf("Unknown environment '%s'.", env))

  host_val <- input$db_host
  port_val <- suppressWarnings(as.integer(input$db_port))
  if (is.na(port_val)) stop("Port must be a number (e.g. 25060 for prod, 5432 for local).")
  # Password: prefer the environment's OWN credential (DB_ENVIRONMENTS $pass_env,
  # i.e. the container's PROD_DB_PASSWORD/DB_PASSWORD) over the typed field. This
  # is a loopback, unauthenticated dashboard running on the box that already holds
  # these creds, and a browser password-manager can silently overwrite the seeded
  # field with a stale value (survives incognito/hard-refresh) -> spurious
  # "password authentication failed for user doadmin". The field is only the
  # fallback when the env carries no password (a manual custom host).
  pass_val <- getenv_any(cfg$pass_env, input$db_pass)
  # sslmode from config; upgrade prefer->require if the host is non-loopback so a
  # remote host typed under a local preset still gets TLS.
  ssl_mode <- cfg$sslmode
  if (ssl_mode == "prefer" &&
      !grepl("^(host\\.docker\\.internal|localhost|127\\.0\\.0\\.1)$", host_val)) {
    ssl_mode <- "require"
  }

  # Return the cached warm connection when it targets the same host and is still
  # live (see the .CON_CACHE note above this fn). The "|@|" separator cannot
  # occur in a host / port / user / db value, so distinct targets never collide.
  ckey <- paste(env, host_val, port_val, input$db_user, cfg$dbname, ssl_mode,
                sep = "|@|")
  if (identical(.CON_CACHE$key, ckey) && !is.null(.CON_CACHE$con) &&
      isTRUE(tryCatch(DBI::dbIsValid(.CON_CACHE$con), error = function(e) FALSE))) {
    return(.CON_CACHE$con)
  }
  # target changed or the socket died: drop the stale handle, then open fresh
  if (!is.null(.CON_CACHE$con)) try(DBI::dbDisconnect(.CON_CACHE$con), silent = TRUE)
  .CON_CACHE$con <- NULL; .CON_CACHE$key <- NULL

  # In-container DNS hiccups surface as "could not translate host name ...
  # Temporary failure in name resolution" (EAI_AGAIN). They are transient, so
  # retry a few times before surfacing a raw error to the user. Non-transient
  # failures (bad password, refused) are raised immediately.
  transient <- "name resolution|translate host name|EAI_AGAIN|could not connect|server closed the connection|connection timed out"
  # The connect runs on the single-threaded event loop and burns NO CPU while it
  # blocks, so the watchdog's CPU-progress guard can't tell a stuck connect from a
  # hang. Bound the total connect budget hard: 2 attempts x 5s connect_timeout +
  # one short pause = ~10.3s worst case, well under the watchdog window, so a slow
  # SSL path can't stack into a self-inflicted kill (esp. with the portfolio
  # reactives each opening a connection per Generate).
  attempts  <- 2
  con <- NULL
  for (i in seq_len(attempts)) {
    con <- tryCatch(
      dbConnect(RPostgres::Postgres(),
        dbname   = cfg$dbname,
        host     = host_val,
        port     = port_val,
        user     = input$db_user,
        password = pass_val,
        sslmode  = ssl_mode,
        connect_timeout = 5
      ),
      error = function(e) {
        if (i < attempts && grepl(transient, e$message, ignore.case = TRUE)) {
          Sys.sleep(0.3)   # brief; connect_timeout already bounds the real wait
          return(NULL)
        }
        stop(e)            # non-transient, or out of retries
      }
    )
    if (!is.null(con)) break
  }
  # Hard cap BELOW the watchdog window so a stuck query self-aborts before the
  # process watchdog could ever fire. Wrapped so a failed SET (SSL drop right
  # after connect) disconnects instead of leaking the open socket.
  tryCatch(DBI::dbExecute(con, sprintf("SET statement_timeout = '%s'", DB_STATEMENT_TIMEOUT)),
           error = function(e) { try(DBI::dbDisconnect(con), silent = TRUE); stop(e) })
  .CON_CACHE$con <- con; .CON_CACHE$key <- ckey
  con
}

# ─── Helper: wire up the ONE global env-switcher ───
# Fills the shared host/port/user/password fields from DB_ENVIRONMENTS whenever
# the environment changes (and once on load). Called a single time in server().
setup_env_switcher <- function(input, session) {
  observeEvent(input$db_env, {
    cfg <- DB_ENVIRONMENTS[[input$db_env]]
    if (is.null(cfg)) return()
    updateTextInput(session, "db_host", value = cfg$host)
    updateNumericInput(session, "db_port", value = suppressWarnings(as.integer(cfg$port)))
    updateTextInput(session, "db_user", value = cfg$user)
    updateTextInput(session, "db_pass", value = getenv_any(cfg$pass_env, ""))
  })
}

# ─── Helper: severity ring (Buy List) ───
# Outline color = how many checks the row fails: green 0, yellow 1, orange 2,
# red 3+. Counted checks: the cluster gate legs from serving
# (cluster_failed_checks = micro_population / young_history / no_wf_evidence /
# wf_below_bar) + sparse ticker tenure + the row's rank bin historically
# losing (win < 50 on n >= 100). Before the evidence-id rebuild lands the
# serving columns are NA -> falls back to the old amber-for-sparse ring.
severity_ring <- function(df) {
  base <- if ("cluster_failed_checks" %in% names(df))
    suppressWarnings(as.integer(df$cluster_failed_checks)) else rep(NA_integer_, nrow(df))
  extra <- ifelse(!is.na(df$evidence_status) &
                  df$evidence_status %in% c("sparse_ticker", "sparse_both"), 1L, 0L)
  if ("bin_win_pct" %in% names(df))
    extra <- extra + ifelse(!is.na(df$bin_win_pct) & !is.na(df$bin_n) &
                            df$bin_n >= 100 & df$bin_win_pct < 50, 1L, 0L)
  checks <- base + extra
  sparse <- !is.na(df$evidence_status) & df$evidence_status != "mature"
  list(
    checks = checks,
    col = ifelse(is.na(checks), ifelse(sparse, "#f59e0b", "rgba(0,0,0,0)"),
          ifelse(checks == 0L, "#059669",
          ifelse(checks == 1L, "#eab308",
          ifelse(checks == 2L, "#f97316", "#ef4444")))),
    w = ifelse(is.na(checks), ifelse(sparse, 1.5, 0), 1.5)
  )
}

