# AlphaStream dashboard - Shared connection (one form for every tab). Part of app.R, split 2026-10-19: files load in name order (see app.R).

  # allowReconnect("force") is deliberately OFF (2026-07-23): the forced
  # reconnect REPLAYS whole sessions into a busy event loop, and that churn
  # is where the native execCallbacks segfaults keep firing (they survived
  # the later/httpuv/promises rebuilds). Cost: after a server restart the
  # page shows a grey overlay and needs one manual reload instead of
  # auto-resuming. Re-enable only if the segfaults are truly gone.

  # ── Shared connection (one form in the navbar header for the whole app) ──
  # The env switcher and a single validating connect observer live here; every
  # tab's own Connect logic also listens to input$connect_btn, so one click sets
  # up all tabs. Enter the password once.
  setup_env_switcher(input, session)
  status_msgConn <- reactiveVal("Ready. Pick an environment and click Connect (top right).")
  output$statusMessageConn <- renderText({ status_msgConn() })
  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgConn("Enter the password, then Connect."); return() }
    status_msgConn(sprintf("Connecting to %s ...", input$db_env))
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      cfg <- DB_ENVIRONMENTS[[input$db_env]]
      status_msgConn(sprintf("Connected to %s (%s@%s/%s). Now Generate on any tab.",
                             input$db_env, input$db_user, input$db_host, cfg$dbname))
    }, error = function(e) status_msgConn(paste("Connection failed:", e$message)))
    # ignoreInit: do NOT fire on page load (before the password auto-populates),
    # otherwise the empty-password guard leaves a misleading status on screen.
  }, priority = 100, ignoreInit = TRUE)

