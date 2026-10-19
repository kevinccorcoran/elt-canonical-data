# AlphaStream dashboard - DB endpoints, connection bar, sidebar and shared plot helpers. Part of app.R, split 2026-10-19: files load in name order (see app.R).
# ─── Single source of truth for DB endpoints ───
# Every environment's host/port/user/dbname/sslmode lives here and NOWHERE else
# (previously duplicated across widget defaults, the get_con dbname map, the
# sslmode host-regex, and the env-switcher). Env vars override each value so the
# same code serves local dev and the prod server without editing this table;
# `pass_env` names the env var(s) the password field is seeded from (first
# non-empty wins). Production reads the bespoke PROD_DB_* overrides if present,
# else falls back to the generic DB_* vars that the rest of the stack and the
# prod server actually set -- so Host/User/Password autopopulate on the server
# too (it sets only DB_*; that gap left the server's Host + Password blank).
getenv_any <- function(names, default = "") {
  for (n in names) { v <- Sys.getenv(n, ""); if (nzchar(v)) return(v) }
  default
}
DB_ENVIRONMENTS <- list(
  Production = list(host = getenv_any(c("PROD_DB_HOST", "DB_HOST")),
                    port = getenv_any(c("PROD_DB_PORT", "DB_PORT"), "25060"),
                    user = getenv_any(c("PROD_DB_USER", "DB_USER"), "doadmin"),
                    dbname = getenv_any(c("PROD_DB_NAME", "DB_DATABASE"), "prod"),
                    sslmode = "require",
                    pass_env = c("PROD_DB_PASSWORD", "DB_PASSWORD")),
  Staging    = list(host = Sys.getenv("LOCAL_DB_HOST", "host.docker.internal"),
                    port = Sys.getenv("LOCAL_DB_PORT", "5432"),
                    user = Sys.getenv("LOCAL_DB_USER", "postgres"),
                    dbname = "staging", sslmode = "prefer", pass_env = "DB_PASSWORD"),
  Dev        = list(host = Sys.getenv("LOCAL_DB_HOST", "host.docker.internal"),
                    port = Sys.getenv("LOCAL_DB_PORT", "5432"),
                    user = Sys.getenv("LOCAL_DB_USER", "postgres"),
                    dbname = "dev", sslmode = "prefer", pass_env = "DB_PASSWORD"))
DB_ENV_DEFAULT <- "Production"

# ─── Helper: the ONE shared connection bar (rendered once in the navbar header) ───
# Replaces the old per-tab connection forms: pick env, type the password once,
# click Connect once, and every tab reads these global inputs.
connection_bar <- function() {
  d <- DB_ENVIRONMENTS[[DB_ENV_DEFAULT]]
  div(class = "conn-bar",
      style = paste("display:flex; flex-wrap:wrap; align-items:flex-end; gap:0.6rem;",
                    "padding:0.6rem 0.9rem; margin:0 0 0.5rem; background:rgba(255,255,255,0.03);",
                    "border:1px solid #1e293b; border-radius:6px;"),
    div(style = "min-width:130px;",
        selectInput("db_env", "Environment",
                    choices = names(DB_ENVIRONMENTS), selected = DB_ENV_DEFAULT, width = "100%")),
    div(style = "min-width:210px; flex:1 1 210px;",
        textInput("db_host", "Host", value = d$host, width = "100%")),
    div(style = "width:90px;",  numericInput("db_port", "Port", value = as.integer(d$port), min = 1, max = 65535, width = "100%")),
    div(style = "width:120px;", textInput("db_user", "User", value = d$user, width = "100%")),
    div(style = "min-width:150px;", passwordInput("db_pass", "Password", value = "", width = "100%")),
    div(actionButton("connect_btn", "Connect", class = "btn-primary")),
    div(style = "flex:1 1 200px; color:#94a3b8; font-size:0.8rem; padding-bottom:0.4rem;",
        textOutput("statusMessageConn", inline = TRUE)))
}

# ─── Helper: sidebar panel for a given tab suffix ───
# Connection widgets now live in the shared connection_bar(); the sidebar holds
# only this tab's filters, its Generate button, optional post-Generate refiners,
# and its own load-status line.
make_sidebar <- function(suffix, title, filter_widgets, post_widgets = NULL) {
  sidebarPanel(
    h4(title),
    h5("Filters"),
    div(id = paste0("filter_panel", suffix), filter_widgets),
    hr(),
    actionButton(paste0("execute_", suffix), "Generate Chart", class = "btn-primary w-100", style = "margin-top: 1rem;"),
    # post_widgets render UNDER Generate: controls that refine already-loaded
    # data (cluster id filter, rank range) belong after the button, not mixed
    # in with the pre-Generate query params (Kevin 2026-07-25).
    if (!is.null(post_widgets)) tagList(
      hr(),
      div(id = paste0("post_panel", suffix), post_widgets)
    ),
    hr(),
    textOutput(paste0("statusMessage", suffix))
  )
}

# ─── Shared plot/style helpers ───

# Credibility tier colors (cell_credibility.tier), used by every tier plot.
TIER_COLORS <- c(high = '#10b981', medium = '#f59e0b', noise = '#64748b',
                 thin = '#334155', anti = '#dc2626')

# Purple -> yellow -> teal diverging scale; midpoint = zero / coin flip.
DIVERGING_COLORSCALE <- list(
  c(0.00, '#762a83'), c(0.25, '#c2a5cf'), c(0.50, '#f7f7b6'),
  c(0.75, '#5ab4ac'), c(1.00, '#01665e'))

# Standard transparent-background placeholder shown before data loads or
# when a query matches nothing.
empty_plot <- function(msg) {
  plot_ly() %>% layout(
    title = list(text = msg, font = list(color = "#f8fafc")),
    paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)")
}

# Transparent dark-theme layout wrapper; pass axis/shape/margin args through.
dark_layout <- function(p, ...) {
  layout(p, paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)", ...)
}

# Postgres COUNT()/SUM()/bigint arrive as integer64 (bit64). Left uncast,
# bit64 arithmetic truncates fractions BEFORE dividing (hit_rate 0.75 -> 0
# inside weighted.mean), and sprintf("%d") errors. Cast to double up front.
coerce_numeric_cols <- function(df, cols) {
  for (col in intersect(cols, names(df))) df[[col]] <- as.numeric(df[[col]])
  df
}

# Row-separator shapes for rank-grouped horizontal bar charts. ids_display is
# the id per bar in DISPLAY (top-to-bottom) order; the categoryarray is the
# reverse, so boundaries are computed on the reversed vector. Thin line
# between every rank row, stronger line where the cluster id changes; the
# faint per-row lines are dropped beyond 120 bars to keep the DOM sane.
rank_sep_shapes <- function(ids_display) {
  # ids_display = cluster id per row, TOP to BOTTOM display order.
  # Lines use paper coords: category slots fill the plot area evenly, so the
  # boundary after k top rows sits at 1 - k/n. Numeric y0 on a category axis
  # is interpreted inconsistently by plotly.js (can stretch the axis with
  # phantom slots), so shapes never reference the category axis directly.
  n <- length(ids_display)
  if (n < 2) return(list())
  shapes <- lapply(seq_len(n - 1), function(k) {
    id_change <- !identical(ids_display[k], ids_display[k + 1])
    if (!id_change && n > 120) return(NULL)
    yy <- 1 - k / n
    list(type = "line", xref = "paper", x0 = 0, x1 = 1, yref = "paper",
         y0 = yy, y1 = yy,
         line = list(width = if (id_change) 1 else 0.5,
                     color = if (id_change) "rgba(255,255,255,0.35)"
                             else "rgba(255,255,255,0.12)"))
  })
  Filter(Negate(is.null), shapes)
}

