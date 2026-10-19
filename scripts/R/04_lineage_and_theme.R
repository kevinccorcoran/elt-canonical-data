# AlphaStream dashboard - Schema lineage order and the theme CSS. Part of app.R, split 2026-10-19: files load in name order (see app.R).
# ─── Schema display order + lineage ranks from dbt manifests ───
SCHEMA_ORDER <- c("raw", "cdm", "metrics", "analysis", "inference")

compute_lineage_ranks <- function() {
  manifest_paths <- c(
    "/opt/elt-inference-models/target/manifest.json",
    "/opt/elt-canonical-data/dbt/src/app/target/manifest.json"
  )
  node_info <- list()
  for (p in manifest_paths) {
    if (!file.exists(p)) next
    m <- tryCatch(jsonlite::fromJSON(p, simplifyVector = FALSE),
                  error = function(e) NULL)
    if (is.null(m) || is.null(m$nodes)) next
    for (uid in names(m$nodes)) {
      n <- m$nodes[[uid]]
      rt <- n$resource_type
      if (isTRUE(rt == "model") || isTRUE(rt == "seed")) {
        node_info[[uid]] <- list(
          schema = tolower(n$schema),
          name   = tolower(n$name),
          deps   = unlist(n$depends_on$nodes)
        )
      }
    }
  }
  if (length(node_info) == 0) {
    return(data.frame(schema=character(), name=character(), rank=integer(),
                      stringsAsFactors = FALSE))
  }
  rank_of  <- new.env(hash = TRUE)
  visiting <- new.env(hash = TRUE)
  compute_rank <- function(uid) {
    cached <- rank_of[[uid]]
    if (!is.null(cached)) return(cached)
    if (!is.null(visiting[[uid]])) return(0L)
    visiting[[uid]] <- TRUE
    info <- node_info[[uid]]
    if (is.null(info) || length(info$deps) == 0) {
      r <- 0L
    } else {
      dep_ranks <- vapply(info$deps, function(d) {
        if (!is.null(node_info[[d]])) compute_rank(d) else 0L
      }, integer(1))
      r <- as.integer(max(dep_ranks)) + 1L
    }
    rm(list = uid, envir = visiting)
    rank_of[[uid]] <- r
    r
  }
  for (uid in names(node_info)) compute_rank(uid)
  data.frame(
    schema = vapply(node_info, `[[`, character(1), "schema"),
    name   = vapply(node_info, `[[`, character(1), "name"),
    rank   = vapply(names(node_info), function(u) rank_of[[u]], integer(1)),
    stringsAsFactors = FALSE
  )
}

LINEAGE_RANKS <- tryCatch(compute_lineage_ranks(),
  error = function(e) data.frame(schema=character(), name=character(),
                                 rank=integer(), stringsAsFactors = FALSE))

# Return sort order for parallel (schema, table) vectors: schema priority,
# then lineage rank, then alpha. Unknown schemas/tables go to the end.
lineage_order <- function(schemas, tables) {
  key <- data.frame(schema = tolower(schemas), name = tolower(tables),
                    stringsAsFactors = FALSE)
  key$idx <- seq_len(nrow(key))
  merged <- merge(key, LINEAGE_RANKS, by = c("schema","name"),
                  all.x = TRUE, sort = FALSE)
  merged <- merged[order(merged$idx), ]
  ranks <- merged$rank
  ranks[is.na(ranks)] <- .Machine$integer.max %/% 2L
  schema_prio <- match(merged$schema, SCHEMA_ORDER)
  schema_prio[is.na(schema_prio)] <- 100L
  order(schema_prio, ranks, merged$name)
}

# Custom CSS matching the returns_analyzer.html dark theme
custom_css <- "
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;600&family=Space+Grotesk:wght@500;600;700&display=swap');

body {
  background-color: #0f172a !important;
  background-image:
    radial-gradient(at 0% 0%, rgba(56, 189, 248, 0.15) 0px, transparent 50%),
    radial-gradient(at 100% 100%, rgba(239, 68, 68, 0.1) 0px, transparent 50%);
  color: #f8fafc !important;
  font-family: 'Inter', sans-serif !important;
  font-variant-numeric: tabular-nums;
  min-height: 100vh;
}

/* Tab strip: pin dark explicitly - the Bootstrap navbar otherwise follows
   the browser's light color scheme and renders a white bar. Selectors cover
   both BS3 (.navbar-default, li.active) and BS5 (.nav-link) variants. */
.navbar, .navbar-default, .navbar-static-top {
  background-color: rgba(15, 23, 42, 0.95) !important;
  border: none !important;
  box-shadow: 0 1px 0 rgba(255, 255, 255, 0.06);
}
.navbar .navbar-brand, .navbar-default .navbar-brand {
  color: #f8fafc !important;
  font-family: 'Space Grotesk', 'Inter', sans-serif !important;
  font-weight: 700;
  letter-spacing: -0.02em;
}
.navbar-nav > li > a, .navbar-nav .nav-link {
  color: #94a3b8 !important; background: transparent !important;
}
.navbar-nav > li > a:hover, .navbar-nav .nav-link:hover {
  color: #f8fafc !important;
}
.navbar-nav > .active > a, .navbar-nav > .active > a:hover,
.navbar-nav > .active > a:focus, .navbar-nav .nav-link.active {
  color: #f8fafc !important;
  background-color: rgba(56, 189, 248, 0.12) !important;
}

h2 {
  font-size: 1.5rem !important;
  font-weight: 700 !important;
  letter-spacing: -0.025em;
  background: linear-gradient(to right, #38bdf8, #818cf8);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  margin-bottom: 1.5rem !important;
}

h4 {
  font-size: 0.875rem !important;
  font-weight: 600 !important;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: #94a3b8 !important;
}

.well {
  background: rgba(30, 41, 59, 0.7) !important;
  border: 1px solid rgba(255, 255, 255, 0.1) !important;
  border-radius: 1rem !important;
  backdrop-filter: blur(12px);
  box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5) !important;
  color: #f8fafc !important;
}

.main-card {
  background: rgba(30, 41, 59, 0.7);
  border: 1px solid rgba(255, 255, 255, 0.1);
  border-radius: 1rem;
  padding: 1.5rem !important;
  backdrop-filter: blur(12px);
  box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
}

label, .control-label {
  color: #94a3b8 !important;
  font-size: 0.75rem !important;
  font-weight: 500 !important;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.form-control {
  background: rgba(15, 23, 42, 0.5) !important;
  border: 1px solid rgba(255, 255, 255, 0.1) !important;
  border-radius: 0.5rem !important;
  color: #f8fafc !important;
  font-family: 'Inter', monospace !important;
  font-size: 0.875rem !important;
  padding: 0.5rem 0.75rem !important;
  transition: all 0.2s;
}

.form-control:focus {
  border-color: #38bdf8 !important;
  box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.2) !important;
  outline: none !important;
}

textarea.form-control {
  font-family: 'JetBrains Mono', 'Fira Code', monospace !important;
  font-size: 0.8rem !important;
  line-height: 1.6;
}

.btn-primary, .btn-default.action-button {
  background: #38bdf8 !important;
  color: #000 !important;
  border: none !important;
  border-radius: 0.5rem !important;
  font-weight: 600 !important;
  padding: 0.75rem 1.5rem !important;
  cursor: pointer;
  transition: all 0.2s;
  font-family: 'Inter', sans-serif !important;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  font-size: 0.8rem !important;
}

.btn-primary:hover, .btn-default.action-button:hover {
  background: #7dd3fc !important;
  transform: translateY(-1px);
}

hr {
  border-color: rgba(255, 255, 255, 0.1) !important;
}

.help-block {
  color: #64748b !important;
  font-size: 0.8rem !important;
}

/* Select dropdown styling */
.selectize-input {
  background: rgba(15, 23, 42, 0.5) !important;
  border: 1px solid rgba(255, 255, 255, 0.1) !important;
  border-radius: 0.5rem !important;
  color: #f8fafc !important;
  font-family: 'Inter', sans-serif !important;
}

.selectize-input.focus {
  border-color: #38bdf8 !important;
  box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.2) !important;
}

.selectize-dropdown {
  background: #1e293b !important;
  border: 1px solid rgba(255, 255, 255, 0.1) !important;
  color: #f8fafc !important;
}

.selectize-dropdown .active {
  background: rgba(56, 189, 248, 0.2) !important;
  color: #f8fafc !important;
}

.selectize-dropdown-content .option {
  color: #f8fafc !important;
}

/* Caveat / read-me boxes above charts. note = neutral doc, info = explanation,
   warning = interpretation caveat the reader must not skip. */
.caveat-note, .caveat-info, .caveat-warning {
  padding: 0.5rem 0.75rem;
  background: rgba(255,255,255,0.03);
  border-radius: 4px;
  color: #94a3b8;
  font-size: 0.75rem;
  line-height: 1.4;
  margin-bottom: 0.75rem;
}
.caveat-note    { border-left: 2px solid #64748b; }
.caveat-info    { border-left: 2px solid #38bdf8; }
.caveat-warning { border-left: 2px solid #f59e0b; }

/* Portfolio + transition tables: readable body text. No !important on the color
   so the formatStyle-colored State/Current cells keep their inline green/amber/red. */
#lcPosTable table.dataTable tbody td,
#lcTransitionsTable table.dataTable tbody td { color: #e2e8f0; }
#lcPosTable table.dataTable thead th,
#lcTransitionsTable table.dataTable thead th { color: #94a3b8 !important; }
#lcPosTable table.dataTable tbody tr:hover td,
#lcTransitionsTable table.dataTable tbody tr:hover td { background: rgba(56,189,248,0.08) !important; }
#lcPosTable table.dataTable tbody tr.selected td { background: rgba(56,189,248,0.22) !important; }
/* row-select checkboxes: accent-tinted, centered, compact select column */
#lcPosTable td.pf-sel, #lcPosTable th.pf-sel { width: 30px; text-align: center; padding-left: 4px; padding-right: 4px; }
#lcPosTable input.pfrow, #lcPosTable input.pfall { accent-color: #38bdf8; cursor: pointer; width: 15px; height: 15px; vertical-align: middle; }
/* portfolio table (Kevin 2026-08-19): single-line compact rows so the Start date
   no longer wraps to two lines and every column is visible; horizontal scroll
   (scrollX) reaches any column that overflows a narrow window. */
#lcPosTable table.dataTable tbody td,
#lcPosTable table.dataTable thead th { white-space: nowrap; }
#lcPosTable table.dataTable.compact tbody td,
#lcPosTable table.dataTable.compact thead th { padding: 3px 7px; font-size: 0.8rem; }
#lcPosTable .dataTables_scrollBody { overflow-x: auto; }
#lcPosTable .dataTables_scrollBody::-webkit-scrollbar { height: 9px; }
#lcPosTable .dataTables_scrollBody::-webkit-scrollbar-thumb { background: #334155; border-radius: 4px; }
/* portfolio panel breaks out of the 8/12 main column to use the empty space to
   its left (the sidebar ends well above it), so the wide table shows every
   column at once. Wide screens only; narrow/mobile keeps it in the column. */
@media (min-width: 1200px) {
  /* pf-wide once broke the portfolio panel out over the sidebar with a
     hardcoded -22vw shift, which misrendered at other window sizes
     (2026-08-23). The panel now stays in the main column; the positions
     table scrolls horizontally on its own. */
  details.pf-wide { box-sizing: border-box; }
}

/* Global loading progress bar: an estimated-progress overlay shown whenever
   Shiny is busy (Connect, Generate, any heavy render), on every tab. Real query
   progress isn't reported, so the fill trickles toward ~90% over the expected
   load time, then snaps to 100% on idle. Toggled by the shiny:busy/idle handler. */
#global-spinner {
  position: fixed; inset: 0; z-index: 99999;
  display: none; flex-direction: column; align-items: center; justify-content: center;
  gap: 14px; background: rgba(2, 6, 23, 0.55);
}
#global-spinner.gs-on { display: flex; }
#gs-track {
  width: 320px; max-width: 60vw; height: 8px;
  background: rgba(148, 163, 184, 0.2); border-radius: 999px; overflow: hidden;
  box-shadow: 0 0 0 1px rgba(148, 163, 184, 0.15);
}
#gs-fill {
  height: 100%; width: 0%;
  background: linear-gradient(90deg, #38bdf8, #22d3ee);
  border-radius: 999px; transition: width 0.35s ease-out;
}
#gs-label {
  color: #cbd5e1; font-size: 0.8rem; font-family: 'Inter', sans-serif; letter-spacing: 0.02em;
}
"

