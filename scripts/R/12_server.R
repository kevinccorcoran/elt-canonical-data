# AlphaStream dashboard - Server function. Part of app.R, split 2026-10-19: files load in name order (see app.R).
# The server body lives in R/server/, one file per tab (Lifecycle in several).
# The parts are parsed once at startup and evaluated in order inside each
# session's server function: the same environment as when they were one body,
# so every tab still sees the shared reactives and helpers.
.server_parts <- lapply(sort(list.files(file.path(.app_dir, "R", "server"), pattern = "[.]R$",
                                        full.names = TRUE)), parse, keep.source = FALSE)
server <- function(input, output, session) {
  for (.part in .server_parts) for (.expr in .part) eval(.expr)
}
