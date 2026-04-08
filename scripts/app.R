library(shiny)
library(DBI)
library(RPostgres)
library(plotly)

# Custom CSS matching the returns_analyzer.html dark theme
custom_css <- "
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

body {
  background-color: #0f172a !important;
  background-image: 
    radial-gradient(at 0% 0%, rgba(56, 189, 248, 0.15) 0px, transparent 50%),
    radial-gradient(at 100% 100%, rgba(239, 68, 68, 0.1) 0px, transparent 50%);
  color: #f8fafc !important;
  font-family: 'Inter', sans-serif !important;
  min-height: 100vh;
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
"

# ─── Helper: sidebar panel for a given tab suffix ───
make_sidebar <- function(suffix, title, filter_widgets) {
  sidebarPanel(
    h4(title),
    selectInput(paste0("db_env", suffix), "Environment", choices = c("Production", "Staging", "Dev"), selected = "Dev"),
    textInput(paste0("db_host", suffix), "Host", value = "host.docker.internal"),
    textInput(paste0("db_port", suffix), "Port", value = "5432"),
    textInput(paste0("db_user", suffix), "User", value = "postgres"),
    passwordInput(paste0("db_pass", suffix), "Password", value = ""),
    actionButton(paste0("connect_btn", suffix), "Connect & Load Filters", class = "btn-primary"),
    hr(),
    h5("Filters"),
    div(id = paste0("filter_panel", suffix), filter_widgets),
    hr(),
    actionButton(paste0("execute_", suffix), "Generate Chart", class = "btn-primary w-100", style = "margin-top: 1rem;"),
    hr(),
    textOutput(paste0("statusMessage", suffix))
  )
}

# ─── Define UI ───
ui <- navbarPage(
  title = "Analysis Dashboard",
  tags$head(tags$style(HTML(custom_css))),

  # ── Tab 1: Past Decomposition ──
  tabPanel("Past Decomposition",
    sidebarLayout(
      make_sidebar("P", "Database Connection (Past)", tagList(
        selectInput("id_valP", "ID", choices = c("Connect first..." = ""), selected = ""),
        selectInput("fib_lagP", "Fibonacci Lag Value", choices = c("Connect first..." = ""), selected = "")
      )),
      mainPanel(div(class = "main-card",
        h4("return_cluster_past_bucket_stats", style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        plotlyOutput("pastPlot", height = "700px")
      ))
    )
  ),

  # ── Tab 2: Future Decomposition ──
  tabPanel("Future Decomposition",
    sidebarLayout(
      make_sidebar("F", "Database Connection (Future)", tagList(
        selectInput("id_valF", "ID", choices = c("Connect first..." = ""), selected = ""),
        selectInput("fib_lagF", "Fibonacci Lag Value", choices = c("Connect first..." = ""), selected = ""),
        selectInput("future_fib_lagF", "Future Fibonacci Lag", choices = c("Connect first..." = ""), selected = "")
      )),
      mainPanel(div(class = "main-card",
        h4("return_cluster_future_bucket_stats", style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        plotlyOutput("futurePlot", height = "700px")
      ))
    )
  ),

  # ── Tab 3: Combined Decomposition ──
  tabPanel("Combined Decomposition",
    sidebarLayout(
      make_sidebar("C", "Database Connection (Combined)", tagList(
        selectInput("id_valC", "ID", choices = c("Connect first..." = ""), selected = ""),
        selectInput("past_fib_lagC", "Fibonacci Lag Value", choices = c("Connect first..." = ""), selected = ""),
        selectInput("future_fib_lagC", "Future Fibonacci Lag", choices = c("Connect first..." = ""), selected = "")
      )),
      mainPanel(div(class = "main-card",
        h4("Past vs Future Bucket Stats", style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        plotlyOutput("combinedPlot", height = "700px")
      ))
    )
  ),

  # ── Tab 4: Transition Range ──
  tabPanel("Transition Range",
    sidebarLayout(
      make_sidebar("T", "Database Connection (Transition)", tagList(
        selectInput("id_valT", "ID", choices = c("Connect first..." = ""), selected = ""),
        selectInput("past_fib_lagT", "Fibonacci Lag Value", choices = c("Connect first..." = ""), selected = ""),
        selectInput("future_fib_lagT", "Future Fibonacci Lag", choices = c("Connect first..." = ""), selected = "")
      )),
      mainPanel(div(class = "main-card",
        h4("Past Boxplot + Future Min/Max Range", style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        plotlyOutput("transitionPlot", height = "700px")
      ))
    )
  )
)

# ─── Helper: create a DB connection ───
get_con <- function(input, suffix) {
  env   <- input[[paste0("db_env", suffix)]]
  db_string <- if (env == "Production") "prod" else if (env == "Staging") "staging" else "dev"
  dbConnect(RPostgres::Postgres(),
    dbname   = db_string,
    host     = input[[paste0("db_host", suffix)]],
    port     = as.integer(input[[paste0("db_port", suffix)]]),
    user     = input[[paste0("db_user", suffix)]],
    password = input[[paste0("db_pass", suffix)]],
    sslmode  = "prefer"
  )
}

# ─── Helper: wire up env-switcher for a given suffix ───
setup_env_switcher <- function(input, session, suffix) {
  observeEvent(input[[paste0("db_env", suffix)]], {
    env <- input[[paste0("db_env", suffix)]]
    if (env == "Production") {
      updateTextInput(session, paste0("db_host", suffix), value = "dbaas-db-4718169-do-user-32264340-0.l.db.ondigitalocean.com")
      updateTextInput(session, paste0("db_port", suffix), value = "25060")
      updateTextInput(session, paste0("db_user", suffix), value = "doadmin")
      updateTextInput(session, paste0("db_pass", suffix), value = Sys.getenv("PROD_DB_PASSWORD", ""))
    } else {
      updateTextInput(session, paste0("db_host", suffix), value = "host.docker.internal")
      updateTextInput(session, paste0("db_port", suffix), value = "5432")
      updateTextInput(session, paste0("db_user", suffix), value = "postgres")
      updateTextInput(session, paste0("db_pass", suffix), value = Sys.getenv("DB_PASSWORD", ""))
    }
  })
}

# ─── Helper: standard boxplot + pct line ───
render_single_boxplot <- function(df, title, x_title, box_color = '#a855f7', fill_color = 'rgba(167, 139, 250, 0.4)') {
  df$pct <- (df$count / sum(df$count, na.rm = TRUE)) * 100

  fig <- plot_ly(df)

  fig <- fig %>% add_trace(
    type = 'box', name = 'Return Distribution',
    x = ~as.factor(bucket), q1 = ~q1, median = ~med, q3 = ~q3,
    lowerfence = ~lo, upperfence = ~hi,
    marker = list(color = box_color), line = list(color = box_color, width = 2),
    fillcolor = fill_color, hoverinfo = "y", offsetgroup = '1'
  )

  fig <- fig %>% add_markers(
    x = ~as.factor(bucket), y = ~med, name = 'Median',
    marker = list(color = '#ffffff', symbol = "line-ew", size = 45, line = list(color='#ffffff', width=3)),
    hoverinfo = "skip", showlegend = FALSE, offsetgroup = '1'
  )

  fig <- fig %>% add_trace(
    x = ~as.factor(bucket), y = ~pct, type = 'scatter', mode = 'lines+markers',
    fill = 'tozeroy', yaxis = 'y2', name = 'Record %',
    line = list(color = '#fbbf24', width = 3), marker = list(color = '#fbbf24', size = 8),
    fillcolor = 'rgba(251, 191, 36, 0.15)',
    hovertemplate = "Bucket: %{x}<br>Percentage: %{y:.2f}%<extra></extra>"
  )

  max_pct <- max(df$pct, na.rm = TRUE)
  fig %>% layout(
    title = list(text = title, font = list(color = "#f8fafc", family = "Inter", size = 18)),
    paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)", barmode = "group",
    xaxis = list(title = x_title, color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)", zerolinecolor = "rgba(255,255,255,0.1)"),
    yaxis = list(title = "Excess Return vs SPY (%)", color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)", zeroline = TRUE, zerolinewidth = 2, zerolinecolor = "rgba(255,255,255,0.2)"),
    yaxis2 = list(title = "Record Percentage (%)", color = "#fbbf24", gridcolor = "transparent", overlaying = "y", side = "right",
                  range = c(0, ifelse(is.infinite(max_pct) || is.na(max_pct), 100, max_pct * 1.5))),
    margin = list(l = 50, r = 60, b = 50, t = 50),
    showlegend = TRUE, legend = list(font = list(color = "#f8fafc"), orientation = "h", y = -0.2)
  )
}

# ─── Server ───
server <- function(input, output, session) {

  # Reactive values
  app_dataP <- reactiveVal(NULL)
  status_msgP <- reactiveVal("Ready")
  app_dataF <- reactiveVal(NULL)
  status_msgF <- reactiveVal("Ready")
  app_dataC <- reactiveVal(NULL)
  status_msgC <- reactiveVal("Ready")

  output$statusMessageP <- renderText({ status_msgP() })
  output$statusMessageF <- renderText({ status_msgF() })
  output$statusMessageC <- renderText({ status_msgC() })

  # Env switchers
  setup_env_switcher(input, session, "P")
  setup_env_switcher(input, session, "F")
  setup_env_switcher(input, session, "C")

  # ── PAST: Connect ──
  observeEvent(input$connect_btnP, {
    if (input$db_passP == "") { status_msgP("Error: Password is not set."); return() }
    status_msgP("Connecting...")
    tryCatch({
      con <- get_con(input, "P")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      id_vals  <- dbGetQuery(con, "SELECT DISTINCT id FROM inference.return_cluster_past_bucket_stats ORDER BY 1")
      fib_vals <- dbGetQuery(con, "SELECT DISTINCT fibonacci_lag_value FROM inference.return_cluster_past_bucket_stats ORDER BY 1")
      updateSelectInput(session, "id_valP",  choices = id_vals[[1]],  selected = id_vals[[1]][1])
      updateSelectInput(session, "fib_lagP", choices = fib_vals[[1]], selected = fib_vals[[1]][1])
      status_msgP("Filters loaded!")
    }, error = function(e) { status_msgP(paste("Error:", e$message)) })
  })

  # ── PAST: Execute ──
  observeEvent(input$execute_P, {
    if (input$db_passP == "") { status_msgP("Error: Password is not set."); return() }
    if (input$fib_lagP == "" || input$id_valP == "") { status_msgP("Error: Select filters first."); return() }
    status_msgP("Running query...")
    query <- sprintf("
      SELECT past_excess_return_z_bucket_num AS bucket,
             SUM(record_count_in_bucket) AS count,
             MIN(min_past_excess_return_vs_spy) AS lo,
             SUM(past_q1_return * record_count_in_bucket) / NULLIF(SUM(record_count_in_bucket), 0) AS q1,
             SUM(past_median_return * record_count_in_bucket) / NULLIF(SUM(record_count_in_bucket), 0) AS med,
             SUM(past_q3_return * record_count_in_bucket) / NULLIF(SUM(record_count_in_bucket), 0) AS q3,
             MAX(max_past_excess_return_vs_spy) AS hi
      FROM inference.return_cluster_past_bucket_stats
      WHERE fibonacci_lag_value = %s AND id = %s
      GROUP BY past_excess_return_z_bucket_num
      ORDER BY past_excess_return_z_bucket_num;",
      input$fib_lagP, input$id_valP)
    tryCatch({
      con <- get_con(input, "P")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      res <- dbGetQuery(con, query)
      for(col in names(res)) res[[col]] <- as.numeric(res[[col]])
      app_dataP(res)
      status_msgP(paste("Loaded", nrow(res), "rows."))
    }, error = function(e) { status_msgP(paste("Error:", e$message)) })
  })

  # ── PAST: Render ──
  output$pastPlot <- renderPlotly({
    req(app_dataP())
    df <- app_dataP()
    if (nrow(df) == 0) return(plot_ly() %>% layout(title = list(text = "No data found", font = list(color="#f8fafc")), paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))
    render_single_boxplot(df, "Past Return Distribution by Alpha Z-Bucket", "Past Excess Return Z-Bucket (SD)", '#a855f7', 'rgba(167, 139, 250, 0.4)')
  })

  # ── FUTURE: Connect ──
  observeEvent(input$connect_btnF, {
    if (input$db_passF == "") { status_msgF("Error: Password is not set."); return() }
    status_msgF("Connecting...")
    tryCatch({
      con <- get_con(input, "F")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      id_vals  <- dbGetQuery(con, "SELECT DISTINCT id FROM inference.return_cluster_future_bucket_stats ORDER BY 1")
      fib_vals <- dbGetQuery(con, "SELECT DISTINCT fibonacci_lag_value FROM inference.return_cluster_future_bucket_stats ORDER BY 1")
      future_fib_vals <- dbGetQuery(con, "SELECT DISTINCT future_fibonacci_lag_value FROM inference.return_cluster_future_bucket_stats ORDER BY 1")
      updateSelectInput(session, "id_valF",  choices = id_vals[[1]],  selected = id_vals[[1]][1])
      updateSelectInput(session, "fib_lagF", choices = fib_vals[[1]], selected = fib_vals[[1]][1])
      updateSelectInput(session, "future_fib_lagF", choices = future_fib_vals[[1]], selected = future_fib_vals[[1]][1])
      status_msgF("Filters loaded!")
    }, error = function(e) { status_msgF(paste("Error:", e$message)) })
  })

  # ── FUTURE: Execute ──
  observeEvent(input$execute_F, {
    if (input$db_passF == "") { status_msgF("Error: Password is not set."); return() }
    if (input$fib_lagF == "" || input$id_valF == "" || input$future_fib_lagF == "") { status_msgF("Error: Select filters first."); return() }
    status_msgF("Running query...")
    query <- sprintf("
      SELECT future_excess_return_z_bucket_num AS bucket,
             SUM(record_count_in_bucket) AS count,
             MIN(min_future_excess_return_vs_spy) AS lo,
             SUM(future_q1_return * record_count_in_bucket) / NULLIF(SUM(record_count_in_bucket), 0) AS q1,
             SUM(future_median_return * record_count_in_bucket) / NULLIF(SUM(record_count_in_bucket), 0) AS med,
             SUM(future_q3_return * record_count_in_bucket) / NULLIF(SUM(record_count_in_bucket), 0) AS q3,
             MAX(max_future_excess_return_vs_spy) AS hi
      FROM inference.return_cluster_future_bucket_stats
      WHERE fibonacci_lag_value = %s AND future_fibonacci_lag_value = %s AND id = %s
      GROUP BY future_excess_return_z_bucket_num
      ORDER BY future_excess_return_z_bucket_num;",
      input$fib_lagF, input$future_fib_lagF, input$id_valF)
    tryCatch({
      con <- get_con(input, "F")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      res <- dbGetQuery(con, query)
      for(col in names(res)) res[[col]] <- as.numeric(res[[col]])
      app_dataF(res)
      status_msgF(paste("Loaded", nrow(res), "rows."))
    }, error = function(e) { status_msgF(paste("Error:", e$message)) })
  })

  # ── FUTURE: Render ──
  output$futurePlot <- renderPlotly({
    req(app_dataF())
    df <- app_dataF()
    if (nrow(df) == 0) return(plot_ly() %>% layout(title = list(text = "No data found", font = list(color="#f8fafc")), paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))
    render_single_boxplot(df, "Future Return Distribution by Alpha Z-Bucket", "Future Excess Return Z-Bucket (SD)", '#0ea5e9', 'rgba(14, 165, 233, 0.4)')
  })

  # ── COMBINED: Connect ──
  observeEvent(input$connect_btnC, {
    if (input$db_passC == "") { status_msgC("Error: Password is not set."); return() }
    status_msgC("Connecting...")
    tryCatch({
      con <- get_con(input, "C")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      id_vals       <- dbGetQuery(con, "SELECT DISTINCT id FROM inference.return_cluster_past_bucket_stats ORDER BY 1")
      past_fib_vals <- dbGetQuery(con, "SELECT DISTINCT fibonacci_lag_value FROM inference.return_cluster_past_bucket_stats ORDER BY 1")
      future_fib_vals <- dbGetQuery(con, "SELECT DISTINCT future_fibonacci_lag_value FROM inference.return_cluster_future_bucket_stats ORDER BY 1")
      updateSelectInput(session, "id_valC", choices = id_vals[[1]], selected = id_vals[[1]][1])
      updateSelectInput(session, "past_fib_lagC", choices = past_fib_vals[[1]], selected = past_fib_vals[[1]][1])
      updateSelectInput(session, "future_fib_lagC", choices = future_fib_vals[[1]], selected = future_fib_vals[[1]][1])
      status_msgC("Filters loaded!")
    }, error = function(e) { status_msgC(paste("Error:", e$message)) })
  })

  # ── COMBINED: Execute ──
  observeEvent(input$execute_C, {
    if (input$db_passC == "") { status_msgC("Error: Password is not set."); return() }
    if (input$id_valC == "" || input$past_fib_lagC == "" || input$future_fib_lagC == "") {
      status_msgC("Error: Select filters first."); return()
    }
    status_msgC("Running query...")
    query <- sprintf("
      SELECT past_excess_return_z_bucket_num AS bucket,
             MAX(past_record_count) AS past_count,
             MIN(past_min) AS past_lo,
             SUM(past_q1 * past_record_count) / NULLIF(SUM(past_record_count), 0) AS past_q1,
             SUM(past_median * past_record_count) / NULLIF(SUM(past_record_count), 0) AS past_med,
             SUM(past_q3 * past_record_count) / NULLIF(SUM(past_record_count), 0) AS past_q3,
             MAX(past_max) AS past_hi,
             SUM(future_record_count) AS future_count,
             MIN(future_min) AS future_lo,
             SUM(future_q1 * future_record_count) / NULLIF(SUM(future_record_count), 0) AS future_q1,
             SUM(future_median * future_record_count) / NULLIF(SUM(future_record_count), 0) AS future_med,
             SUM(future_q3 * future_record_count) / NULLIF(SUM(future_record_count), 0) AS future_q3,
             MAX(future_max) AS future_hi
      FROM inference.return_cluster_combined_bucket_stats
      WHERE past_fibonacci_lag_value = %s AND future_fibonacci_lag_value = %s AND id = %s
      GROUP BY past_excess_return_z_bucket_num
      ORDER BY past_excess_return_z_bucket_num;",
      input$past_fib_lagC, input$future_fib_lagC, input$id_valC)
    tryCatch({
      con <- get_con(input, "C")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      res <- dbGetQuery(con, query)
      for(col in names(res)) res[[col]] <- as.numeric(res[[col]])
      app_dataC(res)
      status_msgC(paste("Loaded", nrow(res), "rows."))
    }, error = function(e) { status_msgC(paste("Error:", e$message)) })
  })

  # ── COMBINED: Render ──
  output$combinedPlot <- renderPlotly({
    req(app_dataC())
    df <- app_dataC()
    if (nrow(df) == 0) return(plot_ly() %>% layout(title = list(text = "No data found", font = list(color="#f8fafc")), paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))

    tot_past <- sum(df$past_count, na.rm = TRUE)
    tot_future <- sum(df$future_count, na.rm = TRUE)
    df$past_pct  <- df$past_count * 100
    df$future_pct <- if(tot_future > 0) (df$future_count / tot_future) * 100 else 0

    fig <- plot_ly(df)

    # Past box (purple)
    fig <- fig %>% add_trace(type='box', name='Past Return Distribution', x=~as.factor(bucket),
      q1=~past_q1, median=~past_med, q3=~past_q3, lowerfence=~past_lo, upperfence=~past_hi,
      marker=list(color='#a855f7'), line=list(color='#a855f7', width=2),
      fillcolor='rgba(167,139,250,0.4)', hoverinfo="y", offsetgroup='1')
    fig <- fig %>% add_markers(x=~as.factor(bucket), y=~past_med, name='Past Median',
      marker=list(color='#ffffff', symbol="line-ew", size=25, line=list(color='#ffffff', width=3)),
      hoverinfo="skip", showlegend=FALSE, offsetgroup='1')

    # Future box (sky blue)
    fig <- fig %>% add_trace(type='box', name='Future Return Distribution', x=~as.factor(bucket),
      q1=~future_q1, median=~future_med, q3=~future_q3, lowerfence=~future_lo, upperfence=~future_hi,
      marker=list(color='#0ea5e9'), line=list(color='#0ea5e9', width=2),
      fillcolor='rgba(14,165,233,0.4)', hoverinfo="y", offsetgroup='2')
    fig <- fig %>% add_markers(x=~as.factor(bucket), y=~future_med, name='Future Median',
      marker=list(color='#ffffff', symbol="line-ew", size=25, line=list(color='#ffffff', width=3)),
      hoverinfo="skip", showlegend=FALSE, offsetgroup='2')

    # Past % (yellow dashed)
    fig <- fig %>% add_trace(x=~as.factor(bucket), y=~past_pct, type='scatter', mode='lines+markers',
      name='Past % of Records', yaxis='y2', line=list(color='#fbbf24', width=2, dash='dot'),
      marker=list(color='#fbbf24', size=6), hovertemplate="Bucket: %{x}<br>Past %%: %{y:.2f}%<extra></extra>")

    # Future % (green solid)
    fig <- fig %>% add_trace(x=~as.factor(bucket), y=~future_pct, type='scatter', mode='lines+markers',
      name='Future % of Records', yaxis='y2', line=list(color='#34d399', width=3),
      marker=list(color='#34d399', size=8), hovertemplate="Bucket: %{x}<br>Future %%: %{y:.2f}%<extra></extra>")

    max_pct <- max(c(df$past_pct, df$future_pct), na.rm = TRUE)

    fig %>% layout(
      title = list(text = "Past vs. Future Expected Returns by Alpha Z-Bucket", font = list(color = "#f8fafc", family = "Inter", size = 18)),
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)", barmode = "group", boxmode = "group",
      xaxis = list(title = "Excess Return Z-Bucket (SD)", color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)", zerolinecolor = "rgba(255,255,255,0.1)"),
      yaxis = list(title = "Excess Return vs SPY (%)", color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)", zeroline = TRUE, zerolinewidth = 2, zerolinecolor = "rgba(255,255,255,0.2)"),
      yaxis2 = list(title = "Record Percentage (%)", color = "#f8fafc", gridcolor = "transparent", overlaying = "y", side = "right",
                    range = c(0, ifelse(is.infinite(max_pct) || is.na(max_pct), 100, max_pct * 1.5))),
      margin = list(l = 50, r = 60, b = 50, t = 50),
      showlegend = TRUE, legend = list(font = list(color = "#f8fafc"), orientation = "h", y = -0.2)
    )
  })

  # ── TRANSITION: Reactive values ──
  app_dataT <- reactiveVal(NULL)
  status_msgT <- reactiveVal("Ready")
  output$statusMessageT <- renderText({ status_msgT() })
  setup_env_switcher(input, session, "T")

  # ── TRANSITION: Connect ──
  observeEvent(input$connect_btnT, {
    if (input$db_passT == "") { status_msgT("Error: Password is not set."); return() }
    status_msgT("Connecting...")
    tryCatch({
      con <- get_con(input, "T")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      id_vals       <- dbGetQuery(con, "SELECT DISTINCT id FROM inference.return_cluster_past_bucket_stats ORDER BY 1")
      past_fib_vals <- dbGetQuery(con, "SELECT DISTINCT fibonacci_lag_value FROM inference.return_cluster_past_bucket_stats ORDER BY 1")
      future_fib_vals <- dbGetQuery(con, "SELECT DISTINCT future_fibonacci_lag_value FROM inference.return_cluster_future_bucket_stats ORDER BY 1")
      updateSelectInput(session, "id_valT", choices = id_vals[[1]], selected = id_vals[[1]][1])
      updateSelectInput(session, "past_fib_lagT", choices = past_fib_vals[[1]], selected = past_fib_vals[[1]][1])
      updateSelectInput(session, "future_fib_lagT", choices = future_fib_vals[[1]], selected = future_fib_vals[[1]][1])
      status_msgT("Filters loaded!")
    }, error = function(e) { status_msgT(paste("Error:", e$message)) })
  })

  # ── TRANSITION: Execute ──
  observeEvent(input$execute_T, {
    if (input$db_passT == "") { status_msgT("Error: Password is not set."); return() }
    if (input$id_valT == "" || input$past_fib_lagT == "" || input$future_fib_lagT == "") {
      status_msgT("Error: Select filters first."); return()
    }
    status_msgT("Running query...")
    query <- sprintf("
      SELECT past_excess_return_z_bucket_num AS bucket,
             MAX(past_record_count) AS past_count,
             MIN(past_min) AS past_lo,
             SUM(past_q1 * past_record_count) / NULLIF(SUM(past_record_count), 0) AS past_q1,
             SUM(past_median * past_record_count) / NULLIF(SUM(past_record_count), 0) AS past_med,
             SUM(past_q3 * past_record_count) / NULLIF(SUM(past_record_count), 0) AS past_q3,
             MAX(past_max) AS past_hi,
             SUM(future_record_count) AS future_count,
             MIN(future_min) AS future_lo,
             SUM(future_median * future_record_count) / NULLIF(SUM(future_record_count), 0) AS future_med,
             MAX(future_max) AS future_hi
      FROM inference.return_cluster_combined_bucket_stats
      WHERE past_fibonacci_lag_value = %s AND future_fibonacci_lag_value = %s AND id = %s
      GROUP BY past_excess_return_z_bucket_num
      ORDER BY past_excess_return_z_bucket_num;",
      input$past_fib_lagT, input$future_fib_lagT, input$id_valT)
    tryCatch({
      con <- get_con(input, "T")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      res <- dbGetQuery(con, query)
      for(col in names(res)) res[[col]] <- as.numeric(res[[col]])
      app_dataT(res)
      status_msgT(paste("Loaded", nrow(res), "rows."))
    }, error = function(e) { status_msgT(paste("Error:", e$message)) })
  })

  # ── TRANSITION: Render ──
  output$transitionPlot <- renderPlotly({
    req(app_dataT())
    df <- app_dataT()
    if (nrow(df) == 0) return(plot_ly() %>% layout(title = list(text = "No data found", font = list(color="#f8fafc")), paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))

    tot_past <- sum(df$past_count, na.rm = TRUE)
    tot_future <- sum(df$future_count, na.rm = TRUE)
    df$past_pct  <- df$past_count * 100
    df$future_pct <- if(tot_future > 0) (df$future_count / tot_future) * 100 else 0

    # Confidence score: Conservative Expected Future - past_med
    # Weights: 60% Median, 30% Min (Downside Risk), 10% Max (Upside Potential)
    df$conf_score <- ((0.60 * df$future_med) + (0.30 * df$future_lo) + (0.10 * df$future_hi)) - df$past_med
    # Build custom hover tooltips
    df$past_hover <- sprintf(
      "<b>Past Distribution</b><br>Max: %.2f%%<br>Q3: %.2f%%<br>Median: %.2f%%<br>Q1: %.2f%%<br>Min: %.2f%%<br>Records: %d",
      df$past_hi, df$past_q3, df$past_med, df$past_q1, df$past_lo, df$past_count)

    df$future_hover <- sprintf(
      "<b>Future Range</b><br>Max: %.2f%%<br>Median: %.2f%%<br>Min: %.2f%%<br>Records: %d",
      df$future_hi, df$future_med, df$future_lo, df$future_count)

    df$conf_color <- ifelse(df$conf_score >= 0, '#34d399', '#f87171')

    fig <- plot_ly(df)

    # Past boxplot (purple)
    fig <- fig %>% add_trace(type='box', name='Past Return Distribution', x=~as.factor(bucket),
      q1=~past_q1, median=~past_med, q3=~past_q3, lowerfence=~past_lo, upperfence=~past_hi,
      marker=list(color='#a855f7'), line=list(color='#a855f7', width=2),
      fillcolor='rgba(167,139,250,0.4)', text=~past_hover, hovertemplate="%{text}<extra></extra>", offsetgroup='1')
    fig <- fig %>% add_markers(x=~as.factor(bucket), y=~past_med, name='Past Median',
      marker=list(color='#ffffff', symbol="line-ew", size=25, line=list(color='#ffffff', width=3)),
      hoverinfo="skip", showlegend=FALSE, offsetgroup='1')

    # Future range (sky blue) — box body spans min to max
    fig <- fig %>% add_trace(type='box', name='Future Range (Min to Max)', x=~as.factor(bucket),
      q1=~future_lo, median=~future_med, q3=~future_hi,
      lowerfence=~future_lo, upperfence=~future_hi,
      marker=list(color='#0ea5e9'), line=list(color='#0ea5e9', width=2),
      fillcolor='rgba(14,165,233,0.4)', text=~future_hover, hovertemplate="%{text}<extra></extra>", offsetgroup='2')
    fig <- fig %>% add_markers(x=~as.factor(bucket), y=~future_med, name='Future Median',
      marker=list(color='#ffffff', symbol="line-ew", size=25, line=list(color='#ffffff', width=3)),
      hoverinfo="skip", showlegend=FALSE, offsetgroup='2')

    # Past % (yellow dashed)
    fig <- fig %>% add_trace(x=~as.factor(bucket), y=~past_pct, type='scatter', mode='lines+markers',
      name='Past % of Records', yaxis='y2', line=list(color='#fbbf24', width=2, dash='dot'),
      marker=list(color='#fbbf24', size=6), hovertemplate="<b>Bucket: %{x}</b><br>Past Records: %{y:.2f}%<extra></extra>")

    # Future % (green solid)
    fig <- fig %>% add_trace(x=~as.factor(bucket), y=~future_pct, type='scatter', mode='lines+markers',
      name='Future % of Records', yaxis='y2', line=list(color='#34d399', width=3),
      marker=list(color='#34d399', size=8), hovertemplate="<b>Bucket: %{x}</b><br>Future Records: %{y:.2f}%<extra></extra>")

    max_pct <- max(c(df$past_pct, df$future_pct), na.rm = TRUE)

    # Build score annotations evenly spaced across plot area
    n_buckets <- nrow(df)
    # Evenly space from ~10% to ~90% of plot width to avoid axis labels
    score_annotations <- lapply(seq_len(n_buckets), function(i) {
      x_pos <- 0.05 + (i - 1) * (0.90 / max(n_buckets - 1, 1))
      list(
        x = x_pos,
        y = 1.04,
        text = sprintf("<b>%s: %.2f</b>", df$bucket[i], df$conf_score[i]),
        font = list(color = df$conf_color[i], size = 12, family = "Inter"),
        showarrow = FALSE, xref = "paper", yref = "paper", xanchor = "center"
      )
    })
    # Add a label for the score row
    score_label <- list(
      x = 0.5, y = 1.08,
      text = "<b>Confidence Score</b>",
      font = list(color = "#94a3b8", size = 11, family = "Inter"),
      showarrow = FALSE, xref = "paper", yref = "paper", xanchor = "center"
    )
    all_annotations <- c(list(score_label), score_annotations)

    fig %>% layout(
      title = list(text = "Past Distribution vs Future Return Range", font = list(color = "#f8fafc", family = "Inter", size = 18)),
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)", barmode = "group", boxmode = "group",
      xaxis = list(title = "Past Excess Return Z-Bucket (SD)", color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)", zerolinecolor = "rgba(255,255,255,0.1)"),
      yaxis = list(title = "Excess Return vs SPY (%)", color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)", zeroline = TRUE, zerolinewidth = 2, zerolinecolor = "rgba(255,255,255,0.2)"),
      yaxis2 = list(title = "Record Percentage (%)", color = "#f8fafc", gridcolor = "transparent", overlaying = "y", side = "right",
                    range = c(0, ifelse(is.infinite(max_pct) || is.na(max_pct), 100, max_pct * 1.5))),
      annotations = all_annotations,
      margin = list(l = 50, r = 60, b = 50, t = 90),
      showlegend = TRUE, legend = list(font = list(color = "#f8fafc"), orientation = "h", y = -0.2)
    )
  })
}

# Run the application 
runApp(list(ui = ui, server = server), host="0.0.0.0", port=3838)
