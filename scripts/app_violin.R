# RETIRED (2026-08-05): this standalone violin dashboard (port 3839) is no longer
# launched by scripts/shiny_entrypoint.sh and 3839 is no longer published in
# docker/airflow/docker-compose.yml. Its views were folded into scripts/app.R.
# Kept for reference only; do not wire it back into the entrypoint without also
# republishing the port.

library(shiny)
library(DBI)
library(RPostgres)
library(plotly)

# Default query string based on the existing python script
default_query <- "SELECT 
    past_excess_return_z_bucket_num as bucket,
    record_pct_per_id as pct,
    median_future_excess_return_vs_spy as returns
FROM inference.return_expectation_decomposition
WHERE fibonacci_lag_value = 4 
    AND future_fibonacci_lag_value = 7
    AND id = 4
ORDER BY past_excess_return_z_bucket_num;"

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

#statusMessage {
  color: #10b981 !important;
  font-weight: 500;
  font-size: 0.875rem;
}

/* Data table styling */
.dataTables_wrapper {
  color: #94a3b8 !important;
}

table.dataTable {
  border-collapse: collapse !important;
}

table.dataTable thead th {
  background: rgba(15, 23, 42, 0.5) !important;
  color: #94a3b8 !important;
  border-bottom: 1px solid rgba(255, 255, 255, 0.1) !important;
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  padding: 0.75rem !important;
}

table.dataTable tbody td {
  background: transparent !important;
  color: #f8fafc !important;
  border-bottom: 1px solid rgba(255, 255, 255, 0.05) !important;
  padding: 0.5rem 0.75rem !important;
}

table.dataTable tbody tr:hover td {
  background: rgba(56, 189, 248, 0.05) !important;
}

.dataTables_info, .dataTables_length, .dataTables_filter {
  color: #64748b !important;
}

.dataTables_filter input {
  background: rgba(15, 23, 42, 0.5) !important;
  border: 1px solid rgba(255, 255, 255, 0.1) !important;
  border-radius: 0.5rem !important;
  color: #f8fafc !important;
  padding: 0.25rem 0.5rem !important;
}

.paginate_button {
  color: #94a3b8 !important;
}

.paginate_button.current {
  background: #38bdf8 !important;
  color: #000 !important;
  border-radius: 0.25rem !important;
}

/* Plot background */
.shiny-plot-output {
  border-radius: 0.75rem;
  overflow: hidden;
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

#connect_btn {
  background: rgba(16, 185, 129, 0.8) !important;
  margin-bottom: 1rem;
}

#connect_btn:hover {
  background: rgba(16, 185, 129, 1) !important;
}

.filter-group {
  opacity: 0.4;
  pointer-events: none;
}

.filter-group.active {
  opacity: 1;
  pointer-events: auto;
}
"

# Define UI
ui <- fluidPage(
  tags$head(
    tags$style(HTML(custom_css))
  ),
  titlePanel(NULL),
  tags$h2("Returns Analyzer"),
  
  sidebarLayout(
    sidebarPanel(
      helpText("Connect to the database, then select filter values from the dropdowns."),
      
      h4("Database Connection"),
      textInput("db_host", "Host", value = Sys.getenv("PROD_DB_HOST", "")),
      textInput("db_port", "Port", value = Sys.getenv("PROD_DB_PORT", "25060")),
      textInput("db_user", "User", value = Sys.getenv("PROD_DB_USER", "")),
      passwordInput("db_pass", "Password", value = Sys.getenv("PROD_DB_PASSWORD", "")),
      actionButton("connect_btn", "Connect & Load Filters", class = "btn-primary"),
      
      hr(),
      h4("Filters"),
      div(id = "filter_panel",
        selectInput("fib_lag", "Fibonacci Lag Value", choices = c("Connect first..." = ""), selected = ""),
        selectInput("future_fib_lag", "Future Fibonacci Lag Value", choices = c("Connect first..." = ""), selected = ""),
        selectInput("id_val", "ID", choices = c("Connect first..." = ""), selected = "")
      ),
      
      hr(),
      actionButton("execute", "Execute Query", class = "btn-primary"),
      hr(),
      textOutput("statusMessage")
    ),
    
    mainPanel(
      div(class = "main-card",
        plotlyOutput("returnsPlot", height = "500px"),
        br(),
        dataTableOutput("dataTable")
      )
    )
  )
)

# Helper to create a DB connection from UI inputs
get_con <- function(input) {
  dbConnect(RPostgres::Postgres(),
            dbname = "prod",
            host = input$db_host,
            port = as.integer(input$db_port),
            user = input$db_user,
            password = input$db_pass,
            sslmode = "require")
}

# Define server logic
server <- function(input, output, session) {
  
  # Reactive value to store the parsed dataframe
  app_data <- reactiveVal(NULL)
  status_msg <- reactiveVal("Ready — click Connect & Load Filters.")
  
  # Connect and populate dropdowns
  observeEvent(input$connect_btn, {
    if (input$db_pass == "") {
      status_msg("Error: Password is not set.")
      return()
    }
    
    status_msg("Connecting and loading filter values...")
    
    tryCatch({
      con <- get_con(input)
      
      fib_vals <- dbGetQuery(con, "SELECT DISTINCT fibonacci_lag_value FROM inference.return_expectation_decomposition ORDER BY 1")
      future_fib_vals <- dbGetQuery(con, "SELECT DISTINCT future_fibonacci_lag_value FROM inference.return_expectation_decomposition ORDER BY 1")
      id_vals <- dbGetQuery(con, "SELECT DISTINCT id FROM inference.return_expectation_decomposition ORDER BY 1")
      
      dbDisconnect(con)
      
      updateSelectInput(session, "fib_lag", choices = fib_vals[[1]], selected = 4)
      updateSelectInput(session, "future_fib_lag", choices = future_fib_vals[[1]], selected = 7)
      updateSelectInput(session, "id_val", choices = id_vals[[1]], selected = 4)
      
      # Enable filter panel via JS
      session$sendCustomMessage("enableFilters", TRUE)
      
      status_msg(paste("Connected! Loaded", nrow(fib_vals), "fib lags,", nrow(future_fib_vals), "future fib lags,", nrow(id_vals), "IDs."))
      
    }, error = function(e) {
      status_msg(paste("Connection Error:", e$message))
    })
  })
  
  # Execute query using dropdown selections
  observeEvent(input$execute, {
    if (input$db_pass == "") {
      status_msg("Error: Password is not set.")
      return()
    }
    if (input$fib_lag == "" || input$future_fib_lag == "" || input$id_val == "") {
      status_msg("Error: Please connect and select filter values first.")
      return()
    }
    
    status_msg("Running query...")
    
    query <- sprintf(
      "SELECT 
          past_excess_return_z_bucket_num as bucket,
          record_pct_per_id as pct,
          median_future_excess_return_vs_spy as returns,
          p05_past_excess_return_vs_spy as past_p05,
          p95_past_excess_return_vs_spy as past_p95,
          p05_future_excess_return_vs_spy as future_p05,
          p95_future_excess_return_vs_spy as future_p95
      FROM inference.return_expectation_decomposition
      WHERE fibonacci_lag_value = %s 
          AND future_fibonacci_lag_value = %s
          AND id = %s
      ORDER BY past_excess_return_z_bucket_num;",
      input$fib_lag, input$future_fib_lag, input$id_val
    )
    
    tryCatch({
      con <- get_con(input)
      res <- dbGetQuery(con, query)
      dbDisconnect(con)
      
      if(all(c("bucket", "pct", "returns", "past_p05", "past_p95", "future_p05", "future_p95") %in% names(res))) {
        res$bucket <- as.numeric(res$bucket)
        res$pct <- as.numeric(res$pct)
        res$returns <- as.numeric(res$returns)
        res$past_p05 <- as.numeric(res$past_p05)
        res$past_p95 <- as.numeric(res$past_p95)
        res$future_p05 <- as.numeric(res$future_p05)
        res$future_p95 <- as.numeric(res$future_p95)
        app_data(res)
        status_msg(paste("Successfully loaded", nrow(res), "rows."))
      } else {
        status_msg("Error: Query must return all required P05/P95 columns.")
        app_data(NULL)
      }
      
    }, error = function(e) {
      status_msg(paste("Database Error:", e$message))
    })
  })
  
  output$statusMessage <- renderText({
    status_msg()
  })
  
  # Render the data table
  output$dataTable <- renderDataTable({
    req(app_data())
    app_data()
  })
  
  # Render the interactive dual-axis plotly boxplot
  output$returnsPlot <- renderPlotly({
    req(app_data())
    df <- app_data()
    
    # Create artificial arrays to plot violins based on our aggregated P05/P95 stats
    buckets_rep <- rep(as.factor(df$bucket), each=3)
    past_y <- c(rbind(df$past_p05, (df$past_p05 + df$past_p95)/2, df$past_p95))
    future_y <- c(rbind(df$future_p05, df$returns, df$future_p95))

    # Create the interactive split violin plot
    fig <- plot_ly(data = df, type = 'violin')
    
    # Past Alpha (Violin)
    fig <- fig %>% add_trace(
      x = buckets_rep,
      y = past_y,
      name = 'Past Return Range (5th - 95th)',
      side = 'negative',
      line = list(color = '#38bdf8', width = 1),
      fillcolor = 'rgba(56, 189, 248, 0.4)',
      points = FALSE,
      hoverlabel = list(bgcolor = "#1e293b", bordercolor = "#38bdf8", font = list(color = "#f8fafc", family = "Inter")),
      hovertemplate = "Bucket: %{x}<br>Est. Range Density: %{y:.2f}%<extra></extra>"
    )
    
    # Future Alpha (Violin)
    fig <- fig %>% add_trace(
      x = buckets_rep,
      y = future_y,
      name = 'Future Spread (5th - 95th)',
      side = 'positive',
      line = list(color = '#ef4444', width = 1),
      fillcolor = 'rgba(239, 68, 68, 0.4)',
      points = FALSE,
      hoverlabel = list(bgcolor = "#1e293b", bordercolor = "#ef4444", font = list(color = "#f8fafc", family = "Inter")),
      hovertemplate = "Bucket: %{x}<br>Est. Spread Density: %{y:.2f}%<extra></extra>"
    )
    
    # Add median line for Future Returns to anchor the violins visually
    fig <- fig %>% add_markers(
      x = ~as.factor(bucket),
      y = ~returns,
      name = 'Future Median',
      marker = list(color = '#f8fafc', symbol = "line-ew", size = 16, line = list(color='#f8fafc', width=3)),
      hoverlabel = list(bgcolor = "#1e293b", bordercolor = "#f8fafc", font = list(color = "#f8fafc", family = "Inter")),
      hovertemplate = "Bucket: %{x}<br>Future Median: %{y:.2f}%<extra></extra>"
    )
    
    # Add Record Percentage as a line/area on Secondary Axis
    fig <- fig %>% add_trace(
      x = ~as.factor(bucket),
      y = ~pct,
      type = 'scatter',
      mode = 'lines+markers',
      fill = 'tozeroy',
      yaxis = 'y2',
      name = 'Record % per ID',
      line = list(color = '#10b981', width = 3),
      marker = list(color = '#10b981', size = 8),
      fillcolor = 'rgba(16, 185, 129, 0.15)',
      hoverlabel = list(bgcolor = "#1e293b", bordercolor = "#10b981", font = list(color = "#f8fafc", family = "Inter")),
      hovertemplate = "Bucket: %{x}<br>Record Percentage: %{y:.2f}%<extra></extra>"
    )
    
    # Configure the layout and styling
    fig <- fig %>% layout(
      hoverlabel = list(bgcolor = "#1e293b", font = list(color = "#f8fafc", family = "Inter")),
      title = list(text = "Past vs. Future Alpha Distribution", font = list(color = "#f8fafc", family = "Inter", size = 18)),
      paper_bgcolor = "rgba(0,0,0,0)",
      plot_bgcolor = "rgba(0,0,0,0)",
      violinmode = "overlay",
      xaxis = list(
        title = "Past Alpha Z-Bucket",
        color = "#94a3b8",
        gridcolor = "rgba(255, 255, 255, 0.1)",
        zerolinecolor = "rgba(255, 255, 255, 0.1)"
      ),
      yaxis = list(
        title = "Alpha vs Benchmark (%)",
        color = "#94a3b8",
        gridcolor = "rgba(255, 255, 255, 0.1)",
        zeroline = TRUE,
        zerolinewidth = 2,
        zerolinecolor = "rgba(255, 255, 255, 0.2)"
      ),
      yaxis2 = list(
        title = "Record Percentage per ID (%)",
        color = "#10b981",
        gridcolor = "rgba(255, 255, 255, 0.0)",
        overlaying = "y",
        side = "right",
        range = c(0, max(df$pct) * 1.5)
      ),
      margin = list(l = 50, r = 60, b = 50, t = 50),
      showlegend = TRUE,
      legend = list(font = list(color = "#f8fafc"), orientation = "h", y = -0.2)
    )
    
    fig
  })
}

# Run the application 
runApp(list(ui = ui, server = server), host="0.0.0.0", port=3839)
