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
ui <- navbarPage(
  title = "Analysis Dashboard",
  
  tags$head(
    tags$style(HTML(custom_css))
  ),
  
  # Tab 2: New Visualization
  tabPanel("New Visualization",
    sidebarLayout(
      sidebarPanel(
        h4("Database Connection (Tab 2)"),
        selectInput("db_env2", "Environment", choices = c("Production", "Staging", "Dev"), selected = "Dev"),
        textInput("db_host2", "Host", value = "host.docker.internal"),
        textInput("db_port2", "Port", value = "5432"),
        textInput("db_user2", "User", value = "postgres"),
        passwordInput("db_pass2", "Password", value = ""),
        actionButton("connect_btn2", "Connect & Load Filters", class = "btn-primary"),
        
        hr(),
        h5("Filters"),
        div(id = "filter_panel2",
          selectInput("fib_lag2", "Fibonacci Lag Value", choices = c("Connect first..." = ""), selected = ""),
          selectInput("id_val2", "ID", choices = c("Connect first..." = ""), selected = "")
        ),
        hr(),
        actionButton("execute_2", "Generate Chart", class = "btn-primary w-100", style = "margin-top: 1rem;"),
        hr(),
        textOutput("statusMessage2")
      ),
      mainPanel(
        div(class = "main-card",
            h4("Return Expectation Decomposition 2", style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
            plotlyOutput("newPlot", height = "700px")
        )
      )
    )
  ),
  
  # Tab 3: Combined Visualization
  tabPanel("Combined Visualization",
    sidebarLayout(
      sidebarPanel(
        h4("Database Connection (Combined)"),
        selectInput("db_env3", "Environment", choices = c("Production", "Staging", "Dev"), selected = "Dev"),
        textInput("db_host3", "Host", value = "host.docker.internal"),
        textInput("db_port3", "Port", value = "5432"),
        textInput("db_user3", "User", value = "postgres"),
        passwordInput("db_pass3", "Password", value = ""),
        actionButton("connect_btn3", "Connect & Load Filters", class = "btn-primary"),
        
        hr(),
        h5("Filters"),
        div(id = "filter_panel3",
          selectInput("fib_lag3", "Past Fib Lag", choices = c("Connect first..." = ""), selected = ""),
          selectInput("future_fib_lag3", "Future Fib Lag", choices = c("Connect first..." = ""), selected = ""),
          selectInput("id_val3", "ID", choices = c("Connect first..." = ""), selected = "")
        ),
        hr(),
        actionButton("execute_3", "Generate Combined Chart", class = "btn-primary w-100", style = "margin-top: 1rem;"),
        hr(),
        textOutput("statusMessage3")
      ),
      mainPanel(
        div(class = "main-card",
            h4("Past vs. Future Expectation Decomposition", style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
            plotlyOutput("combinedPlot", height = "700px")
        )
      )
    )
  )
)

# Helper to create a DB connection from UI inputs
get_con <- function(input, tab = 1) {
  if (tab == 1) {
    db_string <- if (input$db_env == "Production") "prod" else if (input$db_env == "Staging") "staging" else "dev"
    dbConnect(RPostgres::Postgres(),
              dbname = db_string,
              host = input$db_host,
              port = as.integer(input$db_port),
              user = input$db_user,
              password = input$db_pass,
              sslmode = "prefer")
  } else if (tab == 2) {
    db_string <- if (input$db_env2 == "Production") "prod" else if (input$db_env2 == "Staging") "staging" else "dev"
    dbConnect(RPostgres::Postgres(),
              dbname = db_string,
              host = input$db_host2,
              port = as.integer(input$db_port2),
              user = input$db_user2,
              password = input$db_pass2,
              sslmode = "prefer")
  } else if (tab == 3) {
    db_string <- if (input$db_env3 == "Production") "prod" else if (input$db_env3 == "Staging") "staging" else "dev"
    dbConnect(RPostgres::Postgres(),
              dbname = db_string,
              host = input$db_host3,
              port = as.integer(input$db_port3),
              user = input$db_user3,
              password = input$db_pass3,
              sslmode = "prefer")
  }
}

# Define server logic
server <- function(input, output, session) {
  
  # Reactive value to store the parsed dataframe
  app_data2 <- reactiveVal(NULL)
  status_msg2 <- reactiveVal("Ready — select Database Environment and click Connect & Load Filters.")
  app_data3 <- reactiveVal(NULL)
  status_msg3 <- reactiveVal("Ready — select Database Environment and click Connect & Load Filters.")
  
  output$statusMessage2 <- renderText({ status_msg2() })
  output$statusMessage3 <- renderText({ status_msg3() })
  
  observeEvent(input$db_env2, {
    if (input$db_env2 == "Production") {
      updateTextInput(session, "db_host2", value = "dbaas-db-4718169-do-user-32264340-0.l.db.ondigitalocean.com")
      updateTextInput(session, "db_port2", value = "25060")
      updateTextInput(session, "db_user2", value = "doadmin")
      updateTextInput(session, "db_pass2", value = Sys.getenv("PROD_DB_PASSWORD", ""))
    } else if (input$db_env2 == "Staging") {
      updateTextInput(session, "db_host2", value = "host.docker.internal")
      updateTextInput(session, "db_port2", value = "5432")
      updateTextInput(session, "db_user2", value = "postgres")
      updateTextInput(session, "db_pass2", value = "")
    } else if (input$db_env2 == "Dev") {
      updateTextInput(session, "db_host2", value = "host.docker.internal")
      updateTextInput(session, "db_port2", value = "5432")
      updateTextInput(session, "db_user2", value = "postgres")
      updateTextInput(session, "db_pass2", value = "")
    }
  })
  
  observeEvent(input$db_env3, {
    if (input$db_env3 == "Production") {
      updateTextInput(session, "db_host3", value = "dbaas-db-4718169-do-user-32264340-0.l.db.ondigitalocean.com")
      updateTextInput(session, "db_port3", value = "25060")
      updateTextInput(session, "db_user3", value = "doadmin")
      updateTextInput(session, "db_pass3", value = Sys.getenv("PROD_DB_PASSWORD", ""))
    } else if (input$db_env3 == "Staging") {
      updateTextInput(session, "db_host3", value = "host.docker.internal")
      updateTextInput(session, "db_port3", value = "5432")
      updateTextInput(session, "db_user3", value = "postgres")
      updateTextInput(session, "db_pass3", value = "")
    } else if (input$db_env3 == "Dev") {
      updateTextInput(session, "db_host3", value = "host.docker.internal")
      updateTextInput(session, "db_port3", value = "5432")
      updateTextInput(session, "db_user3", value = "postgres")
      updateTextInput(session, "db_pass3", value = "")
    }
  })
  
  # Execute query for Tab 2
  observeEvent(input$execute_2, {
    if (input$db_pass2 == "") {
      status_msg2("Error: Password is not set.")
      return()
    }
    if (input$fib_lag2 == "" || input$id_val2 == "") {
      status_msg2("Error: Please connect and select filter values first.")
      return()
    }
    
    status_msg2("Running query...")
    
    query <- sprintf(
      "SELECT 
          past_excess_return_z_bucket_num as bucket,
          record_count_in_bucket as count,
          min_past_excess_return_vs_spy as past_min,
          past_q1_return as past_q1,
          past_median_return as median,
          past_q3_return as past_q3,
          max_past_excess_return_vs_spy as past_max
      FROM inference.return_expectation_decomposition_past
      WHERE fibonacci_lag_value = %s 
          AND id = %s
      ORDER BY past_excess_return_z_bucket_num;",
      input$fib_lag2, input$id_val2
    )
    
    tryCatch({
      con <- get_con(input, tab = 2)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      res <- dbGetQuery(con, query)
      
      if(all(c("bucket", "count", "past_min", "past_q1", "median", "past_q3", "past_max") %in% names(res))) {
        res$bucket <- as.numeric(res$bucket)
        res$count <- as.numeric(res$count)
        res$past_min <- as.numeric(res$past_min)
        res$past_q1 <- as.numeric(res$past_q1)
        res$median <- as.numeric(res$median)
        res$past_q3 <- as.numeric(res$past_q3)
        res$past_max <- as.numeric(res$past_max)
        app_data2(res)
        status_msg2(paste("Successfully loaded", nrow(res), "rows from Table 2."))
      } else {
        status_msg2("Error: Query must return (bucket, count, past_min, past_q1, median, past_q3, past_max).")
        app_data2(NULL)
      }
      
    }, error = function(e) {
      status_msg2(paste("Database Error:", e$message))
    })
  })
  
  # Connect and populate dropdowns Tab 2
  observeEvent(input$connect_btn2, {
    if (input$db_pass2 == "") {
      status_msg2("Error: Password is not set.")
      return()
    }
    
    status_msg2("Connecting to database...")
    
    tryCatch({
      con <- get_con(input, tab = 2)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      
      # Since `return_expectation_decomposition_past` might not exist yet, handle gracefully
      tryCatch({
        fib_vals <- dbGetQuery(con, "SELECT DISTINCT fibonacci_lag_value FROM inference.return_expectation_decomposition_past ORDER BY 1")
        id_vals <- dbGetQuery(con, "SELECT DISTINCT id FROM inference.return_expectation_decomposition_past ORDER BY 1")
        
        updateSelectInput(session, "fib_lag2", choices = fib_vals[[1]], selected = fib_vals[[1]][1])
        updateSelectInput(session, "id_val2", choices = id_vals[[1]], selected = id_vals[[1]][1])
        status_msg2("Filters loaded successfully!")
      }, error = function(e) {
        status_msg2(paste("Connected, but failed to load filters (check if table 2 is built):", e$message))
      })
      
    }, error = function(e) {
      status_msg2(paste("Database Connection Error:", e$message))
    })
  })
  
  output$statusMessage2 <- renderText({
    status_msg2()
  })
  
  # Render the new visualization for Tab 2
  output$newPlot <- renderPlotly({
    req(app_data2())
    df <- app_data2()
    
    if (nrow(df) == 0) {
      return(plot_ly() %>% layout(title = list(text = "No data found for these filters in Table 2", font = list(color = "#f8fafc", family = "Inter", size = 18)), paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))
    }
    
    # Calculate record percentage within this specific slice
    df$pct <- (df$count / sum(df$count, na.rm = TRUE)) * 100
    
    fig <- plot_ly(df)
    
    # Past Excess Returns (Statistical Boxplot)
    fig <- fig %>% add_trace(
      type = 'box',
      name = 'Past Return Distribution',
      x = ~as.factor(bucket),
      q1 = ~past_q1,
      median = ~median,
      q3 = ~past_q3,
      lowerfence = ~past_min,
      upperfence = ~past_max,
      marker = list(color = '#a855f7'),
      line = list(color = '#a855f7', width = 2),
      fillcolor = 'rgba(167, 139, 250, 0.4)',
      hoverinfo = "y",
      offsetgroup = '1'
    )
    
    # White Median Overlay
    fig <- fig %>% add_markers(
      x = ~as.factor(bucket),
      y = ~median,
      name = 'Median',
      marker = list(color = '#ffffff', symbol = "line-ew", size = 45, line = list(color='#ffffff', width=3)),
      hoverinfo = "skip",
      showlegend = FALSE,
      offsetgroup = '1'
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
      line = list(color = '#fbbf24', width = 3),
      marker = list(color = '#fbbf24', size = 8),
      fillcolor = 'rgba(251, 191, 36, 0.15)',
      hovertemplate = "Bucket: %{x}<br>Percentage: %{y:.2f}%<extra></extra>"
    )
    
    # Configure the layout and styling
    fig %>% layout(
      title = list(text = "Past Return Distribution by Alpha Z-Bucket", font = list(color = "#f8fafc", family = "Inter", size = 18)),
      paper_bgcolor = "rgba(0,0,0,0)",
      plot_bgcolor = "rgba(0,0,0,0)",
      barmode = "group",
      xaxis = list(
        title = "Past Excess Return Z-Bucket (SD)",
        color = "#94a3b8",
        gridcolor = "rgba(255, 255, 255, 0.1)",
        zerolinecolor = "rgba(255, 255, 255, 0.1)"
      ),
      yaxis = list(
        title = "Excess Return vs SPY (%)",
        color = "#94a3b8",
        gridcolor = "rgba(255, 255, 255, 0.1)",
        zeroline = TRUE,
        zerolinewidth = 2,
        zerolinecolor = "rgba(255, 255, 255, 0.2)"
      ),
      yaxis2 = list(
        title = "Record Percentage (%)",
        color = "#fbbf24",
        gridcolor = "rgba(255, 255, 255, 0.0)",
        overlaying = "y",
        side = "right",
        range = c(0, ifelse(is.infinite(max(df$pct, na.rm = TRUE)) || is.na(max(df$pct, na.rm = TRUE)), 100, max(df$pct, na.rm = TRUE) * 1.5))
      ),
      margin = list(l = 50, r = 60, b = 50, t = 50),
      showlegend = TRUE,
      legend = list(font = list(color = "#f8fafc"), orientation = "h", y = -0.2)
    )
  })
  
  # Connect and populate dropdowns Tab 3
  observeEvent(input$connect_btn3, {
    if (input$db_pass3 == "") {
      status_msg3("Error: Password is not set.")
      return()
    }
    status_msg3("Connecting to database...")
    tryCatch({
      con <- get_con(input, tab = 3)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      
      tryCatch({
        fib_vals <- dbGetQuery(con, "SELECT DISTINCT fibonacci_lag_value FROM inference.return_expectation_decomposition_past ORDER BY 1")
        future_fib_vals <- dbGetQuery(con, "SELECT DISTINCT future_fibonacci_lag_value FROM inference.return_expectation_decomposition_future ORDER BY 1")
        id_vals <- dbGetQuery(con, "SELECT DISTINCT id FROM inference.return_expectation_decomposition_past ORDER BY 1")
        
        updateSelectInput(session, "fib_lag3", choices = fib_vals[[1]], selected = fib_vals[[1]][1])
        updateSelectInput(session, "future_fib_lag3", choices = future_fib_vals[[1]], selected = future_fib_vals[[1]][1])
        updateSelectInput(session, "id_val3", choices = id_vals[[1]], selected = id_vals[[1]][1])
        status_msg3("Filters loaded successfully!")
      }, error = function(e) {
        status_msg3(paste("Connected, but failed to load filters:", e$message))
      })
      
    }, error = function(e) {
      status_msg3(paste("Database Connection Error:", e$message))
    })
  })
  
  # Execute query for Tab 3
  observeEvent(input$execute_3, {
    if (input$db_pass3 == "") {
      status_msg3("Error: Password is not set.")
      return()
    }
    if (input$fib_lag3 == "" || input$future_fib_lag3 == "" || input$id_val3 == "") {
      status_msg3("Error: Please connect and select filter values first.")
      return()
    }
    
    status_msg3("Running query...")
    
    query <- sprintf("
      WITH p AS (
        SELECT 
            past_excess_return_z_bucket_num as bucket,
            record_count_in_bucket as past_count,
            min_past_excess_return_vs_spy as past_min,
            past_q1_return as past_q1,
            past_median_return as past_median,
            past_q3_return as past_q3,
            max_past_excess_return_vs_spy as past_max
        FROM inference.return_expectation_decomposition_past
        WHERE fibonacci_lag_value = %s AND id = %s
      ),
      f AS (
        SELECT 
            future_excess_return_z_bucket_num as bucket,
            record_count_in_bucket as future_count,
            min_future_excess_return_vs_spy as future_min,
            future_q1_return as future_q1,
            future_median_return as future_median,
            future_q3_return as future_q3,
            max_future_excess_return_vs_spy as future_max
        FROM inference.return_expectation_decomposition_future
        WHERE future_fibonacci_lag_value = %s AND id = %s
      )
      SELECT 
          COALESCE(p.bucket, f.bucket) as bucket,
          p.past_count, p.past_min, p.past_q1, p.past_median, p.past_q3, p.past_max,
          f.future_count, f.future_min, f.future_q1, f.future_median, f.future_q3, f.future_max
      FROM p 
      FULL OUTER JOIN f ON p.bucket = f.bucket
      ORDER BY bucket;
    ", input$fib_lag3, input$id_val3, input$future_fib_lag3, input$id_val3)
    
    tryCatch({
      con <- get_con(input, tab = 3)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      res <- dbGetQuery(con, query)
      
      if(nrow(res) > 0) {
        # Convert numeric columns
        for(col in names(res)) { res[[col]] <- as.numeric(res[[col]]) }
        
        # Safe percentages
        tot_past <- sum(res$past_count, na.rm = TRUE)
        tot_future <- sum(res$future_count, na.rm = TRUE)
        res$past_pct <- if(tot_past > 0) (res$past_count / tot_past) * 100 else 0
        res$future_pct <- if(tot_future > 0) (res$future_count / tot_future) * 100 else 0
        
        app_data3(res)
        status_msg3(paste("Successfully loaded", nrow(res), "rows from combined tables."))
      } else {
        status_msg3("No data found for this combination.")
        app_data3(NULL)
      }
      
    }, error = function(e) {
      status_msg3(paste("Database Error:", e$message))
    })
  })
  
  # Render the new visualization for Tab 3
  output$combinedPlot <- renderPlotly({
    req(app_data3())
    df <- app_data3()
    
    if (nrow(df) == 0) {
      return(plot_ly() %>% layout(title = list(text = "No data found for these filters", font = list(color = "#f8fafc", family = "Inter", size = 18)), paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))
    }
    
    # Calculate Dynamic Colors for Future Median
    # Using a brighter neon pink/red for better contrast against dark blue
    df$future_median_color <- ifelse(df$future_median > df$past_median, "#10b981", "#f43f5e")
    
    # Calculate X offsets for side-by-side grouped placement
    # Using categorical plotting in Plotly grouped barmode, the boxes are slightly shifted.
    # We will simulate this shift for markers: Past (-0.15), Future (+0.15)
    bar_offset <- 0.16
    df$x_num <- as.numeric(as.factor(df$bucket))
    
    fig <- plot_ly(df)
    
    # PAST EXCESS RETURNS (Purple)
    fig <- fig %>% add_trace(
      type = 'box',
      name = 'Past Return Distribution',
      x = ~as.factor(bucket),
      q1 = ~past_q1,
      median = ~past_median,
      q3 = ~past_q3,
      lowerfence = ~past_min,
      upperfence = ~past_max,
      marker = list(color = '#a855f7'),
      line = list(color = '#a855f7', width = 2),
      fillcolor = 'rgba(167, 139, 250, 0.4)',
      hoverinfo = "y",
      offsetgroup = '1'
    )
    
    # Light Gray Median Overlay (Past)
    fig <- fig %>% add_markers(
      x = ~x_num - bar_offset,
      y = ~past_median,
      name = 'Past Median',
      marker = list(color = '#cbd5e1', symbol = "line-ew", size = 25, line = list(color='#cbd5e1', width=3)),
      hoverinfo = "skip",
      showlegend = FALSE,
      offsetgroup = '1'
    )
    
    # FUTURE EXCESS RETURNS (Sky Blue)
    fig <- fig %>% add_trace(
      type = 'box',
      name = 'Future Return Distribution',
      x = ~as.factor(bucket),
      q1 = ~future_q1,
      median = ~future_median,
      q3 = ~future_q3,
      lowerfence = ~future_min,
      upperfence = ~future_max,
      marker = list(color = '#0ea5e9'),
      line = list(color = '#0ea5e9', width = 2),
      fillcolor = 'rgba(14, 165, 233, 0.4)',
      hoverinfo = "y",
      offsetgroup = '2'
    )
    
    # Traffic Light Median Overlay (Future)
    fig <- fig %>% add_markers(
      x = ~x_num + bar_offset,
      y = ~future_median,
      name = 'Future Median',
      marker = list(
        color = ~future_median_color, 
        symbol = "line-ew", 
        size = 25, 
        line = list(color = ~future_median_color, width = 5)
      ),
      hoverinfo = "skip",
      showlegend = FALSE,
      offsetgroup = '2'
    )
    
    # Record Percentage (PAST) -> Yellow dashed
    fig <- fig %>% add_trace(
      x = ~as.factor(bucket),
      y = ~past_pct,
      type = 'scatter',
      mode = 'lines+markers',
      name = 'Past % of Records',
      yaxis = 'y2',
      line = list(color = '#fbbf24', width = 2, dash = 'dot'),
      marker = list(color = '#fbbf24', size = 6),
      hovertemplate = "Bucket: %{x}<br>Past %: %{y:.2f}%<extra></extra>"
    )
    
    # Record Percentage (FUTURE) -> Green solid
    fig <- fig %>% add_trace(
      x = ~as.factor(bucket),
      y = ~future_pct,
      type = 'scatter',
      mode = 'lines+markers',
      name = 'Future % of Records',
      yaxis = 'y2',
      line = list(color = '#34d399', width = 3),
      marker = list(color = '#34d399', size = 8),
      hovertemplate = "Bucket: %{x}<br>Future %: %{y:.2f}%<extra></extra>"
    )
    
    # Layout configuration
    max_pct <- max(c(df$past_pct, df$future_pct), na.rm = TRUE)
    
    fig %>% layout(
      title = list(text = "Past vs. Future Expected Returns by Alpha Z-Bucket", font = list(color = "#f8fafc", family = "Inter", size = 18)),
      paper_bgcolor = "rgba(0,0,0,0)",
      plot_bgcolor = "rgba(0,0,0,0)",
      barmode = "group",
      boxmode = "group",
      xaxis = list(
        title = "Excess Return Z-Bucket (SD)",
        color = "#94a3b8",
        gridcolor = "rgba(255, 255, 255, 0.1)",
        zerolinecolor = "rgba(255, 255, 255, 0.1)"
      ),
      yaxis = list(
        title = "Excess Return vs SPY (%)",
        color = "#94a3b8",
        gridcolor = "rgba(255, 255, 255, 0.1)",
        zeroline = TRUE,
        zerolinewidth = 2,
        zerolinecolor = "rgba(255, 255, 255, 0.2)"
      ),
      yaxis2 = list(
        title = "Record Percentage (%)",
        color = "#f8fafc",
        gridcolor = "transparent",
        overlaying = "y",
        side = "right",
        range = c(0, ifelse(is.infinite(max_pct) || is.na(max_pct), 100, max_pct * 1.5))
      ),
      margin = list(l = 50, r = 60, b = 50, t = 50),
      showlegend = TRUE,
      legend = list(font = list(color = "#f8fafc"), orientation = "h", y = -0.2)
    )
  })
}

# Run the application 
runApp(list(ui = ui, server = server), host="0.0.0.0", port=3838)
