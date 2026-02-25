library(shiny)
library(DBI)
library(RPostgres)

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

# Define UI
ui <- fluidPage(
  titlePanel("Dynamic Returns Dashboard (Live SQL)"),
  
  sidebarLayout(
    sidebarPanel(
      helpText("Execute a SELECT statement directly against the Production database."),
      textAreaInput("sql_query", "SQL Query", value = default_query, rows = 10),
      actionButton("execute", "Execute Query", class = "btn-primary"),
      hr(),
      textOutput("statusMessage")
    ),
    
    mainPanel(
      plotOutput("returnsPlot"),
      dataTableOutput("dataTable")
    )
  )
)

# Define server logic
server <- function(input, output, session) {
  
  # Reactive value to store the parsed dataframe
  app_data <- reactiveVal(NULL)
  status_msg <- reactiveVal("Ready to query.")
  
  observeEvent(input$execute, {
    status_msg("Connecting to database...")
    query <- input$sql_query
    
    # Read environment variables for DB connection
    db_user <- Sys.getenv("DB_USER", "doadmin")
    db_pass <- Sys.getenv("DB_PASSWORD")
    db_host <- Sys.getenv("DB_HOST", "localhost")
    db_port <- Sys.getenv("DB_PORT", "5432")
    db_name <- "prod"
    
    if (db_pass == "") {
      status_msg("Error: DB_PASSWORD environment variable is not set.")
      return()
    }
    
    tryCatch({
      # Connect to Postgres
      con <- dbConnect(RPostgres::Postgres(),
                       dbname = db_name,
                       host = db_host,
                       port = as.integer(db_port),
                       user = db_user,
                       password = db_pass,
                       sslmode = "prefer")
      
      status_msg("Running query...")
      
      # Execute query
      res <- dbGetQuery(con, query)
      
      # Disconnect
      dbDisconnect(con)
      
      # Ensure expected column names exist
      if(all(c("bucket", "pct", "returns") %in% names(res))) {
        res$bucket <- as.numeric(res$bucket)
        res$pct <- as.numeric(res$pct)
        res$returns <- as.numeric(res$returns)
        app_data(res)
        status_msg(paste("Successfully loaded", nrow(res), "rows."))
      } else {
        status_msg("Error: Query must return 'bucket', 'pct', and 'returns' columns.")
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
  
  # Render the dual-axis plot
  output$returnsPlot <- renderPlot({
    req(app_data())
    df <- app_data()
    
    # Set margins
    par(mar = c(5, 4, 4, 4) + 0.3)  
    
    # Left axis bar plot
    barplot(df$pct, names.arg = df$bucket, col = "lightblue", 
            ylab = "Record Percentage per ID (%)", 
            xlab = "Past Excess Return Z-Bucket",
            main = "Excess Returns vs. Distribution by Z-Bucket",
            ylim = c(0, max(df$pct, 45, na.rm = TRUE) * 1.2))
    
    par(new = TRUE)
    
    # Right axis line chart
    min_ret <- min(df$returns, -5, na.rm = TRUE)
    max_ret <- max(df$returns, 65, na.rm = TRUE)
    
    plot(df$bucket, df$returns, type = "b", pch = 16, col = "firebrick", lwd = 3,
         axes = FALSE, xlab = "", ylab = "", ylim = c(min_ret, max_ret * 1.1))
    
    # Add axis
    axis(side = 4, at = pretty(range(min_ret, max_ret * 1.1)))
    mtext("Median Future Excess Return vs SPY (%)", side = 4, line = 3, col="firebrick")
  })
}

# Run the application 
runApp(list(ui = ui, server = server), host="0.0.0.0", port=3838)
