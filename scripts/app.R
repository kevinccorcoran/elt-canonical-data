library(shiny)
library(jsonlite)

# Define UI
ui <- fluidPage(
  titlePanel("Dynamic Returns Dashboard from Prod Database"),
  
  sidebarLayout(
    sidebarPanel(
      helpText("This dashboard reads dynamically from data.json generated from your production database."),
      actionButton("refresh", "Refresh Data")
    ),
    
    mainPanel(
      plotOutput("returnsPlot"),
      dataTableOutput("dataTable")
    )
  )
)

# Define server logic
server <- function(input, output, session) {
  
  # Reactive function to load data
  get_data <- reactive({
    input$refresh # Re-run when Refresh is clicked
    
    file_path <- "data.json"
    if (file.exists(file_path)) {
      df_json <- fromJSON(file_path)
      data.frame(
        bucket = as.numeric(df_json$bucket),
        pct = as.numeric(df_json$pct),
        returns = as.numeric(df_json$returns)
      )
    } else {
      # Fallback dummy data if file is missing
      data.frame(
        bucket = 1:7,
        pct = c(0.11, 4, 12, 28, 39, 15, 1),
        returns = c(61.65, 36.45, 16.32, 13.23, 13.75, -0.44, 5.84)
      )
    }
  })
  
  # Render the data table
  output$dataTable <- renderDataTable({
    get_data()
  })
  
  # Render the dual-axis plot (using base R logic from plot_returns.R)
  output$returnsPlot <- renderPlot({
    df <- get_data()
    
    # Set margins
    par(mar = c(5, 4, 4, 4) + 0.3)  
    
    # Left axis bar plot
    barplot(df$pct, names.arg = df$bucket, col = "lightblue", 
            ylab = "Record Percentage per ID (%)", 
            xlab = "Past Excess Return Z-Bucket",
            main = "Excess Returns vs. Distribution by Z-Bucket",
            ylim = c(0, 45))
    
    par(new = TRUE)
    
    # Right axis line chart
    plot(df$bucket, df$returns, type = "b", pch = 16, col = "firebrick", lwd = 3,
         axes = FALSE, xlab = "", ylab = "", ylim = c(-5, 65))
    
    # Add axis
    axis(side = 4, at = pretty(range(-5, 65)))
    mtext("Median Future Excess Return vs SPY (%)", side = 4, line = 3, col="firebrick")
  })
}

# Run the application 
runApp(list(ui = ui, server = server), host="0.0.0.0", port=3838)
