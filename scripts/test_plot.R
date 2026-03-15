library(plotly)

df <- data.frame(
  bucket = c(1,2,3),
  past_p05 = c(-5, -2, 0),
  past_p95 = c(5, 8, 10),
  returns = c(0, 3, 5),
  future_p05 = c(-10, -5, -2),
  future_p95 = c(10, 15, 20),
  pct = c(10, 20, 30)
)

buckets <- as.factor(rep(df$bucket, each = 3))
past_y <- c(rbind(df$past_p05, (df$past_p05 + df$past_p95)/2, df$past_p95))
future_y <- c(rbind(df$future_p05, df$returns, df$future_p95))

fig <- plot_ly(type = 'violin')

fig <- fig %>% add_trace(
  x = ~buckets,
  y = ~past_y,
  name = 'Past Return Range (5th - 95th)',
  side = 'negative',
  line = list(color = '#38bdf8'),
  fillcolor = 'rgba(56, 189, 248, 0.4)',
  points = FALSE
)

fig <- fig %>% add_trace(
  x = ~buckets,
  y = ~future_y,
  name = 'Future Spread (5th - 95th)',
  side = 'positive',
  line = list(color = '#ef4444'),
  fillcolor = 'rgba(239, 68, 68, 0.4)',
  points = FALSE
)

fig <- fig %>% add_trace(
  x = ~as.factor(df$bucket),
  y = ~df$pct,
  type = 'scatter',
  mode = 'lines+markers',
  yaxis = 'y2',
  name = 'Record %'
)

fig <- fig %>% layout(violinmode = 'overlay')

htmlwidgets::saveWidget(fig, "test_plot.html")
