server <- function(input, output, session) {
  output$num_sessions <- renderText({
    paste0(dfeR::comma_sep(filter_on_date(combined_data1, input$date_choice) %>% as.data.frame() %>% summarise(sum(sessions))))
  })

  output$num_pageviews <- renderText({
    paste0(dfeR::comma_sep(filter_on_date(combined_data1, input$date_choice) %>% as.data.frame() %>% summarise(sum(pageviews))))
  })

  output$S <- renderPlot({
    ggplot(filter_on_date(combined_data1, input$date_choice), aes(x = date, y = sessions)) +
      geom_line(color = "steelblue") +
      xlab("") +
      theme_minimal() +
      theme(legend.position = "top")
  })

  output$PV <- renderPlot({
    ggplot(filter_on_date(combined_data1, input$date_choice), aes(x = date, y = pageviews)) +
      geom_line(color = "steelblue") +
      xlab("") +
      theme_minimal() +
      theme(legend.position = "top")
  })

  output$P_num_sessions <- renderText({
    paste0(dfeR::comma_sep(filter_on_date_pub(pub_agg1, input$P_date_choice, input$publication_choice) %>% as.data.frame() %>% summarise(sum(sessions))))
  })

  output$P_num_pageviews <- renderText({
    paste0(dfeR::comma_sep(filter_on_date_pub(pub_agg1, input$P_date_choice, input$publication_choice) %>% as.data.frame() %>% summarise(sum(pageviews))))
  })

  output$P_S <- renderPlot({
    ggplot(filter_on_date_pub(pub_agg1, input$P_date_choice, input$publication_choice), aes(x = date, y = sessions)) +
      geom_line(color = "steelblue") +
      xlab("") +
      theme_minimal() +
      theme(legend.position = "top")
  })

  output$P_PV <- renderPlot({
    ggplot(filter_on_date_pub(pub_agg1, input$P_date_choice, input$publication_choice), aes(x = date, y = pageviews)) +
      geom_line(colour = "steelblue") +
      xlab("") +
      theme_minimal() +
      theme(legend.position = "top")
  })

  output$P_r_bypage <- renderTable({
    filter_on_date_pub(joined_data1, input$P_date_choice, input$publication_choice) %>%
      filter(!str_detect(url, "methodology") & !str_detect(url, "data-tables") & !str_detect(url, "data-guidance") & !str_detect(url, "prerelease-access-list")) %>%
      group_by(publication, pagePath) %>%
      summarise("sessions" = sum(sessions), "pageviews" = sum(pageviews)) %>%
      as.data.frame() %>%
      select(pagePath, sessions, pageviews)
  })

  output$P_oth_bypage <- renderTable({
    filter_on_date_pub(joined_data1, input$P_date_choice, input$publication_choice) %>%
      filter(str_detect(url, "methodology") | str_detect(url, "data-tables") | str_detect(url, "data-guidance") | str_detect(url, "prerelease-access-list")) %>%
      group_by(publication, pagePath) %>%
      summarise("sessions" = sum(sessions), "pageviews" = sum(pageviews)) %>%
      as.data.frame() %>%
      select(pagePath, sessions, pageviews)
  })

  session$onSessionEnded(function() {
    stopApp()
  })
}
