ui <- page_navbar(
  title = "EES Google Analytics dashboard",
  bg = "#0062cc",
  nav_panel(
    title = "Full service",
    layout_sidebar(
      sidebar = sidebar(
        title = "Options",
        radioButtons(
          "date_choice",
          "Choose date range",
          c(
            "week",
            "four_week",
            "since_2ndsep",
            "six_month",
            "one_year",
            "all_time"
          ),
          selected = "six_month"
        ),
      ),
      alert(
        status = "warning",
        tags$b("This app is changing!"),
        tags$p(
          "Following the move to GA4 (from universal analytics) we've had to revisit all the data underpinning this app, we are working on bringing back the same level of information you've had previously. Let us know what is most important for you being emailing ",
          a(
            href = "mailto:explore.statistics@education.gov.uk",
            "explore.statistics@education.gov.uk.",
            target = "_blank"
          )
        )
      ),
      layout_columns(
        value_box(
          title = tooltip(
            span(
              "Number of sessions",
              bsicons::bs_icon("question-circle-fill")
            ),
            "The total number of sessions. This is only applicable to the service as a whole - sessions are only counted for entry pages in the Google Analytics data. Sessions have a 24 hour limit, a session lasting 25 hours would count as two sessions.",
            placement = "bottom"
          ),
          value = textOutput("num_sessions")
        ),
        value_box(
          title = tooltip(
            span(
              "Number of pageviews",
              bsicons::bs_icon("question-circle-fill")
            ),
            "The total number of pageviews.",
            placement = "bottom"
          ),
          value = textOutput("num_pageviews")
        ),
      ),
      layout_columns(
        card(card_header(
          "Sessions",
          tooltip(
            bs_icon("info-circle"),
            "The total number of sessions. This is only applicable to the service as a whole - sessions are only counted for entry pages in the Google Analytics data. Sessions have a 24 hour limit, a session lasting 25 hours would count as two sessions."
          )
        ), plotOutput("S")),
        card(card_header(
          "Page views", tooltip(bs_icon("info-circle"), "The total number of pageviews.")
        ), plotOutput("PV")),
        col_widths = c(6, 6)
      )
    )
  ),
  nav_panel(
    title = "By publication",
    layout_sidebar(
      sidebar = sidebar(
        title = "Options",
        selectInput(
          "publication_choice",
          label = p(strong("Choose a publication")),
          choices = str_sort(pubs1$publication),
          selected = NULL
        ),
        radioButtons(
          "P_date_choice",
          "Choose date range",
          c(
            "week",
            "four_week",
            "since_4thsep",
            "six_month",
            "one_year",
            "all_time"
          ),
          selected = "six_month"
        ),
      ),
      alert(
        status = "warning",
        tags$b("This app is changing!"),
        tags$p(
          "Following the move to GA4 (from universal analytics) we've had to revisit all the data underpinning this app, we are working on bringing back the same level of information you've had previously. Let us know what is most important for you being emailing ",
          a(
            href = "mailto:explore.statistics@education.gov.uk",
            "explore.statistics@education.gov.uk.",
            target = "_blank"
          )
        )
      ),
      layout_columns(
        value_box(
          title = tooltip(
            span(
              "Number of sessions",
              bsicons::bs_icon("question-circle-fill")
            ),
            "The total number of sessions. This is only applicable to the service as a whole - sessions are only counted for entry pages in the Google Analytics data. Sessions have a 24 hour limit, a session lasting 25 hours would count as two sessions.",
            placement = "bottom"
          ),
          value = textOutput("P_num_sessions")
        ),
        value_box(
          title = tooltip(
            span(
              "Number of pageviews",
              bsicons::bs_icon("question-circle-fill")
            ),
            "The total number of pageviews.",
            placement = "bottom"
          ),
          value = textOutput("P_num_pageviews")
        ),
      ),
      layout_columns(
        card(card_header(
          "Sessions",
          tooltip(
            bs_icon("info-circle"),
            "The total number of sessions. This is only applicable to the service as a whole - sessions are only counted for entry pages in the Google Analytics data. Sessions have a 24 hour limit, a session lasting 25 hours would count as two sessions."
          )
        ), plotOutput("P_S")),
        card(card_header(
          "Page views", tooltip(bs_icon("info-circle"), "The total number of pageviews.")
        ), plotOutput("P_PV")),
        col_widths = c(6, 6)
      ),
      layout_columns(
        tableOutput("P_r_bypage"),
        tableOutput("P_oth_bypage"),
        col_widths = c(6, 6)
      )
    )
  ),
  nav_panel(
    title = "Help",
    alert(
      status = "warning",
      tags$b("This app is changing!"),
      tags$p(
        "Following the move to GA4 (from universal analytics) we've had to revisit all the data underpinning this app, we are working on bringing back the same level of information you've had previously. Let us know what is most important for you being emailing ",
        a(
          href = "mailto:explore.statistics@education.gov.uk",
          "explore.statistics@education.gov.uk.",
          target = "_blank"
        )
      )
    ),
    tags$table(
      class = "table",
      tags$tr(tags$th("Field name"), tags$th("Description")),
      tags$tr(
        tags$td("date"),
        tags$td("The date of the session formatted as YYYYMMDD.")
      ),
      tags$tr(
        tags$td("pageviews"),
        tags$td("The total number of pageviews for the property.")
      ),
      tags$tr(
        tags$td("sessions"),
        tags$td(
          "The total number of sessions. This is only applicable to the service as a whole - sessions are only counted for entry pages in the Google Analytics data. Sessions have a 24 hour limit, a session lasting 25 hours would count as two sessions."
        )
      )
    )
  ),
  nav_spacer(),
  nav_item(link_guidance),
  nav_menu(
    title = "Other links",
    align = "right",
    nav_panel(
      "Service KPI tracking",
      alert(
        status = "warning",
        tags$b("This app is changing!"),
        tags$p(
          "Following the move to GA4 (from universal analytics) we've had to revisit all the data underpinning this app, we are working on bringing back the same level of information you've had previously. Let us know what is most important for you being emailing ",
          a(
            href = "mailto:explore.statistics@education.gov.uk",
            "explore.statistics@education.gov.uk.",
            target = "_blank"
          )
        )
      ),
      p(
        "We will add out top level show and tell stats and summary stats to support our service KPIs (that rely on GA) here"
      )
    ),
    nav_panel(
      "Specific Q1",
      alert(
        status = "warning",
        tags$b("This app is changing!"),
        tags$p(
          "Following the move to GA4 (from universal analytics) we've had to revisit all the data underpinning this app, we are working on bringing back the same level of information you've had previously. Let us know what is most important for you being emailing ",
          a(
            href = "mailto:explore.statistics@education.gov.uk",
            "explore.statistics@education.gov.uk.",
            target = "_blank"
          )
        )
      ),
      p("Q1 tab content")
    ),
    nav_panel(
      "Specific Q2",
      alert(
        status = "warning",
        tags$b("This app is changing!"),
        tags$p(
          "Following the move to GA4 (from universal analytics) we've had to revisit all the data underpinning this app, we are working on bringing back the same level of information you've had previously. Let us know what is most important for you being emailing ",
          a(
            href = "mailto:explore.statistics@education.gov.uk",
            "explore.statistics@education.gov.uk.",
            target = "_blank"
          )
        )
      ),
      p("Q3 tab content")
    ),
    nav_item(link_posit)
  )
)
