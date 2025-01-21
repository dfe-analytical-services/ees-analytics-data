# Start an app running ========================================================
# TODO: Undo this temporary system environment variable setting
# - I'm sure there's a better way to do this
Sys.setenv("TEST_MODE" = "TRUE")

app <- AppDriver$new(
  name = "basic_load_nav",
  expect_values_screenshot_args = FALSE
)

test_that("App loads and title of app appears as expected", {
  expect_equal(app$get_text("title"), "Explore education statistics analytics")
})

Sys.unsetenv("TEST_MODE")
