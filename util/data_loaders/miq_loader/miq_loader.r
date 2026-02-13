# load the RSelenium library
# this package lets R control a web browser via Selenium
library(RSelenium)

# start a Selenium driver; this example uses Chrome in headless mode
rD <- rsDriver(browser = "chrome",
               chromever = "latest",
               extraCapabilities = list(
                 chromeOptions = list(
                   args = c("--headless",
                            "--disable-gpu",
                            "--no-sandbox",
                            "--disable-dev-shm-usage")
                 )
               ))
driver <- rD$client

# navigate to MiQ Sigma login page
driver$navigate("https://sigma.miqdigital.com/login")

# wait a few seconds for the page to load
Sys.sleep(5)

# find the username input and enter your username
user_input <- driver$findElement("name", "username")
user_input$sendKeysToElement(list(Sys.getenv("MIQ_USERNAME")))

# find the password input and enter your password
pass_input <- driver$findElement("name", "password")
pass_input$sendKeysToElement(list(Sys.getenv("MIQ_PASSWORD")))

# submit the login form by pressing the Enter key
pass_input$sendKeysToElement(list(key = "enter"))

# wait for the dashboard to load
Sys.sleep(10)

# navigate to the TV intelligence page
driver$navigate("https://sigma.miqdigital.com/intelligence/tv")

# wait for the page to load fully
Sys.sleep(10)

# find the export arrow button and click it
# the exact XPath/CSS might differ; adjust as needed by inspecting the site
export_button <- driver$findElement(using = "xpath",
                                    value = "//div[@role='button']/following::button[1]")
export_button$clickElement()

# wait for the dropdown
Sys.sleep(2)

# click on the EXCEL option
excel_option <- driver$findElement(using = "xpath",
                                   value = "//span[text()='EXCEL']")
excel_option$clickElement()

# wait for the success banner
Sys.sleep(5)

# close the browser
driver$close()
rD$server$stop()
