[
    
](https://chatgpt.com/share/e/691f85c9-a82c-800b-a6ca-d0bf44c5ed82)

Here’s a high‑level approach for automating the export without using the browser agent. The idea is to script the steps you performed manually: sign in to MiQ Sigma, navigate to the TV intelligence page, click the export button, and wait for the email. Since MiQ Sigma doesn’t publish a public API for this report, browser automation (via Selenium in Python or RSelenium in R) is the practical workaround. Below are simplified examples for both languages.

**Important notes before you start**

* You should store your MiQ Sigma username and password securely (e.g., in environment variables or a secrets manager) and reference them in the script—never hard‑code credentials.
* Headless browser automation mimics a real user, so respect MiQ Sigma’s terms of service.
* Schedule the script to run daily using cron (Linux/macOS) or Task Scheduler (Windows).

---

## 🐍 Example in Python (Selenium)

This script uses Selenium WebDriver with Chrome to automate the export. Each step is commented to explain what it does. There are no functions—just a linear sequence of commands.

```python
# import the necessary modules
# webdriver controls the browser; By helps locate elements; Keys lets us send keyboard presses
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import os

# set up the Chrome driver (make sure chromedriver is installed and in your PATH)
# the 'options' allow headless mode so the browser doesn't open a visible window
options = webdriver.ChromeOptions()
options.add_argument("--headless")          # run Chrome without opening a window
options.add_argument("--no-sandbox")        # required for some Linux environments
options.add_argument("--disable-dev-shm-usage")  # overcome limited resource problems
driver = webdriver.Chrome(options=options)

# navigate to the MiQ Sigma login page
driver.get("https://sigma.miqdigital.com/login")

# wait briefly to allow the page to load (simple sleep used for clarity)
time.sleep(5)

# locate the username and password fields and enter your credentials
# credentials are pulled from environment variables for security
username_field = driver.find_element(By.NAME, "username")
username_field.send_keys(os.environ["MIQ_USERNAME"])  # replace with your env var name

password_field = driver.find_element(By.NAME, "password")
password_field.send_keys(os.environ["MIQ_PASSWORD"])  # replace with your env var name

# submit the login form by pressing the ENTER key
password_field.send_keys(Keys.RETURN)

# wait for the dashboard page to load completely
time.sleep(10)

# navigate directly to the TV intelligence page with appropriate query parameters
driver.get("https://sigma.miqdigital.com/intelligence/tv")

# wait for the page content to load
time.sleep(10)

# locate the export/share button (square with an arrow) and click it
export_button = driver.find_element(By.XPATH, "//div[@role='button']/following::button[1]")
export_button.click()

# wait briefly for the dropdown menu to appear
time.sleep(2)

# locate the "EXCEL" option in the dropdown and click it
excel_option = driver.find_element(By.XPATH, "//span[text()='EXCEL']")
excel_option.click()

# wait for the success banner to appear (indicates the report was sent via email)
time.sleep(5)

# close the browser session
driver.quit()
```

This script:

* Launches a headless Chrome browser.
* Signs in to MiQ Sigma using credentials stored in environment variables (`MIQ_USERNAME` and `MIQ_PASSWORD`).
* Navigates to the TV report.
* Clicks the export button and selects **EXCEL**.
* Waits for confirmation that the report email has been sent.
* Closes the browser.

You can schedule this script to run each morning using a cron job like:

```bash
# open the cron editor
crontab -e

# add a line to run the script every day at 8:00 AM (adjust the path)
0 8 * * * /usr/bin/python3 /path/to/miq_sigma_export.py
```

---

## 📊 Example in R (RSelenium)

Below is a similar process using the RSelenium package in R. This example runs through the same sequence of steps. Again, each line is commented.

```r
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
```

This R script:

* Launches a headless Chrome session using RSelenium.
* Logs in to MiQ Sigma.
* Navigates to the TV intelligence report.
* Clicks the export option for Excel.
* Waits for confirmation that the report email is sent.
* Closes the session.

---

These examples should help you automate the export process without using the browser agent mode. You can adapt them to your environment, store credentials securely, and schedule them to run daily.
