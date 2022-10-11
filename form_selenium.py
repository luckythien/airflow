from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

url = "https://www.google.com"

browser = webdriver.Chrome(executable_path=r"C:\selenium\chromedriver")

browser.get(url)

inputElement = browser.find_element(By.XPATH,"/html/body/div[1]/div[3]/form/div[1]/div[1]/div[1]/div/div[2]/input")
# inputElement = browser.find_element_by_id("lst-ib")
inputElement.send_keys("Yahoo Finance")
inputElement.submit()
# element = browser.find_element(By.XPATH,"""//*[@id="hdtb-msb"]/div[1]/div/div[2]/a""")   # click image section after search keyword
# element.click()
elemant = WebDriverWait(browser,10).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="hdtb-msb"]/div[1]/div/div[2]/a"""))).click()
# browser.quit()