url = "http://testing-ground.scraping.pro/ajax"
from selenium import webdriver

def findXPath(element,target,path):
    if target in element.get_attribute('textContent') and element.tag_name == "ul":
        return path
    newElements = element.find_elements_by_xpath("./*")
    for newElement in newElements:
#        print(path+ "/"+newElement.tag_name)
        final = findXPath(newElement,target,path + "/"+newElement.tag_name)
        if final != "":
            return final
    return ""
browser = webdriver.Chrome(executable_path=r"C:\selenium\chromedriver")
browser.get(url)
#print(browser.page_source)
elements = browser.find_element_by_xpath("html")
finalXPath = findXPath(elements,"Andrew","html")
print("Final xPath:",finalXPath)
element = browser.find_element_by_xpath(finalXPath)
print("Names:\n",element.text) #Fortunately here, we don't need to use get_attribute("textContent")
                                 #But you can try it out anyway to see the difference in formatting
browser.quit()