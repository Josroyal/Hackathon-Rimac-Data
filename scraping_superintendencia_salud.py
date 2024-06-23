import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# URL of the webpage containing the link to the CSV file
url = 'http://datos.susalud.gob.pe/dataset/consulta-b2-morbilidad-en-consulta-ambulatoria'

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

try:
    driver.get(url)
    time.sleep(2)  # Give time for the page to load

    # Find all links on the webpage
    links = driver.find_elements(By.TAG_NAME, 'a')
    
    print("Here are the links found on the webpage: " + str(len(links)))
    for link in links:
        print(link.get_attribute('href'))

    csv_links = [link.get_attribute('href') for link in links if link.get_attribute('href') and 'csv' in link.get_attribute('href').lower()]
    
    print("Here are the CSV links found on the webpage: " + str(len(csv_links)))
    for csv_link in csv_links:
        print(csv_link)

    if csv_links:
        csv_url = csv_links[0]
        driver.get(csv_url)
        time.sleep(5)  # Give time for download to complete (adjust as needed)

        import pandas as pd
        df = pd.read_csv(csv_url)
        print(df.head())

    else:
        print('No CSV file links found on the webpage.')

finally:
    driver.quit()