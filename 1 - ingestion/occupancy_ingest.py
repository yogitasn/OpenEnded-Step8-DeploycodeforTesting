import requests
import shutil
import argparse
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import time
from selenium.webdriver.support.select import Select
import datetime
import urllib.request 
import configparser
import traceback
import logging
import sys

logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d \
:: %(message)s', level = logging.INFO)

config = configparser.ConfigParser()
config.read('..\config.cfg')

DEBUG = False
chromeDriver_Path="chromedriver.exe"
year_codes={}


# function to take care of downloading file
def get_parkingOccupancy_file_code(year):
    """
    This function gets the dynamic parking occupancy file code as per year

    Used Selenium Python Library and Headless chrome to perform series of operations on Seattle Open Data UI to get the code

    Parameters:
            year (Integer): Year

    Returns:
            Code: The alpha-numeric file code for that patricular year

    """

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--verbose')

    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-software-rasterizer')

    # initialize driver object and change the <path_to_chrome_driver> depending on your directory where your chromedriver should be
    driver = webdriver.Chrome(chrome_options=chrome_options, executable_path=chromeDriver_Path)

    # get request to target the site selenium is active on
    driver.get(config['URLS']['seattle_open_data_url'])
    time.sleep(2)

    # Enter the search '{Year} Paid Parking' in the search bar 
    search_data = driver.find_element_by_xpath(config['XPATH']['search_dataByYear'])
    search_data.send_keys("{} Paid Parking".format(year))
    time.sleep(4)

    # Click on the search '{Year} Paid Parking' in the dropdown 
    print(driver.find_element_by_xpath(config['XPATH']['parking_Occpn_Option']).text)
    driver.find_element_by_xpath(config['XPATH']['parking_Occpn_Option']).click()

    time.sleep(10)

    # Get the URL of Parking Occupancy Data by Year
    url=driver.find_element_by_xpath(config['XPATH']['parking_Occpn_Option_ByYear']).get_attribute("href")
    global url_type, file_extn

    urls=url.split("/")

    if "Archive" in url:
        url_type ="Archive"
        file_extn =".zip" 
    else:
        url_type = "Latest"
        file_extn = ".csv"
    
    code=urls[5]

    print('Code is {}'.format(code))

    return code

# This function returns a concatenated string: base_url+year.csv"
def _get_parkingOcc_url_by_year(code):
    """
    This function gets the Parking Occupancy URL as per the code

    Parameters:
            code (String): File code as per year

    Returns:
            URL (String): URL for downloading the Seattle Parking Occupancy data (Year to Date)

    """
    # Retrieves the correct URL depending on whether the file is archived or latest (i.e. <= 2 yrs old)
    if url_type=="Latest":
        return "{}{}{}".format(config['URLS']['url1'],code,config['URLS']['url2'])
    else:
        return "{}{}{}".format(config['URLS']['arch_url1'],code,config['URLS']['arch_url2'])



# Retrieves data from list of urls
def _process_url(url, filename):
    """
    This function downloads the Parking Occupancy Data as per year

    """

    logging.info("Starting URL processing for {} and {}".format(url,filename))
    
    r = requests.get(url, verify=False,stream=True)
    if r.status_code!=200:
        logging.error('Error occured for URL {}'.format(url))
        exit()
    else:
        r.raw.decode_content = True
        with open(filename, 'wb') as f:
            #shutil.unpack_archive(filename, "Z:\\")
            shutil.copyfileobj(r.raw, f)
        logging.info("{} downloaded successfully".format(filename))
   



def sync_files(year,code):
    url = _get_parkingOcc_url_by_year(code)
    file="Z:\{}_Paid_Parking{}".format(year,file_extn)
    _process_url(url, file)
  

def sync_sec_file(url):
    file="Z:\BlockFace.csv"
    _process_url(url, file)
  

def _line_separator():
    return "=" * 50


def _banner():
    return "%s\nWhole Data Overview\n%s" % ((_line_separator(),) * 2)


def _greeting(day, month, year):
    return f"Starting program for {month}-{year}-{day}\n" + _line_separator()


def run():
    today = datetime.datetime.now()
    current_day, current_month, current_year = today.day, today.month, today.year

    print(_greeting(current_day, current_month, current_year))
    print(_banner())
  
     # Commented the below lines of code as it downloads a huge file of about 21GB
    for year in range(2018,2019):
        year_codes[year] = get_parkingOccupancy_file_code(year)
    
    
    for year,code in year_codes.items():
        sync_files(year,code)
    
    # Download Blockface data
    #sync_sec_file(config['URLS']['blockface_url'])

    # Download Transaction data


    
run()
    

      
