#! python3

import bs4, requests, sys, re, datetime
import pandas as pd
from selenium import webdriver
from df2gspread import gspread2df as g2d
from df2gspread import df2gspread as d2g


# setup Google Sheets
sheets_id = '1i7wVz-uP9kgLwNRWyRBvmQ9TTuEx9i0xi23GfSA7sSw'
sheet_name = 'northwood'

chrome = webdriver.Chrome('C:/Users/xi373146/Downloads/chromedriver.exe')
#chrome.get('http://www.zoopla.co.uk/')

today = datetime.date.today().strftime('%d/%m/%Y')
zoopla_url = 'wwww.zoopla.co.uk'
search_box_selector = '#search-input-location'
min_price_selector = '#forsale_price_min'
max_price_selector = '#forsale_price_max'
type_selector = '#property_type'
beds_selector = '#beds_min'
url_search_northwood = 'http://www.zoopla.co.uk/for-sale/houses/london/northwood/?beds_min=3&price_max=1100000&price_min=900000&q=Northwood%2C%20London&results_sort=newest_listings'
listing_selector = '#content > ul > li'
listing_url_prefix = 'http://www.zoopla.co.uk/for-sale/details'
station_re = re.compile(r'(.*) \((.*)\)')
list_date_re = re.compile(r'Listed on (.*) by')


chrome.get(url_search_northwood)
listings = chrome.find_elements_by_css_selector(listing_selector)

title = []
address = []
lat = []
long = []
price = []
desc_br = []
contact_num = []
num_beds = []
num_baths = []
num_receps = []
list_date = []
station1 = []
station1_dist = []
station2 = []
station2_dist = []
id = []
url = []
count = 0

for l in listings:
	
	count += 1
	print('\nProcessing listing ' + str(count) + ' of ' + str(len(listings)) + ' ...')
	left = l.find_element_by_css_selector('div > div.listing-results-left > div')
	right = l.find_element_by_css_selector('div > div.listing-results-right.clearfix')
	footer = l.find_element_by_css_selector('div > div.listing-results-footer.clearfix')
	
	title.append(right.find_element_by_css_selector('h2 > a').text)
	address.append(right.find_element_by_css_selector('span > a').text)
	
	lat_long = right.find_elements_by_css_selector('div > meta')
	lat.append(lat_long[0].get_attribute('content'))
	long.append(lat_long[1].get_attribute('content'))
	
	price_raw = right.find_element_by_css_selector('div > a').text
	price_appendix = right.find_element_by_css_selector('div > a > span').text
	price.append((price_raw[:(len(price_raw) - len(price_appendix))]).strip())
	
	desc_br.append(right.find_element_by_css_selector('div > p').text)
	contact_num.append(footer.find_element_by_css_selector('div.listing-results-right > p > span > a > span').text)
	
	try:
		beds = right.find_element_by_css_selector('h3 > span.num-icon.num-beds')
		num_beds.append(beds.text)
	except:
		num_beds.append('')
	
	try:
		baths = right.find_element_by_css_selector('h3 > span.num-icon.num-baths')
		num_baths.append(baths.text)
	except:
		num_baths.append('')
	
	try:
		receps = right.find_element_by_css_selector('h3 > span.num-icon.num-reception')
		num_receps.append(receps.text)
	except:
		num_receps.append('')
		
	list_date_raw = footer.find_element_by_css_selector('div.listing-results-left > p > small').text
	list_date.append(list_date_re.findall(list_date_raw)[0])
	
	try:
		stations = right.find_elements_by_css_selector('div.nearby_stations_schools.clearfix > ul > li')
		station1_raw = stations[0].text
		station1_tup = station_re.findall(station1_raw)[0]
		station1.append(station1_tup[0])
		station1_dist.append(station1_tup[1])
		station2_raw = stations[1].text
		station2_tup = station_re.findall(station2_raw)[0]
		station2.append(station2_tup[0])
		station2_dist.append(station2_tup[1])
	except:
		station1.append('')
		station1_dist.append('')
		station2.append('')
		station2_dist.append('')

	
	id_raw = l.get_attribute('data-listing-id')
	id.append(id_raw)
	url.append(listing_url_prefix + '/' + id_raw)
	

chrome.quit()
print('\nQuerying finshed, now produce output...\n')

d = {
	'title' : pd.Series(title),
	'address' : pd.Series(address),
	'lat' : pd.Series(lat),
	'long' : pd.Series(long),
	'price' : pd.Series(price),
	'desc_br' : pd.Series(desc_br),
	'contact_num' : pd.Series(contact_num),
	'num_beds' : pd.Series(num_beds),
	'num_baths' : pd.Series(num_baths),
	'num_receps' : pd.Series(num_receps),
	'list_date' : pd.Series(list_date),
	'station1' : pd.Series(station1),
	'station1_dist' : pd.Series(station1_dist),
	'station2' : pd.Series(station2),
	'station2_dist' : pd.Series(station2_dist),
	'id' : pd.Series(id),
	'url' : pd.Series(url),
	'log_date' : today}

df = pd.DataFrame(d)
cols = ['title', 'address', 'price', 'desc_br', 'contact_num',
		'num_beds', 'num_baths', 'num_receps', 'station1', 'station1_dist',
		'station2', 'station2_dist', 'lat', 'long', 'id', 'url', 'list_date', 'log_date']
df = df[cols]

#df.to_csv('test.csv', index = False)
# upload to Google Sheets
d2g.upload(df, sheets_id, sheet_name, row_names = False)