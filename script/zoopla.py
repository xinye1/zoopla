
import bs4, requests, sys, re, datetime
import pandas as pd
from selenium import webdriver
from df2gspread import gspread2df as g2d
from df2gspread import df2gspread as d2g


def query_zoopla(loc = 'northwood', min_bed = 3, max_bed = 5, min_price = 900000, max_price = 1100000, driver = webdriver.Chrome('chromedriver.exe')):

#	import bs4, requests, sys, re, datetime
#	import pandas as pd
#	from selenium import webdriver

	#chrome = webdriver.Chrome('C:/Users/xi373146/Downloads/chromedriver.exe')
	#chrome.get('http://www.zoopla.co.uk/')
	chrome = driver

	timestamp = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
	zoopla_url = 'wwww.zoopla.co.uk'
	search_box_selector = '#search-input-location'
	min_price_selector = '#forsale_price_min'
	max_price_selector = '#forsale_price_max'
	type_selector = '#property_type'
	beds_selector = '#beds_min'
	url_search_northwood = 'http://www.zoopla.co.uk/for-sale/houses/london/' + loc + '/?beds_min=' + \
		str(min_bed) + '&beds_max=' + str(max_bed) + '&price_max=' + str(max_price) + '&price_min=' + str(min_price) + \
		'&q=' + loc + '&results_sort=newest_listings'
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
		print('\r Processing listing ' + str(count) + ' of ' + str(len(listings)) + '...')
		
		# check if the li element is an actual listing by checking the data-listing-id attribute
		if l.get_attribute('data-listing-id') == None:
			print('Not a proper listing, next...')
			continue
		else:
			left = l.find_element_by_css_selector('div > div.listing-results-left > div')
			right = l.find_element_by_css_selector('div > div.listing-results-right.clearfix')
			footer = l.find_element_by_css_selector('div > div.listing-results-footer.clearfix')
			
			title.append(right.find_element_by_css_selector('h2 > a').text)
			address.append(right.find_element_by_css_selector('span > a').text)
			
			lat_long = right.find_elements_by_css_selector('div > meta')
			lat.append(lat_long[0].get_attribute('content'))
			long.append(lat_long[1].get_attribute('content'))
			
			price_raw = right.find_element_by_css_selector('div > a').text
			try:
				price_appendix = right.find_element_by_css_selector('div > a > span').text
			except:
				price_appendix = ''
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
		'log_timestamp' : timestamp}

	df = pd.DataFrame(d)
	cols = ['title', 'address', 'price', 'desc_br', 'contact_num',
			'num_beds', 'num_baths', 'num_receps', 'station1', 'station1_dist',
			'station2', 'station2_dist', 'lat', 'long', 'id', 'url', 'list_date', 'log_timestamp']
	df = df[cols]
	
	return df


def get_latest_gs(sheets_id = '1i7wVz-uP9kgLwNRWyRBvmQ9TTuEx9i0xi23GfSA7sSw', sheet_name = 'northwood'):
	try:
		df = g2d.download(sheets_id, sheet_name, row_names = False, col_names = True)
		return df
	except TimeoutError:
		import sys
		sys.exit()
	
def get_ids(df):
	return list(df.id)
	
def get_new_ids(old_df, new_df):
	old = get_ids(old_df)
	new = get_ids(new_df)
	return list(set(new) - set(old))
	
def get_new_rows(old_df, new_df):
	new_ids = get_new_ids(old_df, new_df)
	return new_df[new_df.id.isin(new_ids)]
	
	
def upload_new_rows(existing_gs, new_zoopla, sheets_id = '1i7wVz-uP9kgLwNRWyRBvmQ9TTuEx9i0xi23GfSA7sSw', sheet_name = 'northwood'):
	
	df_to_upload = get_new_rows(existing_gs, new_zoopla)
	rows_of_update = df_to_upload.shape[0]
	
	# check if there's anything to update
	if rows_of_update == 0:
		print("There's nothing to update.")
		import sys
		sys.exit()
	else:
		f_listing = lambda x : x == 1 and 'listing' or 'listings'
		print('Uploading %d new %s to Google Sheets...' % (rows_of_update, f_listing(rows_of_update)))
		
		# find the row number the new data to go into
		new_row_num = existing_gs.shape[0] + 2
		start_c = 'A' + str(new_row_num)
		
		# upload, without headers
		d2g.upload(df_to_upload, sheets_id, sheet_name, row_names = False, col_names = False, start_cell = start_c, clean = False)
		print('Done!')
