#! python3

# first of all, check if there's a connecton to Google API
from df2gspread import gspread2df as g2d

def main():
	print('Testing Google API connection...')
	sheets_id = '1i7wVz-uP9kgLwNRWyRBvmQ9TTuEx9i0xi23GfSA7sSw'
	sheet_name = 'northwood'
	try:
		gs = g2d.download(sheets_id, sheet_name, row_names = False, col_names = True)
	except TimeoutError:
		print('No connection, programme stops.')
		import sys
		sys.exit()

	print('Connected, continue...')

	import script.zoopla as zpl

	print('Getting the latest from Zoopla...')
	full_zoopla = zpl.query_zoopla()

	# print('Getting the current Google Sheets...')
	# gs = zpl.get_latest_gs()

	zpl.upload_new_rows(existing_gs = gs, new_zoopla = full_zoopla)
	
if __name__ == '__main__':
	main()