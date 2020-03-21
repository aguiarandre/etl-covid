from bs4 import BeautifulSoup
import logging
import re
import requests 
import os

logger = logging.getLogger('nodes.data_preparation')


def run(client, params):
	""" Check files that are missing in our database. """

	response = requests.get(params.url)
	html = response.content
	soup = BeautifulSoup(html, 'lxml')

	# get raw csv link from url
	csv_files = ['https://github.com' + tag['href'] 
					for tag in soup.find_all('a') 
						if tag['href'].endswith('.csv')]

	# extract dates from github
	dates_in_github = [re.findall('\d{2}-\d{2}-\d{4}', file)[0] for file in  csv_files]


	# get dates for tables in our database
	dates_in_db = [text[0].replace('_','-') 
						for text in [re.findall('\d{2}_\d{2}_\d{4}', name) 
							for name in client.engine.table_names()] if len(text) > 0]
    
	missing_months = set(dates_in_github) - set(dates_in_db)
	
	logger.info(f'Missings months: {missing_months}')

	files_to_download = []

	for month in missing_months:
	    for file in csv_files:
	        if month in file:
	            files_to_download.append(file)

	params.files_to_download = files_to_download

	if params.rerun:


		logger.info('Rerun. Setting all months to missing_months.')
		missing_months = set(dates_in_github)
		params.files_to_download = csv_files

		for file in params.files_to_download:
			date = re.findall('\d{2}-\d{2}-\d{4}', file)[0].replace('-','_')
			params.filename = 'corona_' + date + '.csv'
			params.table_name = 'corona_' + date
			#remove file from cache
			logger.warning('RERUN')
			logger.info(f'Deleting {params.backup_folder + params.filename}')
			try:
				os.remove(params.backup_folder + params.filename)
			except:
				logger.warning(f'{params.backup_folder + params.filename} could not be deleted')
			logger.info(f'Dropping {params.table_name}.')
			client.conn.execute(f'DROP TABLE IF EXISTS {params.table_name}')

			
