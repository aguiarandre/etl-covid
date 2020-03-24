import logging
import os
import pandas as pd
import re
import requests 
from bs4 import BeautifulSoup

logger = logging.getLogger('nodes.data_gathering')


def update(client, params):
	""" 
	Download the csv files for the respective missing days. 
	"""

	logger.info(f'Gathering data from {params.file}')

	# enter the file url to extract the url of the raw csv file
	response = requests.get(params.file)
	html = response.content
	soup = BeautifulSoup(html, 'lxml')
	
	# get raw url	
	csv_url = 'https://github.com' + soup.find_all('div', attrs={'class':'BtnGroup'})[-1].find_all('a')[0]['href']
	
	# download the url to the dataframe
	df = pd.read_csv(csv_url)
	params.dataframe = df

	df.to_csv(params.backup_folder + params.filename, 
		sep=',', 
		index=False)

def done(client, params):
	"""
	This node is considered done if there is a backup 
	(cached) file stored in the backup_folder.
	"""

	date = re.findall('\d{2}-\d{2}-\d{4}', params.file)[0].replace('-','_')
	params.filename = 'corona_' + date + '.csv'
	params.table_name = 'corona_' + date

	logger.info(f'Looking for stored file named: {params.backup_folder + params.filename}')

	if os.path.exists(params.backup_folder + params.filename):
		# if the file exists, then just read from the backup/cache
		logger.info('File found in cache. Reading it now and skipping node.')
		params.dataframe = pd.read_csv(params.backup_folder + params.filename)
		return True

	if not os.path.exists(params.backup_folder):
		os.makedirs(params.backup_folder)

	logger.info('Dataframe not found in cache. The data should be gathered.')

	return False