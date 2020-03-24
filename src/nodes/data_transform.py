import logging
import pandas as pd

logger = logging.getLogger('nodes.data_transform')


def update(client, params):
	"""
	Normalize dataframe information.

	This function rename columns, transform date from string
	format to datetime format, normalize country names (from 
	Mainland China to China) and create a column called 
	`anomesdia` for future use. 

	Every data modification and manipulation should be done 
	in this file.
	"""
	logger.info('Transforming data.')
	df = params.dataframe
	
	# rename columns
	colnames = df.rename({'Province/State':'province', 'Country/Region':'country', 'Last Update': 'last_update'}, axis=1).columns
	df.columns = [col.lower() for col in colnames]

	# normalize date format
	df['last_update'] = pd.to_datetime(df['last_update'])

	# normalize names
	df.loc[df.country.str.contains('China'), 'country'] = 'China'

	# create column containing year-month-day (it sorts in the correct order)
	df['anomesdia'] = df.last_update.apply(lambda x : f'{str(x.year)}-{str(x.month).zfill(2)}-{str(x.day).zfill(2)}')

	params.dataframe = df
	
	logger.info(f'Dataset from {params.table_name} successfully treated.')
	
def done(client, params):
	""" 
	This is a placeholder function. 

	Data transformation is a step that 
	will always be executed and, therefore,
	should always return False (meaning the 
	node is not up-to-date).
	"""
	return False