from pandas import DataFrame

class Params:
	"""
	Parameters class.

	This file centralizes anything that can be 
	parametrized in the code.
	"""


	# root url for the COVID-19 source files from John Hopkins University
	url = 'https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports'

	backup_folder = '../backup/'

	# if this is set to True, then all the nodes will be automatically 
	# considered not up-to-date and will be rerun.
	rerun = True 

	## Database connection params
	user = 'postgres'
	password = 'admin'
	host = 'localhost'
	database = 'corona'


	## attributes that are created on the fly
	dataframe = DataFrame()
	file = None
	files_to_download = None
	table_name = None
	filename = None
