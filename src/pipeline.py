from nodes import data_gathering
from nodes import data_transform
from nodes import data_storage
from nodes import data_viz
from nodes import data_preparation

from params import Params 
from client import Client
import logging

def process(client, params):  
	"""
	The ETL pipeline.

	It contains the main nodes of the extract-transform-load 
	pipeline from the COVID-19 process. Mainly, it prepares 
	which files are necessary to download from the url-source. 
	Then, it goes through each node by performing the operations 
	described, namely: gathering, transforming, storing and 
	visualizing the information.

	"""
	data_preparation.run(client, params)

	for file in params.files_to_download:
		params.file = file

		if not data_gathering.done(client, params):
			data_gathering.update(client, params)

		if not data_transform.done(client, params):
			data_transform.update(client, params)

		if not data_storage.done(client, params): 
			data_storage.update(client, params)

	if not data_viz.done(client, params):
		data_viz.update(client, params)

if __name__ == '__main__': 

	logging.basicConfig(filename='dump.log',
						level=logging.INFO,
						format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    					datefmt='%Y-%m-%d %H:%M:%S')

	params = Params()
	client = Client(params)
	process(client, params)