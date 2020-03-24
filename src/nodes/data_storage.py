import logging

logger = logging.getLogger('nodes.data_storage')


def update(client, params):
	"""
	Store results into the database.
	
	This node takes the treated dataframe and stores it 
	on our database.
	"""

	logger.info(f'Storing {params.table_name} in the {params.database} database.')

	# store dataframe in database
	params.dataframe.to_sql(params.table_name, 
							con=client.conn, 
							if_exists='fail', 
							index=False)

	logger.info(f'{params.table_name} written successfully to the {params.database} database.')

def done(client, params):
	"""
	This node is considered done if the table can be found 
	inside the database.
	"""	
	if client.conn.engine.has_table(params.table_name):
		logger.info(f'Table {params.table_name} found in the database.')
		logger.info(f'Skipping node.')

		return True

	logger.info('Node not up-to-date.')
	return False
