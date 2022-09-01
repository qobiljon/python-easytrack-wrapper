class EnhancedDataSource:
	db_data_source: dict
	plot_str: str

	def __init__(self, db_data_source, plot_str=None):
		self.db_data_source = db_data_source
		self.plot_str = plot_str

	def attach_plot(self, plot_str):
		self.plot_str = plot_str

	def id(self):
		return self.db_data_source['id']

	def name(self):
		return self.db_data_source['name']

	def icon_name(self):
		return self.db_data_source['icon_name']

	def plot(self):
		return self.plot_str
