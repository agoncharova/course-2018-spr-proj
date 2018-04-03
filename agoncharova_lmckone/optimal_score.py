from sklearn import preprocessing
from scipy.stats.stats import pearsonr
import pandas as pd
import prov.model
import datetime
import uuid
import dml
import z3

class optimal_score(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = ['agoncharova_lmckone.stability_score']
	writes = ['agoncharova_lmckone.optimal_score']

	# HELPER

	def get_stability_scores_from_repo():
		'''
		Gets and returns the data from the stability_score collection.
		'''
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		stability_score_collection = repo['agoncharova_lmckone.stability_score']

		stability_scores_cursor = stability_score_collection.find()
		stability_scores_arr = []
		for stability_score_object in stability_scores_cursor:
			stability_scores_arr.append(stability_score_object)
		repo.logout()
		return stability_scores_arr

	def explore_business_data(all_data):
		'''
		Normalize business data and compute correlation coefficient 
		between the number 
		'''
		# normalize the business scores for each entry
		# inspiration: http://sebastianraschka.com/Articles/2014_about_feature_scaling.html#about-standardization
		df = pd.DataFrame(all_data)
		# print(df)

		std_scale = preprocessing.StandardScaler().fit(df[['businesses']])
		df_std = std_scale.transform(df[['businesses']])
		# print(df_std[:10])
		
		minmax_scale = preprocessing.MinMaxScaler().fit(df[['businesses']])
		df_minmax = minmax_scale.transform(df[['businesses']])
		# print(df_minmax[:25])

		stability_list = list(df['stability'].as_matrix())
		print(len(stability_list))
		df_minmax_formatted = [x[0] for x in df_minmax]
		print(len(df_minmax_formatted))
		# find correlation between the normalized business scores and the optimal score
		corr = pearsonr(stability_list, df_minmax_formatted)  # corr = 0.10446045881042658, 2-tailed p-val = 0.13704210138510017 
		# non zero correlation of 0.1 obviously means it is CORRELATED
		# let's optimize
		return corr[0]

	def compute_optimal_num_businesses(all_data):
		'''
		Assuming that adding 2 businesses will decrease the score by 0.01,
		we want to see how many businesses we can add to an area using z3.
		High number of evictions and crimes lead to a higher score, so 
		we want to lower the score. 
		'''
		# setup
		# S = z3.Solver()
		# (x1,x2) = [z3.Real('x'+str(i)) for i in range(1,3)]
		# S.add(x2 <= 7, x3 <= 8, x4 <= 6)
		df = pd.DataFrame(all_data)

		# isolate and format the vars
		businesses = list(df['businesses'].as_matrix())
		stability = list(df['stability'].as_matrix())
		additional_businesses = [0]*len(businesses)
		print(additional_businesses)
		print(businesses)
		print(stability)
		# if score is already <= 1, don't touch it


		return 0


	@staticmethod
	def execute(trial=False):
		'''
		Incorporate the number of businesses into the tract stability score 
		by first normalizing it in construct_business_score.	
		Run an SMT solver to see how many businesses could be added
		to a tract in order to increase stability score.
		'''
		startTime = datetime.datetime.now()
		
		this = optimal_score		

		all_data = this.get_stability_scores_from_repo()
		# corr = this.explore_business_data(all_data)
		this.compute_optimal_num_businesses(all_data)
		return {"start": startTime, "end":  datetime.datetime.now()}

	@staticmethod
	def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

		this_agent = doc.agent('alg:agoncharova_lmckone#optimal_score',
		                      	{prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

		this_entity = doc.entity('dat:agoncharova_lmckone#optimal_score',
                            {prov.model.PROV_LABEL: 'Optimal Score', prov.model.PROV_TYPE: 'ont:DataSet'})
		
		optimal_score_resource = doc.entity('dat:agoncharova_lmckone#optimal_score',
		                  {prov.model.PROV_LABEL: 'Optimal Score', prov.model.PROV_TYPE: 'ont:DataSet'})

		get_optimal_score = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

		doc.usage(get_optimal_score, optimal_score_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
		
		doc.wasAssociatedWith(get_optimal_score, this_agent)
		doc.wasAttributedTo(this_entity, this_agent)
		doc.wasGeneratedBy(this_entity, get_optimal_score, endTime)
		doc.wasDerivedFrom(this_entity, optimal_score_resource, get_optimal_score, get_optimal_score, get_optimal_score)

		repo.logout()

		return doc

optimal_score.execute()
optimal_score.provenance()