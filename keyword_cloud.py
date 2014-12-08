import weibo_data
import pandas as pd

# read file into DF
DF = pd.read_excel('keyword_cloud.xlsx', 0)

''' generate keyword calculation

weibo_data.generate_content_term_cloud([file.title].[outpot_file_name],[write to csv: 1=true])

'''
weibo_data.generate_content_term_cloud(DF['title'], 'keyword_cloud_output', csv=1)
