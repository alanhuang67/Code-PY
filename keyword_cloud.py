import weibo_data
import pandas as pd

DF = pd.read_excel('keyword_cloud.xlsx', 0)

weibo_data.generate_content_term_cloud(DF['title'], 'keyword_cloud_output', csv=1)
