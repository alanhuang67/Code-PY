# -*- coding: utf-8 -*-
import get_page_content_base
import pandas as pd
from get_page_content_base import get_content_title_keyword

# 指定mongodb name
db_name = 'get_page_content'
db_table = 'table'

# 读取的档案名以及字段
input_file_name = u'京东商城_title_sample.csv'
input_file_column = 'url'

# 编码模式
input_file_encode = 'utf-8'
# input_file_encode = 'gb18030'


# Optupt file name
output_file_name = 'get_page_content_output.csv'


records = get_content_title_keyword(input_file_name, input_file_column, input_file_encode, output_file_name, db_name, db_table)
