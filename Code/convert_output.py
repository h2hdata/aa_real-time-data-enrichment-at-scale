import os
import ast
import pandas as pd
import shutil
# abs_path = os.path.abspath(os.path.dirname(__file__))

df = pd.DataFrame(columns=['UserID','State','Hobbies'])
"""
 RESULTS
---------
"""

for dir_,subdir,files in os.walk('../Output/Raw_output/'):
	if files:
		for file_ in files:
			if 'part' in file_ and '.crc' not in file_:
				with open(dir_+'/'+file_) as f:
					data = [ast.literal_eval(user.strip()) for user in f.readlines() if user.strip() != 'None']
					df = df.append(pd.DataFrame(data,columns=['UserID','State','Hobbies']))

df = df.reset_index(drop=True)
shutil.rmtree('../Output/Raw_output/', ignore_errors=False, onerror=None)
df.to_csv('../Output/Output_States_Hobbies.csv')
print df