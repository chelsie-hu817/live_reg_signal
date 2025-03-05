import datetime

# Input the username and password to connect MongoDB
username = 'chelsie_hu'
password = 'q3AFVXhvENGbVHw4'

# Set data range
start_date = (datetime.datetime.today() - datetime.timedelta(days = 1)).replace(hour=0, minute=0, second=0, microsecond=0) # yesterday
end_date = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

# parameters
project_condor = 'condor'
project_saticoy = 'saticoy'
project_coso = 'coso'

gen_db_condor = 'data'
gen_db_coso = 'coso'
gen_db_saticoy = 'saticoy'

device_condor = 788
device_coso = 45
device_saticoy = 2

tag_condor = 2
tag_coso = 2
tag_saticoy = 2

project_id_condor = 17
project_id_coso = 3
project_id_saticoy = 1

# Save paths
path_coso = f'data/coso_{start_date.strftime("%m_%d")}.xlsx'
path_saticoy = f'data/saticoy_{start_date.strftime("%m_%d")}.xlsx'
path_condor = f'data/condor_{start_date.strftime("%m_%d")}.xlsx'