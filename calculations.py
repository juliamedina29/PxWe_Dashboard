import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import colors
import sqlalchemy as sql
import os

#------------------stargate parameters--------------------#
star_host = 'redshift-production.weworkers.io'
star_user = str(os.environ.get('STAR_USER'))
star_pwd = str(os.environ.get('STAR_PWD'))
star_db = 'analyticdb'
star_port = '5439'
redshift_string = 'redshift+psycopg2://{}:{}@{}:5439/{}'.format(star_user, star_pwd, star_host, star_db)

#------------------postgressql strings--------------------#
PxWe_spaces= """SELECT DISTINCT
pr.address1 AS project,
fl.description AS floor,
rm.name AS room,
rm.program_type AS space_type,
rm.number AS room_number,
rm.area_sf AS sf,
rm.desk_count AS desk_count
FROM stargate_bi_tables.bi_space as rm
INNER JOIN stargate_bi_tables.bi_floor as fl ON fl.current_harvest_sync_uuid = rm.harvest_sync_log_uuid
INNER JOIN stargate_bi_tables.bi_property as pr ON pr.uuid = fl.property_uuid
INNER JOIN stargate_bi_tables.bi_project as pj ON pj.property_uuid= pr.uuid
INNER JOIN stargate_bi_tables.bi_projecttype AS t ON pj.type_id=t.id
INNER JOIN stargate_bi_tables.bi_status as pjstat ON pjstat.id = pj.status_id
WHERE pjstat.name != 'Dead' AND ((t.name = 'PxWe') OR (t.name = 'Enterprise - Custom') OR (t.name = 'Enterprise - Off the Shelf')) AND sf > 0  
ORDER BY project, room_number"""

#------------------connection and df----------------------#
def fetch(connection_string, postgresql_string):

    engine = sql.create_engine(connection_string, connect_args={'sslmode': 'prefer'})
    with engine.connect() as conn, conn.begin():
        df = pd.read_sql(postgresql_string, conn)

    return df

#-------------Fetch Raw Data From Redshift----------------#
rooms = fetch(redshift_string, PxWe_spaces)

#-----------Function for finding avg of list--------------#
def Average(lst): 
    return sum(lst) / len(lst)

#-----------------set up color dictionary-----------------#
space_colors_dict = {
    'CIRCULATE':(1.0, 0.97, 0.87),
    'MEET':(0.71,0.94,0.85),
    'OPERATE': (0.87,0.87,0.87),
    'WE': (1.0,0.82,0.415),
    'WASH': (0.764,0.764,0.764),
    'WORK':(0.67,0.867,0.905),
    'SERVE': (0.251,0.752,0.753),
    'INFRASTRUCTURE': (0.87,0.87,0.87),
    'THRIVE': (1.0,0.82,0.415),
    'BASE': (0.87,0.87,0.87),
    'MEETING': (0.71,0.94,0.85),
    'OTHER': (0.87,0.87,0.87),
    'SUPPORT': (0.87,0.87,0.87),
    'TYPICAL OFFICE': (0.67,0.867,0.905),
    'WORKSTATIONS': (0.67,0.867,0.905),
    'EAT & DRINK': (1.0,0.82,0.415),
    'PLAY': (1.0,0.82,0.415),
    'HALLWAY': (1.0, 0.97, 0.87),
    'PHONE ROOM': (1.0,0.82,0.415),
    'VT': (0.87,0.87,0.87),
    'BREAKOUT': (0.71,0.94,0.85),
    'OUTDOOR': (1.0,0.82,0.415),
}

#---------------separate projects----------------#
proj_list = []
for x in rooms['project'].unique() :
    proj_list.append(x)

proj_count = len(proj_list)
print("Number of harvested projects: " + str(len(proj_list)))

#---------------create list of dataframes-----------------#
proj_dfs = []
for x in range(0, len(proj_list)) :
    indices = rooms['project'] == proj_list[x]
    proj_dfs.append(rooms.loc[indices,["project", "space_type","sf","desk_count"]])

#-----------create list of types per project--------------#
type_list = []
proj_types =[]
for x in range(0, len(proj_dfs)) :
    type_list = proj_dfs[x]['space_type'].unique()
    proj_types.append(type_list)

#----------create dict matching types to proj-------------#
keys = proj_list
values = proj_types
space_type_dict = dict(zip(keys, values))

#----create dict matching proj names to respective dfs----#
keys = proj_list
values = proj_dfs
proj_df_dict = dict(zip(keys, values))

#--------------------finding area sums--------------------#
proj_areas =[]
for y in range(0,proj_count) :
    cur_proj_name = proj_list[y]
    cur_proj_df = proj_df_dict[cur_proj_name]
    area_list = []
    for x in range(len(space_type_dict[cur_proj_name])) :
        new_area_sum = cur_proj_df.loc[cur_proj_df['space_type'] == space_type_dict[cur_proj_name][x], 'sf'].sum()
        area_list.append(new_area_sum)
    proj_areas.append(area_list)
proj_areas_dict = dict(zip(proj_list, proj_areas))

#--------------------find total area---------------------#
total_proj_areas =[]
for y in range(0,proj_count) :
    cur_proj_name = proj_list[y]
    cur_proj_df = proj_df_dict[cur_proj_name]
    total_area_sum = cur_proj_df['sf'].sum()
    total_proj_areas.append(total_area_sum)
lst = total_proj_areas    
avg = Average(lst)
print("Average area of all projects: " + str(round(avg, 2)) + " sf")

print("--------------------------------------------------------------------")

#------------------one project analysis-------------------#
#proj name for indexing#
first_proj_name = proj_list[5]
print("Project name: " + first_proj_name)
first_proj_df = proj_dfs[5]
first_proj_desk_count = first_proj_df['desk_count'].sum()
print("Desk count: " + str(first_proj_desk_count))
#set up list of space types in the project#
first_proj_space_list = space_type_dict[first_proj_name]
#set up list of area sums for each type#
first_proj_space_area_sums_list = proj_areas_dict[first_proj_name]
#create new df which only includes area sums per space type#
first_proj_sum_df = pd.DataFrame(first_proj_space_area_sums_list, index=first_proj_space_list)
#create plot#
first_proj_sum_df.columns = ['sf']
first_proj_color_list = []
for x in first_proj_space_list : 
    first_proj_color_list.append(space_colors_dict[x])
first_proj_sum_df.plot.pie(y='sf',colors=first_proj_color_list)
plt.show()
