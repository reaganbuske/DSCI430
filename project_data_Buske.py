import pandas as pd

df = pd.read_csv('./data/parcel_common_columns_2004_2014.csv')
common_columns = df["0"].to_list()


#part 2 _________________________________

from dfply import *
import pandas as pd
lakes = pd.read_csv('./data/MinneMUDAC_raw_files/mces_lakes_1999_2014.csv', header = 0, sep = ',', engine = 'python') 

lakes.latitude = lakes.latitude.round(5).astype(str)
lakes.longitude = lakes.longitude.round(5).astype(str)

parcel = pd.read_csv('./data/MinneMUDAC_raw_files/Parcel_Lake_Monitoring_Site_Xref.txt', header = 0, sep = '\t', engine = 'python')

parcel.centroid_lat = parcel.centroid_lat.round(5).astype(str)
parcel.centroid_long = parcel.centroid_long.round(5).astype(str)


lakesset = set(lakes.DNR_ID_Site_Number)
parcelset = set(parcel.Monit_MAP_CODE1)

common_codes = lakesset.intersection(parcelset)

(lakes >> 
     group_by(X.DNR_ID_Site_Number) >>
     summarize(cnt = n(X.DNR_ID_Site_Number)) >>
     filter_by(~X.DNR_ID_Site_Number.isin(common_codes))  
)

(parcel >>
     group_by(X.Monit_MAP_CODE1) >>
     summarize(cnt = n(X.Monit_MAP_CODE1)) >>
     filter_by(~X.Monit_MAP_CODE1.isin(common_codes)) 
)

lake2 = (lakes >>
             select(X.LAKE_NAME, X.DNR_ID_Site_Number)>>
             group_by(X.LAKE_NAME, X.DNR_ID_Site_Number)>>
             summarize(cnt = n(X.DNR_ID_Site_Number)) >>
             filter_by(X.DNR_ID_Site_Number.isin(common_codes))>>
             drop(X.cnt)
)


latlongset = set(zip(parcel.centroid_lat, parcel.centroid_long))
lat_long_set = lambda df: set(zip(df.centroid_lat, df.centroid_long))


lakelatlong = (lakes >>
             select(X.latitude, X.longitude, X.DNR_ID_Site_Number)>>
             group_by(X.latitude, X.longitude, X.DNR_ID_Site_Number)>>
             filter_by(X.DNR_ID_Site_Number.isin(common_codes)) >>
             summarize(cnt = n(X.DNR_ID_Site_Number)) >>
             drop(X.cnt)
)

latlong_to_code = {ID:code for ID, code in zip(zip(parcel.centroid_lat, parcel.centroid_long), parcel.Monit_MAP_CODE1)}
code_to_latlong = {code:iD for iD,code in zip(zip(parcel.centroid_lat, parcel.centroid_long) ,parcel.Monit_MAP_CODE1)}
code_to_name = {iD:name for iD,name in zip(lake2.DNR_ID_Site_Number, lake2.LAKE_NAME)}
latlong_to_name = {latlong:code_to_name[code] for latlong,code in latlong_to_code.items() if code in code_to_name}
code_to_distance = {code:distance for code,distance in zip(parcel.Monit_MAP_CODE1,parcel.Distance_Parcel_Monitoring_Site_meters)}
latlong_to_distance = {iD:code_to_distance[name] for iD,name in latlong_to_code.items()}

###########
lakes_by_year = pd.read_csv('./data/MinneMUDAC_raw_files/lakes_by_year.csv')
lakes_w_complete_measurement = pd.read_csv('./data/MinneMUDAC_raw_files/lakes_w_complete_measurement.csv')
lake_names_to_keep = lakes_w_complete_measurement.LAKE_NAME
lake_codes_to_keep = lakes_w_complete_measurement.DNR_ID_Site_Number


########
final_parcels = pd.read_csv('./data/MinneMUDAC_raw_files/final_parcels.csv')