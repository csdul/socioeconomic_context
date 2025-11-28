import os
import numpy as np
import pandas as pd
import geopandas as gpd
import time


def drop_points_by_xy(df, out_file=''):
    """Drops any EPOI records with duplicate x/y coordinates"""
    orig_count = len(df)
    try:
        df['XY'] = list(zip(df.X, df.Y))
    except AttributeError:
        df['XY'] = list(zip(df.LONGITUDE, df.LATITUDE))

    df['XY'] = df['XY'].astype(str)  # tuples are unsupported in shapefile output
    df_out = df.drop_duplicates(subset=['XY'])  # keeps first point only
    
    # return count of points filtered out
    post_count = len(df_out)
    dropped_records = orig_count - post_count

    # optionally write dropped points to disk
    if out_file and dropped_records > 0:
        diff = df.loc[df.index.difference(df_out.index)]
        diff.to_file(out_file)

    return df_out, dropped_records
    

def drop_points_by_keywords(df, keyword_list, out_file=''):
    """Drops any EPOI records that contain specific keywords in their 'NAME' field"""
    orig_count = len(df)
    df_out = df.copy()
    for keyword in keyword_list:
        df_out = df_out[~df_out['NAME'].str.contains(keyword.upper())]  # field is all caps
    
    post_count = len(df_out)
    dropped_records = orig_count - post_count

    # optionally write dropped points to disk
    if out_file and dropped_records > 0:
        diff = df.loc[df.index.difference(df_out.index)]
        diff.to_file(out_file)

    return df_out, dropped_records


def drop_duplicate_point_names(df, out_file=''):
    """Drops any EPOI records that contain exact duplicates in their 'Name' field"""
    orig_count = len(df)
    df_out = df.drop_duplicates(subset=['NAME'])  # keeps first point only
    
    # return count of points filtered out
    post_count = len(df_out)
    dropped_records = orig_count - post_count

    # optionally write dropped points to disk
    if out_file and dropped_records > 0:
        diff = df.loc[df.index.difference(df_out.index)]
        diff.to_file(out_file)

    return df_out, dropped_records


start = time.time()

# Some constants
save_shp = True                                # save output counts as polyon geodatframe
save_points = True                             # save point shapefile of each extracted category
drop_xy = True                                 # drop points with identical x/y coordinates
drop_duplicates = False                        # drop points with duplicate NAME field entry (exact match only)
drop_keywords = ['LIMO', 'VIP']                # remove private limousine transportation
keyword_drop_codes = ['41110000', '41190000']  # SIC codes of points to drop keywords from 
code_type = 'SIC'

# output naming conventions are based on EPOI year
epoi_year = 2015
out_dir = f'C:/Users/Sean/Documents/csdul/Data/counts_expanded_fhrtr/{epoi_year}'
out_csv = f'{out_dir}2011_ct_{epoi_year}_epoi_fhrtr_expanded.csv'
if not os.path.exists(out_dir):
    os.makedirs(out_dir)

# Define filepath to EPOI shapefile and import as geodataframe
epoi_dir = 'C:/Users/Sean/Documents/csdul/Data/EPOI/merged'
epoi_fp = f'{epoi_dir}/canada_epoi_{epoi_year}.shp'
if 'epoi_gdf' not in globals():  # avoids repeat imports of large dataset
    epoi_gdf = gpd.read_file(epoi_fp)

# Define codes to loop through
sic_codes = [41110000, 41310000, 41510000, 41190000,            # Transportation and Mobility
             80110000, 80210000, 80410000, 80420000, 80490000,  # Health and Medical Services
             83510000, 82310000,                                # Education and Childcare
             83310000, 83220000,                                # Social and Community Services
             79910000, 79970000,                                # Recreation and Wellness
             84120000, 86610000]                                # Religion and Culture

if code_type == 'SIC':  # option to add additional coding types in the future
    code_list = sic_codes

# Import census geography with GPD and clean columns
geog_dir = 'C:/Users/Sean/Documents/csdul/Data/boundaries/census_tracts'
geog_fp = f'{geog_dir}/canada_ct_2011.shp'
boundaries = gpd.read_file(geog_fp)

# Reproject if necessary
if boundaries.crs != epoi_gdf.crs:
    boundaries = boundaries.to_crs(epoi_gdf.crs)

# list of dicts to store filter results
dict_list = []

# Loop over each SIC code in list
for code in code_list:
    stat_dict = {}
    subset_bounds = boundaries.copy()
    subset_gdf_list = []

    if code_type =='SIC':
        coded_points = epoi_gdf[epoi_gdf['SIC_1'] == str(code)]
    
    orig_count = len(coded_points)
    stat_dict['sic'] = code
    stat_dict['original_count'] = orig_count

    # drop points based on list of keywords
    if drop_keywords and code in keyword_drop_codes:
        coded_points, keyword_count = drop_points_by_keywords(coded_points, drop_keywords, out_file=f'{out_dir}/drop_keyword_{code}.shp')
        stat_dict['keyword_count'] = keyword_count

    # drop points with duplicate coordinates
    if drop_xy:
        coded_points, xy_count = drop_points_by_xy(coded_points, out_file=f'{out_dir}/drop_xy_{code}.shp')
        stat_dict['xy_count'] = xy_count

    # drop points with duplicate names
    if drop_duplicates:
        coded_points, name_count = drop_duplicate_point_names(coded_points, out_file=f'{out_dir}/drop_name_{code}.shp')
        stat_dict['name_count'] = name_count

    # log point count after applying all filters
    final_count = len(coded_points)
    stat_dict['final_count'] = final_count
    dict_list.append(stat_dict)    
    print(f'{final_count} total points for code {code}')

    # optionally export shapefile of points for each code
    if save_points == True:
        if len(coded_points) > 0:
            coded_points.to_file(f'{out_dir}/{code_type}_{code}.shp'.lower())

    # Perform spatial join between remaining coded points and polygon boundaries
    census_geog_id = boundaries.columns[0]
    joined = gpd.sjoin(coded_points, subset_bounds, how="left", predicate='intersects')
    sic_code_counts = joined.groupby(census_geog_id).size().reset_index(name=code)
   
    # Create new series with row-wise list of point sums
    boundaries = boundaries.merge(sic_code_counts, on=census_geog_id, how='left')
    boundaries[code] = boundaries[code].fillna(0).astype(np.int16)

joined_boundaries = boundaries.drop(columns=['geometry'])

# Always save CSV with counts, option to save SHP for easier spot checks in a desktop GIS
joined_boundaries.to_csv(out_csv, index=False)
if save_shp == True:
    boundaries.columns = boundaries.columns.map(str)
    boundaries.to_file(out_csv.replace('.csv', '.shp'))

# save additional CSV with filtering results
stat_df = pd.DataFrame(dict_list)
stat_df.to_csv(out_csv.replace('.csv', '_filter_results.csv'))

end = time.time()
print(f'Elapsed time: {round((end - start) / 60, 1)} minutes')
