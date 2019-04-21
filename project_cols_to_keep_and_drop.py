cols_to_keep = ['ACRES_DEED',
                'ACRES_POLY',
                'AGPRE_ENRD',
                'AG_PRESERV',
                'BASEMENT',
                'CITY',
                'COOLING',
                'DWELL_TYPE',
                'EMV_BLDG',
                'EMV_LAND',
                'FIN_SQ_FT',
                'GARAGE', 
                'GARAGESQFT', 
                'GREEN_ACRE',
                'HOMESTEAD',
                'ID',
                'LANDMARK',
                'OWN_ADD_L1',
                'OWN_ADD_L2',
                'OWN_ADD_L3',
                'PARC_CODE',
                'PIN', 
                'SALE_VALUE', 
                'SPEC_ASSES',
                'TAX_CAPAC', 
                'TAX_EXEMPT', 
                'TOTAL_TAX', 
                'USE1_DESC',
                'USE2_DESC',
                'USE3_DESC',
                'USE4_DESC',
                'WSHD_DIST',
                'XUSE1_DESC',
                'XUSE2_DESC',
                'XUSE3_DESC',
                'XUSE4_DESC',
                'YEAR_BUILT',
                'Year',
                'centroid_lat',
                'centroid_long')]
                
cols_to_drop = ['AGPRE_EXPD',#drop
                'BLDG_NUM',#drop
                'BLOCK',#drop
                'CITY_USPS',#drop
                'COUNTY_ID',#drop
                'EMV_TOTAL',#drop
                'HEATING', #drop
                'HOME_STYLE',#drop
                'LOT',#drop
                'MULTI_USES',#drop
                'NUM_UNITS',#drop
                'OPEN_SPACE',#drop
                'OWNER_MORE',#drop
                'OWNER_NAME',#drop
                'PLAT_NAME', #drop
                'PREFIXTYPE', #drop
                'PREFIX_DIR', #drop
                'SALE_DATE', #drop
                'SCHOOL_DST', #drop
                'STREETNAME', #drop
                'STREETTYPE', #drop
                'SUFFIX_DIR', #drop
                'Shape_Area', #drop
                'Shape_Leng', #drop
                'TAX_ADD_L1', #drop
                'TAX_ADD_L2', #drop
                'TAX_ADD_L3', #drop
                'TAX_NAME', #drop
                'UNIT_INFO', #drop
                'ZIP',#drop
                'ZIP4',#drop
                ]