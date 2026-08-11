
# Civic, Community, and Neighbourhood Stability Resources

The **Civic, Community, and Neighbourhood Stability Resources (CCNSR)** represent institutional and community resources associated with civic support, access to services, and neighbourhood stability.

These indicators capture neighbourhood-level infrastructure related to community support, access to essential services, and different dimensions of neighbourhood stability.

## Indicators

These include:

- **Healthcare services**
  Examples: general medical and surgical hospitals  
  SIC code: `8062`

- **Legal services**
  Examples: law offices and legal service providers  
  SIC code: `8111`

- **Residential care**
  Examples: assisted living facilities, group homes, and other residential care facilities  
  SIC code: `8361`

- **Social services**
  Examples: advocacy organizations, social agencies, and other social service organizations  
  SIC code: `8399`

- **Drinking establishments**
  Examples: bars and nightclubs  
  SIC code: `5813`

- **Liquor stores**
  SIC code: `5921`

- **Personal credit institutions**
  Examples: short-term loan establishments and payday loan providers  
  SIC code: `6141`

- **Hotels and motels**
  SIC code: `7011`

- **Rooming and boarding houses**
  Examples: shelters, hostels, and boarding houses  
  SIC code: `7021`

- **Coin-operated laundries and drycleaning**
  Examples: laundromats  
  SIC code: `7215`

## Methodology

The CCNSR indicators are derived through a structured process involving **data collection, classification, spatial aggregation, counting, and quality control** across census tracts.

### Data Collection and Classification

CCNSR indicators are based on the **Enhanced Points of Interest (EPOI)** dataset from **DMTI Spatial**, which provides geospatial information on community resources across Canada, including geographic coordinates and categorical information.

The raw EPOI datasets are obtained through **Scholars GeoPortal** in point shapefile format. Census geography boundary files are obtained from **Statistics Canada** and used to spatially aggregate resources within census tracts.

Resources are categorized using **Standard Industrial Classification (SIC) codes**, which identify establishments according to their primary economic activity. SIC codes are used to identify the resource categories included in the CCNSR indicators.

The data are provided by **Digital Map Technologies Inc. (DMTI)**, a Canadian company specializing in geospatial data and mapping solutions. DMTI provides high-precision location-based datasets used in academic, government, and business research.

The raw data are accessible to researchers at universities that hold a **Scholars GeoPortal license**.

More information on **DMTI Spatial** and **SIC codes** can be found [here](https://www.ic.gc.ca/eic/site/cis-sic.nsf/eng/home).

### Counting Resources by Census Tract

For each year, the DMTI EPOI dataset and census tract boundaries are loaded using **Python**.

For every SIC code associated with a CCNSR resource, points corresponding to that classification are selected and spatially assigned to census tract polygons.

The number of resources belonging to each CCNSR category is then counted within each census tract.

The resulting dataset contains the number of identified resources by **Census Tract Unique Identifier (CTUID)**.

Higher values indicate a greater number of resources belonging to the corresponding classification within the census tract.

### Quality Control and Spot Checking

A spot-checking process was conducted to evaluate the validity and reliability of the CCNSR indicators.

For each selected SIC code, census tracts were manually inspected using **ArcGIS Pro**. The number and type of establishments identified in the data were compared with their geographic locations and, when possible, verified using **Google Earth historical imagery** for the corresponding year.

The process was designed to verify:

- whether identified points represented the intended type of resource;
- whether the number of establishments was correctly counted;
- whether establishments were assigned to the correct census tract; and
- whether duplicate or incorrectly classified establishments were affecting the indicators.

Following the spot-checking process, filtering procedures were implemented to reduce redundant or irrelevant observations. Points with exactly the same latitude and longitude coordinates were treated as duplicates for census geography counts.

Both **raw counts** and **filtered CCNSR counts** are retained to allow researchers to evaluate the effect of these quality-control procedures.

### Known Data and Classification Limitations

The spot-checking process identified several issues that users should consider when working with the CCNSR indicators:

- **Healthcare services (SIC 8062):** counts may be inflated when multiple services or naming variations represent the same healthcare facility.

- **Legal services (SIC 8111):** individual lawyers located within the same law firm may appear as separate establishments, potentially inflating counts.

- **Social services (SIC 8399):** this broad classification may include organizations that are not directly related to social or human services.

- **Hotels, motels, rooming houses, and residential services (SIC 7011/7021):** some establishments may be incorrectly classified across these categories.

The indicators also measure the **presence and number of resources**, but do not directly measure characteristics such as resource capacity, quality, suitability, or actual accessibility to residents.

Researchers should therefore consider these limitations when interpreting or using the indicators.

### Using the Indicators

The basic CCNSR variables are provided as **raw resource counts**.

Researchers interested in comparing census tracts with different population sizes may standardize the resource counts by population.

For descriptive comparisons, standardized counts may also be transformed into **z-scores**, allowing different resource categories to be compared on a common scale.

Using the **CTUID**, CCNSR indicators can also be joined with other datasets available at the census tract level or aggregated to coarser census geographies.

## [Datasets](https://github.com/csdul/pre_beta_datasets)

- DMTI Spatial – Enhanced Points of Interest (EPOI)
- Statistics Canada – Census geography boundaries
- Census

## Documentation

For additional information about the construction, validation, and interpretation of the indicators, see:

- [**Indicator Development and Screening – CCNSR**](https://github.com/csdul/socioeconomic_context/blob/main/civic_community_and_neighbourhood_stability_resources/documents/indicator_development_and_screening_ccnsr.docx)
- [**Spot Check Queries and Recommendations – CCNSR**](https://github.com/csdul/socioeconomic_context/blob/main/civic_community_and_neighbourhood_stability_resources/documents/spot_check_queries_and_recommendations_node%203_ccnsr.docx)
- [**Add Inputs to CSDUL – CCNSR Indicators and Counts**](https://github.com/csdul/socioeconomic_context/blob/main/civic_community_and_neighbourhood_stability_resources/documents/add_nputs_to_csdul_node%203_ccnsr_indicators_and_counts.doc)
- [**Civic, Community, and Neighbourhood Stability Resources Codebook**](https://github.com/csdul/socioeconomic_context/blob/main/civic_community_and_neighbourhood_stability_resources/documents/civic_community_and_neighbourhood_stability_resources_codebook_node%203.docx)

## Files

- [**Codes**](https://github.com/csdul/socioeconomic_context/tree/main/civic_community_and_neighbourhood_stability_resources/codes): Contains the Python scripts and algorithms used to extract, classify, spatially aggregate, and calculate the CCNSR indicators.

- [**Documents**](https://github.com/csdul/socioeconomic_context/tree/main/civic_community_and_neighbourhood_stability_resources/data): Includes detailed documentation describing the methodology, indicator development, quality-control procedures, spot-checking process, assumptions, limitations, and codebook.

- [**Data**](https://github.com/csdul/socioeconomic_context/tree/main/civic_community_and_neighbourhood_stability_resources/data): Contains the raw and/or processed data used to calculate the CCNSR indicators.

- [**Results**](https://github.com/csdul/socioeconomic_context/tree/main/civic_community_and_neighbourhood_stability_resources/results): Contains tables and datasets with the calculated CCNSR indicators, including resource counts by census tract.
