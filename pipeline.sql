

@transform_pandas(
    Output(rid="ri.vector.main.execute.df7b25fa-ace6-44c1-b8af-ae00c57f6c94"),
    county_daily_status=Input(rid="ri.foundry.main.dataset.99814779-5924-4e29-8df1-5d80b4da5cf7")
)
SELECT
   region_id,mapbox_geoid,state_name,state_abbr,date,local_code,covid19_total_cases,
   covid19_new_cases,
   AVG(covid19_new_cases)
         OVER(PARTITION BY local_code ORDER BY local_code, date  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
         AS moving_average
FROM county_daily_status

@transform_pandas(
    Output(rid="ri.vector.main.execute.13a5ce68-56df-4a08-90a1-baeedd167b74"),
    BU_SC_SDOH_N3C_2018_20201130=Input(rid="ri.foundry.main.dataset.f9fb2781-bed3-421e-bb57-6eaa24ddd85d"),
    county_daily_status=Input(rid="ri.foundry.main.dataset.99814779-5924-4e29-8df1-5d80b4da5cf7")
)
SELECT a.*, b.CBSA_CODE, b.CBSA_Name
FROM county_daily_status a LEFT JOIN BU_SC_SDOH_N3C_2018_20201130 b
ON a.local_code = b.FIPS_CODE

