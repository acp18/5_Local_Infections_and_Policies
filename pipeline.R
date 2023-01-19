

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.27a0e619-0e4a-4dd8-b06f-6d7e763da771"),
    county_daily_ma=Input(rid="ri.foundry.main.dataset.11d706f0-587c-40ed-a9da-601a12b3b4bd")
)
date_first_case <- function(county_daily_ma) {
    ##Subset dataset to only those counties with positive case counts and compute the minimum date per county
    df<-subset(county_daily_ma,covid19_total_cases>0)
     df1<-df %>% 
    group_by(local_code) %>% 
    mutate(first_case_date = min(date, na.rm = T))
    df2<-df1[,c("local_code","date","first_case_date")]
    ##Merge back into full dataset
    df3<-merge(county_daily_ma,df2,by=c("local_code","date"))

    return(df3)
}

