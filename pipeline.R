

@transform_pandas(
    Output(rid="ri.vector.main.execute.f1fa590b-239c-483e-97ed-ab22856ea8ec")
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

