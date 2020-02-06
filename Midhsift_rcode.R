rm(list = ls(all = T))
setwd("D:\\cash4you\\MIDSHIFT\\March'17\\March'17\\hourwise\\MIDSHIFT-DOC\\R-output")
Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_151') 


#library(rJava)

library(RODBC)
#dbhandle = odbcConnect(dsn="c4y3r_winchklive", uid="bizacuity_read",pwd="bizacuity@14")
dbhandle = odbcConnect(dsn="c4y_winchk", uid="bizacuity",pwd="Cash$20!")
Data_d <-sqlQuery(dbhandle, "SELECT  trx_month,location,SUM(pdl_cnt) AS PDL
                  FROM ( SELECT  CONVERT(VARCHAR(7),pra_date,121) trx_month,
                  CONVERT(VARCHAR(10),pra_date,121) trx_date,
                  SUBSTRING(DATENAME(WEEKDAY,pra_date),1,3)  week_day,
                  DATEPART(HH,pra_date) HourFarmot,
                  LOCATION,
                  Count(1) AS pdl_cnt,
                  0 AS cc_cnt,
                  0 AS Wu_cnt,
                  0 serv_cnt			
                  FROM pra p WITH(NOLOCK)
                  WHERE 1=1
                  AND p.pra_date >= '2015-11-01'
                  AND p.pra_date <  CAST(DATEADD(DAY,-DAY(GETDATE())+1, CAST(GETDATE() AS DATE)) AS DATETIME)
                  AND reverse = 0                   
                  GROUP BY CONVERT(VARCHAR(7),pra_date,121),CONVERT(VARCHAR(10),pra_date,121),
                  SUBSTRING(DATENAME(WEEKDAY,pra_date),1,3) ,DATEPART(HH,pra_date) ,LOCATION
) Trans 
                  GROUP BY trx_month ,LOCATION
                  ORDER BY 1,2
                  
                  ")  #PDL
Data_e <- sqlQuery(dbhandle, "SELECT  trx_month,location,SUM(cc_cnt) AS CC
                   FROM (                  
                   SELECT CONVERT(VARCHAR(7),trx_date,121)  trx_month,
                   CONVERT(VARCHAR(10),trx_date,121) trx_date,
                   SUBSTRING(DATENAME(WEEKDAY,trx_date),1,3)  week_day,
                   DATEPART(HH,trx_date) HourFarmot,
                   LOCATION,
                   0 pdl_cnt,
                   Count(1) AS cc_cnt,
                   0 AS WU_cnt,
                   0 serv_cnt					
                   FROM bank_dep WITH(NOLOCK)
                   WHERE 1=1
                   AND issuer <> 'RI Payment'
                   AND trx_void IS NULL
                   AND trx_date >= '2015-11-01'
                   AND trx_date <  CAST(DATEADD(DAY,-DAY(GETDATE())+1, CAST(GETDATE() AS DATE)) AS DATETIME)
                   GROUP BY CONVERT(VARCHAR(7),trx_date,121),CONVERT(VARCHAR(10),trx_date,121),SUBSTRING(DATENAME(WEEKDAY,trx_date),1,3),
                   DATEPART(HH,trx_date),LOCATION                 
) Trans 
                   GROUP BY trx_month ,LOCATION
                   ORDER BY 1,2
                   
                   ") #CC
Data_f <- sqlQuery(dbhandle, "SELECT  trx_month,location,SUM(WU_cnt) AS WU
                   FROM ( 
                   SELECT     CONVERT(VARCHAR(7),trx_date,121) trx_month,
                   CONVERT(VARCHAR(10),trx_date,121) trx_date,
                   SUBSTRING(DATENAME(WEEKDAY,trx_date),1,3)  week_day,
                   DATEPART(HH,trx_date) HourFarmot,
                   trx_location Location,
                   0 AS  pdl_cnt,
                   0 AS  cc_cnt,
                   SUM(1) AS WU_cnt,
                   0 as serv_cnt
                   FROM svc_tran st WITH(nolock)
                   WHERE 1=1
                   AND st.trx_date >= '2015-11-01'
                   AND st.trx_date <  CAST(DATEADD(DAY,-DAY(GETDATE())+1, CAST(GETDATE() AS DATE)) AS DATETIME)
                   AND st.trx_void = 0
                   AND st.trx_svc IN ( 1,40,42 ) 
                   GROUP BY  CONVERT(VARCHAR(7),trx_date,121), CONVERT(VARCHAR(10),trx_date,121) ,
                   SUBSTRING(DATENAME(WEEKDAY,trx_date),1,3),DATEPART(HH,trx_date),trx_location
) Trans 
                   GROUP BY trx_month ,LOCATION
                   ORDER BY 1,2
                   
                   ")  #Wu
Data_g <- sqlQuery(dbhandle, "SELECT  trx_month,location,SUM(serv_cnt) AS SERV 
                   FROM (SELECT CONVERT(VARCHAR(7),trx_date,121) trx_month,
                   CONVERT(VARCHAR(10),trx_date,121) trx_date,
                   SUBSTRING(DATENAME(WEEKDAY,trx_date),1,3)  week_day,
                   DATEPART(HH,trx_date) HourFarmot,
                   trx_location Location,
                   0 AS  pdl_cnt,
                   0 AS  cc_cnt,
                   0 AS  WU_cnt,
                   SUM(1) as serv_cnt
                   FROM svc_tran st WITH(nolock)
                   WHERE 1=1
                   --AND trx_location=@loc
                   AND st.trx_date >= '2015-11-01'
                   AND st.trx_date <  CAST(DATEADD(DAY,-DAY(GETDATE())+1, CAST(GETDATE() AS DATE)) AS DATETIME)
                   AND st.trx_void = 0
                   AND st.trx_svc IN (2, 17, 18, 30, 31, 32, 33, 34, 35, 36, 37, 38 ) 
                   GROUP BY  CONVERT(VARCHAR(7),trx_date,121), CONVERT(VARCHAR(10),trx_date,121) ,
                   SUBSTRING(DATENAME(WEEKDAY,trx_date),1,3),DATEPART(HH,trx_date),trx_location
                   
                   
) Trans 
                   GROUP BY trx_month ,LOCATION
                   ORDER BY 1,2
                   
                   ") #other services
Storefcst <- data.frame(0,0,0,0,0)
names(Storefcst) <- c("location", "PDL_Counts","CC_Counts","WU_counts","serv_counts")
j<-1
for(j in 1:110)
{
  #if(j==1){
  Storefcst1 <- data.frame(0,0,0,0,0)
  names(Storefcst1) <- c("location", "PDL_Counts","CC_Counts","WU_counts","serv_counts")
  Store_1_PDL<- subset(Data_d, location == j)
  Store_1_cc<- subset(Data_e, location == j)
  Store_1_WU<- subset(Data_f, location == j)
  Store_1_serv<- subset(Data_g, location == j)
  if (nrow(Store_1_PDL)>0){
    Store1_PDL.ts <- ts(Store_1_PDL$PDL, start = c(2015,01,01), frequency = 365)
    Store_1_PDL.hwPDL <- HoltWinters(Store1_PDL.ts, gamma = F)
    forecast_1_PDL.hw <- as.data.frame(predict(Store_1_PDL.hwPDL, n.ahead = 1, prediction.interval = T, level = 0.90));
    Store1_cc.ts <- ts(Store_1_cc$CC, start = c(2015,01,01), frequency = 365)
    Store_1_cc.hwCC <- HoltWinters(Store1_cc.ts, gamma = F)
    forecast_1_cc.hw <- as.data.frame(predict(Store_1_cc.hwCC, n.ahead = 1, prediction.interval = T, level = 0.90));
    Store_1_WU$WU[3]<-Store_1_WU$WU[3]+5
    Store1_WU.ts <- ts(Store_1_WU$WU, start = c(2015,01,01), frequency = 365)
    Store_1_WU.hwCC <- HoltWinters(Store1_WU.ts, gamma = F)
    forecast_1_WU.hw <- as.data.frame(predict(Store_1_WU.hwCC, n.ahead = 1, prediction.interval = T, level = 0.90));
    Store_1_serv$SERV[3]<-Store_1_serv$SERV[3]+5
    Store1_Serv.ts <- ts(Store_1_serv$SERV, start = c(2015,01,01), frequency = 365)
    Store_1_Serv.hwServ <- HoltWinters(Store1_Serv.ts, gamma = F)
    forecast_1_Serv.hw <- as.data.frame(predict(Store_1_Serv.hwServ, n.ahead = 1, prediction.interval = T, level = 0.90));
    
    k<-nrow(forecast_1_PDL.hw)
    for(l in 1:k){
      Storefcst1$location<-j;
      Storefcst1$PDL_Counts<-forecast_1_PDL.hw$fit[l];
      Storefcst1$CC_Counts<-forecast_1_cc.hw$fit[l];
      Storefcst1$WU_counts<-forecast_1_WU.hw$fit[l];
      Storefcst1$serv_counts<-forecast_1_Serv.hw$fit[l];
      Storefcst<-rbind(Storefcst,Storefcst1 )
      Storefcst
    }
    
  }
  Storefcst
  #}
}


dbhandle2 = odbcConnect(dsn="replica1", uid="bizacuity_read",pwd="bizacuity@22")


sqlSave(dbhandle2,Storefcst,tablename="scheduling_input_1");

close(dbhandle2)

#write.csv(Storefcst, "June_monthlydate1.csv", row.names = T)
