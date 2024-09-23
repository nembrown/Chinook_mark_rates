

buildcon <- function(pwd){
  pwd <- "Lt#v68gp"
  user <- "OTOLITH_READ_USER"
  host <- "VSBCIOSXP75.ENT.DFO-MPO.CA"
  port <- 1523
  svc = "OIOSP01"
  drlst <- odbcListDrivers()[[1]]
  drvr <- unique(drlst[grep('Oracle',drlst)])[[1]]
  tcon <- DBI::dbConnect(odbc::odbc(),
                         Driver = drvr,
                         DBQ = paste0(host,":",port, "/" , svc),
                         Uid    = user,
                         Pwd   = pwd)
  return (tcon)
}

PullAllSport <- function(start_year) {

c_lic.years <- NULL
for  (y in start_year:format(now(),"%Y")) {
  c_lic.years <- append(c_lic.years, y)
}
c_lic.year.str <- paste(c_lic.years, collapse=",")

con <- buildcon(pwd)

Q=dbSendQuery(con,"Select * from otolith_v1.creel_queries where use = -1 and name = 'iRec - Data for calibrations'")
F=dbFetch(Q)
out <- paste0("Results_",format(Sys.Date(), "%Y_%m_%d_"),format(Sys.time(), "%H_%M_%S"),"\\")

defaultW <- getOption("warn") 
options(warn = -1) 
s=F$SQL[1]
ss = strsplit(s,split=";;")
s2 = unlist(strsplit(unlist(ss), ";;"))
s2 = s2[[2]]
s2a = gsub("XXXYEARXXX",c_lic.year.str,s2)
Q = dbSendQuery(con, s2a)

message("Loading Creel Quality Report")
inclusionsR <- dbFetch(Q)
inclusionsR$ID<-paste(inclusionsR$AREA,"-",inclusionsR$MONTH,"-",inclusionsR$YEAR) 
xlname <- paste0(out, "\\Sport_data_set_", "Creel_Quality_", format(now(), "%Y%m%d_%H%M%S_"),".csv")
inclusionsRaw <- inclusionsR
# inclusionsR <- inclusionsR[inclusionsR$INCLUDE_20 == 1,]  
inclusionsR15 <- inclusionsR[inclusionsR$INCLUDE_15 == 1,]  

inclusionsR = data.frame(inclusionsR15$ID,inclusionsR15$INCLUDE_15)
names(inclusionsR)[names(inclusionsR) == "inclusionsR15.ID"] <- "ID"
names(inclusionsR)[names(inclusionsR) == "inclusionsR15.INCLUDE_15"] <- "INCLUDE_15"

qs <- paste(readLines("Creel_iRec.txt"), collapse="\n")
qss = strsplit(qs,split=";;")
qs2 = unlist(strsplit(unlist(qss), ";;"))
nmeloc <- "/*-- QueryNameIs:"


for (i in 1:length(qs2)){
  qrynme <- substring(qs2[[i]],stringr::str_locate(qs2[[i]],nmeloc)[1])
  qrynme <- stringr::str_replace(qrynme, '\\*/','')
  qrynme <- trimws(stringr::str_replace(qrynme,nmeloc, "")) 
  qs2[[i]] <- gsub("XXXYEARXXX", c_lic.year.str,qs2[[i]])

  Q = dbSendQuery(con, qs2[[i]])
  message(paste0("Loading Estimates ", qrynme))
  et <- system.time(Sport <- dbFetch(Q))
  message("Done loading")
  message(et[3])

  Sport$ID<-paste(Sport$AREA,"-",Sport$MONTH,"-",Sport$YEAR)
  Sport<-merge(inclusionsR,Sport, by = "ID", all = TRUE)
}
returnData <- list()
returnData[[1]] <- inclusionsRaw
returnData[[2]] <- Sport
return (returnData)
}

