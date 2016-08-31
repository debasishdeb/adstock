library(data.table);library(bit64);library(reshape2)
library(doSNOW)
setwd("C:\\Users\\yuemeng1\\Desktop\\code\\adstock")
options(digits=4)
source("adstock_formula.R",local=F)

no.cluster=3
hl=c(0,3,6,10,30,50,70,90)
hrf=c(5,15,30,45,60,75)
# weeknum=105;
dim=c("dmanum","week","weeknum")
##################################################################


ad_para=expand.grid(hl,hrf)
setnames(ad_para,c("hl","hrf"))

index.only=fread("input_index_only.csv")
shell=fread("input_shell.csv")
adstock=fread("input_adstock.csv")
adstock[adstock==0]=NA
weeknum=max(shell$weeknum)


dma_list=unique(shell$dmanum)

#index only

index.only=index.only[,lapply(.SD,sum),by=c("dmanum","week")]
index.only=merge(shell,index.only,by=c("dmanum","week"),all.x=T)
index_orig_var=colnames(index.only)[!colnames(index.only) %in% c(dim)]
index_var=paste(index_orig_var,rep("i",length(index_orig_var)),sep="_")
index_mean_var=paste(index_orig_var,rep("m",length(index_orig_var)),sep="_")
index.only[index.only==0]=NA
index.only=to_num(index.only,dim)
for(i in 1:length(index_var)) {
  temp=paste(index_var[i],":=",index_orig_var[i],"/mean(",index_orig_var[i],",na.rm=T)")
  expr=parse(text=temp)
  index.only[,eval(expr),by=list(dmanum)]
  tempm=paste(index_mean_var[i],":=","mean(",index_orig_var[i],",na.rm=T)")
  exprm=parse(text=tempm)
  index.only[,eval(exprm),by=list(dmanum)]
  
}
# index.only[,c(index_orig_var):=NULL]
index.only$week=as.Date(index.only$week,"%m/%d/%Y")

#adstock and index
adstock=adstock[,lapply(.SD,sum),by=c("dmanum","week")]
adstock=merge(shell,adstock,by=c("dmanum","week"),all.x=T)
adstock=adstock[order(dmanum,weeknum)]
measure_orig_var=colnames(adstock)[!colnames(adstock) %in% c(dim)]
adstock=to_num(adstock,dim)

for(i in 1:length(measure_orig_var)) {
  temp=paste(measure_orig_var[i],":=",measure_orig_var[i],"/max(",measure_orig_var[i],",na.rm=T)")
  expr=parse(text=temp)
  adstock[,eval(expr),by=list(dmanum)]
}


adstock[is.na(adstock)]=0
adstock=adstock[order(weeknum,dmanum)]
adstock_dim=adstock[,colnames(adstock)[colnames(adstock) %in% c(dim)],with=F]

# for.diff.adstock=list()
# for.diff.week=list()
start.time=Sys.time()
cl=makeCluster(no.cluster,type="SOCK",outfile="")
registerDoSNOW(cl)


result=foreach(i=1:nrow(ad_para),.combine="cbind") %dopar% {
  library(data.table);
  adstock_process(i,adstock)
}
stopCluster(cl)

end=Sys.time()-start.time
print(paste("Note: Adstock time: ",round(end[[1]],digit=2),attr(end,"units"),sep="")) 



# result=round(result,digits=6)
adstock.final=cbind(adstock_dim,result)
adstock.final[adstock.final==0]=NA

index_media_var=colnames(adstock.final)[!colnames(adstock.final) %in% c(dim)]

for(i in 1:length(index_media_var)) {
  temp=paste(index_media_var[i],":=",index_media_var[i],"/mean(",index_media_var[i],",na.rm=T)")
  expr=parse(text=temp)
  adstock.final[,eval(expr),by=list(dmanum)]
}


index_media_var_i=paste(index_media_var,rep("i",length(index_media_var)),sep="_")
setnames(adstock.final,index_media_var,index_media_var_i)
adstock.final[is.na(adstock.final)]=0

# index.only$week=as.Date(index.only$week,"%m/%d/%Y")
adstock.final$week=as.Date(adstock.final$week,"%m/%d/%Y")

Final=merge(index.only,adstock.final,by=c(dim),all=T)
Final[is.na(Final)]=0

write.csv(Final,"output_final.csv",row.names=F)
