to_num = function(dt,dimension) {
  dim_part=dt[,c(dimension),with=F]
  num_part=dt[,colnames(dt)[!colnames(dt) %in% c(dimension)],with=F]
  num_part=as.data.table(sapply(num_part,as.numeric))
  final=cbind(dim_part,num_part)
  return(final)
}


adstock_process = function(ind,dt) {
  lambda=exp(log(0.5)/(ad_para[ind,]$hl/10))
  beta=-10*log(1-ad_para[ind,]$hrf/100)
  for.diff.adstock=dt
  for(j in 1:weeknum) {
    if (j==1) {
      for.diff.adstock[weeknum==j,measure_orig_var]=1-exp(-beta*subset(for.diff.adstock,weeknum==j,select = c(measure_orig_var)))
    } else {
      # temp.mat=(1-exp(-beta*temp.mat))+lambda*for.diff.week[[j-1]][,measure_orig_var,with=F]]
      for.diff.adstock[weeknum==j,measure_orig_var]=1-exp(-beta*subset(for.diff.adstock,weeknum==j,select = c(measure_orig_var)))+
        lambda*subset(for.diff.adstock,weeknum==(j-1),select = c(measure_orig_var))
    }
    # for.diff.week[[j]]=cbind(temp.dim,temp.mat)
  }
  setnames(for.diff.adstock,c(dim,paste(measure_orig_var,
                                        paste(formatC(ad_para[i,]$hl,width=2,flag="0"),formatC(ad_para[i,]$hrf,width=2,flag="0"),sep=""),sep="_")))
  for.diff.adstock[order(weeknum,dmanum)]
  for.diff.adstock[,c(dim):=NULL]
  # temp_col_name=colnames(for.diff.adstock)[!colnames(for.diff.adstock) %in% c(dim)]
  return(data.table(subset(for.diff.adstock)))
  # return(data.table(for.diff.adstock))
}