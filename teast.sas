libname ss "D:\Work\Daily_Stock_20131216";

proc sql;
create table code
as select unique stkcd
from ss.datawithmarket
where year(tradedate) = 1997;quit;


proc sql;
create table tt
as select a.*
from ss.datawithmarket as a, code as b
where a.stkcd = b.stkcd and year(tradedate) >1996;
quit;


data ss;set tt;if close = .;run;

proc sql;
create table code2
as select unique stkcd,count(stkcd) as n
from ss
group by stkcd
order by n;quit;
data code3;set code2;if n <=100;run;

proc sql;
create table tt2
as select a.*
from tt as a, code3 as b
where a.stkcd = b.stkcd;quit;


data tt2;set tt2;
if close = . then delete;
run;

data tt2;set tt2;
	if High = Open > Close = Low then Name = '2pa';
	*if High = Open = Close = Low  and stkret >= 0 then Name = '1pp';
	*if High = Open = Close = Low  and stkret < 0 then Name = '1pm';
	if High = Open = Close = Low then Name = '1p';
	if High = Close > Open = Low then Name = '2pb';
	if High = Open = Close > Low then Name = '2pc';
	if High > Open = Close = Low then Name = '2pd';
	if High = Open > Close > Low then Name = '3pa';
	if High > Open = Close > Low then Name = '3pb';
	if High > Open > Close = Low then Name = '3pc';
	if High > Close > Open = Low then Name = '3pd';
	if High = Close > Open > Low then Name = '3pe';
	if High > Open > Close > Low then Name = '4pa';
	if High > Close > Open > Low then Name = '4pb';
run;
/*========================================================*/
/*						TABLE 1*/
/*========================================================;*/



/*========================================================*/

proc expand data = tt2 out = tt2;
	by stkcd;
	id tradedate;
	convert stkret = d1ret / transformout=(LEAD 1); 
run;
proc expand data = tt2 out = tt2;
	by stkcd;
	id tradedate;
	convert d1ret = d10ret / transformout=(reverse +1 movprod 10 -1 reverse);
	convert d1ret = d5ret / transformout=(reverse +1 movprod 5 -1 reverse);
run;


/*Wednesday, March 02, 2016 08:31:43*/
*Open Return;

data tt2;set tt2;
	by stkcd;
	adjopen = open/close * adjclose;
	openret = (adjopen - lag(adjopen))/lag(adjopen);
	if first.stkcd then openret = 0;
run;


proc expand data = tt2 out = tt2;
	by stkcd;
	id tradedate;
	convert stkret = d1ret / transformout=(LEAD 1); 
run;
proc expand data = tt2 out = tt2;
	by stkcd;
	id tradedate;
	convert d1ret = d10ret / transformout=(reverse +1 movprod 10 -1 reverse);
	convert d1ret = d5ret / transformout=(reverse +1 movprod 5 -1 reverse);
run;

proc expand data = tt2 out = tt2;
	by stkcd;
	id tradedate;
	convert openret = d1reto / transformout=(LEAD 2); 
run;
proc expand data = tt2 out = tt2;
	by stkcd;
	id tradedate;
	convert d1reto = d10reto / transformout=(reverse +1 movprod 10 -1 reverse);
	convert d1reto = d5reto / transformout=(reverse +1 movprod 5 -1 reverse);
run;



data tt2;set tt2;
	if tradedate > '01jan2014'd then delete;
	*if d10ret = . then delete;
	*保证都有10天的持有期;
run;
data tt2;set tt2;
	if d1ret >= 0 then win1 = 1;else win1 = 0;
	if d10ret >= 0 then win10 = 1;else win10 = 0;
	if d5ret >= 0 then win5 = 1;else win5 = 0;
run; 

proc expand data = tt2 out = tt2;
	by stkcd;
	id tradedate;
	convert adjclose = d3avgprc / transformout=( movave 3 ); 
run;

data tt2;set tt2;
	if lag6(d3avgprc) < lag5(d3avgprc) < lag4(d3avgprc) < lag3(d3avgprc) < lag2(d3avgprc) < lag1(d3avgprc) then trend1 = 1;
	else if lag6(d3avgprc) > lag5(d3avgprc) > lag4(d3avgprc) > lag3(d3avgprc) > lag2(d3avgprc) > lag1(d3avgprc) then trend1 = -1;
	else trend1 = 0;
run;

proc expand data = tt2 out = tt2;
	by stkcd;
	id tradedate;
	convert adjclose = d5avgprc / transformout=(movave 5); 
	convert adjclose = d20avgprc / transformout=(movave 20); 
	convert adjclose = d1avgprc / transformout=(movave 1); 
	convert adjclose = d200avgprc / transformout=(movave 200); 
run;

data tt2;set tt2;
	if lag(d5avgprc) > lag(d20avgprc)*1.05 then trend2 = 1;
	else if lag(d5avgprc)*1.05 < lag(d20avgprc) then trend2 = -1;
	else trend2 = 0;
	if lag(d1avgprc) > lag(d200avgprc)*1.25 then trend3 = 1;
	else if lag(d1avgprc)*1.25 < lag(d200avgprc) then trend3 = -1;
	else trend3 = 0;
run;*use lag(d5) > lag(d20);
data tt2;set tt2;
	if stkret > 0 then trend0 = 1;
	if stkret < 0 then trend0 = -1;
	if stkret = 0 then trend0 = 0;
run;
/*Wednesday, March 02, 2016 08:42:20*/

* Table 1;

proc sort data = tt2 out = table1;by name tradedate;run;
proc means data = table1 noprint;
	by name;
	vars d1ret d5ret d10ret d1reto d5reto d10reto;
	output out = table1out mean(d1ret)= mean(d5ret)=mean(d10ret)=mean(d1reto)=mean(d5reto)=mean(d10reto)=
	t(d1ret)= t(d5ret)=t(d10ret)=t(d1reto)=t(d5reto)=t(d10reto)=/autoname;
run;


/*Wednesday, March 02, 2016 08:49:45*/

proc sort data = tt2  out = table2;by trend0 name tradedate;run;
proc means data = table2 noprint;
	by  trend0 name ;
	vars d1ret d5ret d10ret d1reto d5reto d10reto;
	output out = table2out0 mean(d1ret)= mean(d5ret)=mean(d10ret)=mean(d1reto)=mean(d5reto)=mean(d10reto)=
	t(d1ret)= t(d5ret)=t(d10ret)=t(d1reto)=t(d5reto)=t(d10reto)=/autoname;
run;

proc sort data = tt2  out = table2;by trend1 name tradedate;run;
proc means data = table2 noprint;
	by  trend1 name ;
	vars d1ret d5ret d10ret d1reto d5reto d10reto;
	output out = table2out1 mean(d1ret)= mean(d5ret)=mean(d10ret)=mean(d1reto)=mean(d5reto)=mean(d10reto)=
	t(d1ret)= t(d5ret)=t(d10ret)=t(d1reto)=t(d5reto)=t(d10reto)=/autoname;
run;

proc sort data = tt2  out = table2;by trend2 name tradedate;run;
proc means data = table2 noprint;
	by  trend2 name ;
	vars d1ret d5ret d10ret d1reto d5reto d10reto;
	output out = table2out2 mean(d1ret)= mean(d5ret)=mean(d10ret)=mean(d1reto)=mean(d5reto)=mean(d10reto)=
	t(d1ret)= t(d5ret)=t(d10ret)=t(d1reto)=t(d5reto)=t(d10reto)=/autoname;
run;


proc sort data = tt2  out = table2;by trend3 name tradedate;run;
proc means data = table2 noprint;
	by  trend3 name ;
	vars d1ret d5ret d10ret d1reto d5reto d10reto;
	output out = table2out3 mean(d1ret)= mean(d5ret)=mean(d10ret)=mean(d1reto)=mean(d5reto)=mean(d10reto)=
	t(d1ret)= t(d5ret)=t(d10ret)=t(d1reto)=t(d5reto)=t(d10reto)=/autoname;
run;

proc means data = tt2 noprint;
	vars d1ret d5ret d10ret d1reto d5reto d10reto;
	output out = table0out mean(d1ret)= mean(d5ret)=mean(d10ret)=mean(d1reto)=mean(d5reto)=mean(d10reto)=
	t(d1ret)= t(d5ret)=t(d10ret)=t(d1reto)=t(d5reto)=t(d10reto)=/autoname;
run;



proc means data = ttt2 noprint;
	by trend1 name;
	var sss;
	output out = ttt3 mean =sss2;
	run;


data sss;set tt2;if trend1 = -1 and name = '2pd';run;

proc univariate data=sss;
      var d10ret;
      histogram d10ret;
   run;
   proc sort data = sss ;by tradedate;run;
   proc means data = sss noprint;
	by tradedate;
	var d10ret;
	output out =sss2 mean = mean;
	run;
/**/
/*	proc sgplot data=ttt2;*/
/*	by trend1 name;*/
/*      series x=tradedate y=sss / markers;*/
/*   run;*/


data tt3;set tt2;
keep stkcd tradedate openper highper lowper adjclose stkret;
openper = round((close - open)/close*1000)/1000 ;
highper = round((high-close)/close*1000)/1000  ;
lowper = round((close-low)/close*1000)/1000;
run;

data ssss2;set sss;run;


data tt3;set tt3;
by stkcd;
logret = log(adjclose/lag(adjclose));
if first.stkcd then logret = 0;
run;


data ss.tt3;set tt3;run;
data tt3;set ss.tt3;run;
/*data ss.out;set _NULL_;run;*/


proc iml;
run ExportDataSetToR("work.tt3", "df" );
submit / R;
	library(rugarch);
	library(doParallel);
	library(foreach);
	library(plyr);
	library(dplyr);
	library(TTR);
	spec1 <- ugarchspec(variance.model = list(model = "sGARCH", garchOrder = c(1, 1),
											  submodel = NULL, external.regressors = NULL, variance.targeting = FALSE),
						mean.model = list(armaOrder = c(0, 1), include.mean = TRUE, archm = TRUE,
										  archpow = 2, arfima = FALSE, external.regressors = NULL, archex = FALSE),
						distribution.model = "norm", start.pars = list(), fixed.pars = list())
	fun1 <- function(data){
	  out1 <- ugarchfit(data$logret,spec=spec1,solver = "hybrid");
	  sim1 <- ugarchsim(out1,n.sim = length(data$logret),n.start=1, m.sim=1,
						startMethod="sample");
	  shigh <- as.data.frame(sample(data$highper,replace = TRUE));
	  slow <- as.data.frame(sample(data$lowper,replace = TRUE));
	  sopen <- sample(data$lowper,replace = TRUE);
	  fun5 <- function(n,m){
		y <- sample (data$openper[(-n[1]) <= data$openper & data$openper <= m[1]],1);
		return(y)
	  }

		resamp <- function(p,y,z){
		  repeat{
		    if(sum((-y) > p|p > z) == 0){
		      break
		      }else if(sum((-y) > p|p > z) < 30) {
		      p[(-y) > p|p > z] = mapply(fun5,y[(-y) > p|p > z],z[(-y) > p|p > z]);
		    }else{
		      p[(-y) > p|p > z] = sample(data$openper,size = sum((-y) > p|p > z),replace = TRUE)
		      }
		  }
		  return(p)
		}
		
	;
	sopen <- resamp(sopen,shigh,slow);

	result <-cbind(fitted(sim1),row(fitted(sim1)),length(fitted(sim1))-row(fitted(sim1)), as.data.frame(cumprod(exp(fitted(sim1)))),shigh,slow
			  ,as.data.frame(sopen));
	  colnames(result) <- c("sim","id","nid","sclose","shighper","slowper","sopenper");
	  return(result);
	};
endsubmit;

start getboot(workout);
submit workout /R;	
	ptm <- proc.time();
	cl <- makeCluster(3)
	registerDoParallel(cl)

	fit1 <- ddply(df,~stkcd,.parallel=TRUE,.fun = fun1,
				  .paropts=list(.export=c('spec1','fun1'),
								.packages='rugarch')
	);


	fit1$newname[fit1$shighper== 0 & fit1$slowper==0 & fit1$sopenper==0] = '1p';
	fit1$newname[(fit1$shighper== (-fit1$sopenper)) & (fit1$shighper > 0) & (fit1$slowper ==0)] = '2pa';
	fit1$newname[(fit1$shighper== 0) & (fit1$slowper==fit1$sopenper) & (fit1$sopenper > 0)] = '2pb';
	fit1$newname[(fit1$shighper==0) & (fit1$sopenper==0) & (fit1$slowper > 0)] = '2pc';
	fit1$newname[(fit1$shighper>0) & (fit1$slowper==0) & (fit1$sopenper==0)] = '2pd';
	fit1$newname[(fit1$shighper==(-fit1$sopenper)) & (fit1$shighper > 0) & (fit1$slowper > 0)] = '3pa';
	fit1$newname[fit1$shighper>0 & fit1$slowper>0 & fit1$sopenper==0] = '3pb';
	fit1$newname[fit1$shighper> (-fit1$sopenper) & fit1$slowper==0 & fit1$sopenper < 0] = '3pc';
	fit1$newname[fit1$shighper>0 & (fit1$slowper==fit1$sopenper) & fit1$sopenper > 0] = '3pd';
	fit1$newname[fit1$shighper==0 & (fit1$slowper>fit1$sopenper) & fit1$sopenper > 0] = '3pe';
	fit1$newname[(fit1$shighper > (- fit1$sopenper)) & fit1$sopenper < 0 & fit1$slowper > 0] = '4pa';
	fit1$newname[fit1$shighper > 0 & (fit1$slowper > fit1$sopenper) & fit1$sopenper > 0] = '4pb';

	x<- fit1$sclose;
	xx<- log(x[-(1:10)]/x[-((length(x)-9):length(x))]);
	fit1$Cret10 <- c(xx,rep(NA,10));
	fit1$Cret10lag1<-c(xx[-1],rep(NA,11));
	fit1$Cret1 <- c(fit1$sim[-1],NA);
	fit1$Cret1lag1 <- c(fit1$sim[-(1:2)],rep(NA,2));
	y<- fit1$sclose *(1 - fit1$sopenper);
	yy<- log(y[-(1:10)]/y[-((length(y)-9):length(y))]);
	fit1$Cret10open <- c(yy,rep(NA,10));
	fit1$Cret10lag1open<-c(yy[-1],rep(NA,11));
	fit1$Cret1open <- c(log(y[-1]/y[-length(y)]),NA);
	fit1$Cret1lag1open <- c(log(y[-1]/y[-length(y)])[-1],rep(NA,2));
	zz <- SMA(x,3);
	hh <- (zz > (c(NA,zz[-length(zz)]))) & ((c(NA,zz[-length(zz)])) > (c(NA,NA,zz[-((length(zz)-1):length(zz))]))) 	& ((c(rep(NA,2),zz[-((length(zz)-1):length(zz))])) >(c(rep(NA,3),zz[-((length(zz)-2):length(zz))]))) & ((c(rep(NA,3),zz[-((length(zz)-2):length(zz))])) >(c(rep(NA,4),zz[-((length(zz)-3):length(zz))]))) & ((c(rep(NA,4),zz[-((length(zz)-3):length(zz))])) >(c(rep(NA,5),zz[-((length(zz)-4):length(zz))]))) & ((c(rep(NA,5),zz[-((length(zz)-4):length(zz))])) >(c(rep(NA,6),zz[-((length(zz)-5):length(zz))]))) ;
	gg <- (zz < (c(NA,zz[-length(zz)]))) & ((c(NA,zz[-length(zz)])) < (c(NA,NA,zz[-((length(zz)-1):length(zz))]))) 	& ((c(rep(NA,2),zz[-((length(zz)-1):length(zz))])) <(c(rep(NA,3),zz[-((length(zz)-2):length(zz))]))) & ((c(rep(NA,3),zz[-((length(zz)-2):length(zz))])) <(c(rep(NA,4),zz[-((length(zz)-3):length(zz))]))) & ((c(rep(NA,4),zz[-((length(zz)-3):length(zz))])) <(c(rep(NA,5),zz[-((length(zz)-4):length(zz))]))) & ((c(rep(NA,5),zz[-((length(zz)-4):length(zz))])) <(c(rep(NA,6),zz[-((length(zz)-5):length(zz))]))) ;
	fit1$trend = 0;
	fit1$trend[hh] = 1;
	fit1$trend[gg] = -1;

	fit2 <- fit1[(fit1$id > 11) & (fit1$nid > 11),-(2:8)]; 
	out1 <- ddply(fit2,.(trend,newname),numcolwise(mean),.parallel=TRUE);
	out2 <- ddply(fit2,.(trend,newname),nrow,.parallel = TRUE);
	out1$simnum = &workout;
	out1$numbs = out2$V1;
	str(out1);
	stopCluster(cl);
	proc.time()-ptm;
endsubmit;

Call ImportDataSetFromR ("work.sss","out1");
submit;
data ss.out;set ss.out sss;run;
endsubmit;

finish;

/*call getboot(1);*/

*undo;
do i = 137 to 300;
call getboot(i);
end;


quit;


