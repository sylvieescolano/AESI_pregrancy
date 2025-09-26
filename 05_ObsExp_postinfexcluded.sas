/*Analyse de sensibilité : Calcul des ratios Observed/expected
pour l'analyse principale (tous types de diagnostics, delai 42j)
en retirant les périodes 90j post COVID du suivi "covout"*/

/*Supprimer tables de WORK*/
proc datasets lib=work kill;
run;quit;   

/*Definir la librairie ObservesAttendusAESI*/
libname rep '...';
/*Definir la librairie base grossesse*/
libname baseg '...';


/*Creer table init pour transposition finale*/
DATA work.init;
length phase $8.;
input phase;
datalines;
T0
  T1
  T2
  T3
  T1-T3
RUN;


/*Utilisation de la macro précédente de calcul des ratios Observed/Expected 
(stratifié selon age +/- "trimestre") avec intervalles de confiance à95%
pour un AESI défini (diag) 
recherché pour tous types de diagnostic (DP_only=0) ou principal seulement (DP_only=1)
pour les vaccinations de la table "allvac_42_COVOUT" 
(excluant les périodes post infection COVID du follow up)
selon type de schema vaccinal (regimen)*/



%macro OBSEXP(regimen=,diag=,DP_only=);
/*On filtre la table des vaccins avec durees FU recalculées(allvac_42_covout) selon regimen
(parmi vaccins durant PP (LMP-70-EOPE) et <23/06/23-42) */
PROC SQL;
   CREATE TABLE WORK.tmp1 AS 
   SELECT t1.indsej, 
          t1.trimester, 
          t1.dbt_FUvac, 
          t1.fin_FUvac
      FROM rep.allvac_42_covout t1
      WHERE t1.regimen = "&regimen.";
QUIT;


/*Table des grossesses: grossesses présentes dans tmp1 (vaccinées)*/

PROC SQL;
   CREATE TABLE WORK.tmp2 AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
   t1.age_ann,
   			T1.INDSEJ,
			t1.dureeG,
          t1.lmp_calc, 
          t1.dat_evt
      FROM baseg.baseG t1 
	  RIGHT JOIN work.tmp1 t2
	  ON t1.indsej=t2.indsej
;
QUIT;

/* RECHERCHE DES DIAG  à partir de la table des CIM
(La table des CIM comprend tous les CIM pour les patientes de BASEG <=31dec2024)
(tables MCO 13 à 24)*/


PROC SQL;
   CREATE TABLE WORK.DIAG AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          t1.date_diag
      FROM baseg.cim_mat t1
	  inner join  rep.cim_aesi t2
	  ON T2.CIM10=t1.CIM
      WHERE T2.DIAG = "&diag." 
%if &DP_only. %then %do;
AND 
(t1.diag_type='DGN_PAL' OR t1.diag_type='DGN_PAL_UM')
%end; ;
QUIT;

/*Grossesses dont le BNA est associé a >1 diagnostic + Ajout des variables:
- "during" (1 si diagnostic au cours de T0-T3 : LMP-28 / Evt -1)
-"clean_window" (1 si diagnostic dans les 365j précédant T0 pour les AESI)*/

PROC SQL;
   CREATE TABLE WORK.tmp3 AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
  	 T1.INDSEJ,
          t1.lmp_calc, 
          t1.dat_evt, 
          t2.date_diag,

	/* during */
	((CASE
		WHEN t1.lmp_calc -28 <= t2.date_diag <t1.dat_evt THEN 1 ELSE 0 END )
	) AS during,

	/* clean_window */
	((CASE
		WHEN t1.lmp_calc -28 -365<= t2.date_diag < t1.lmp_calc -28 THEN 1 ELSE 0 END )
	) AS clean_window 

      FROM WORK.TMP2 t1
           left JOIN WORK.DIAG t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO);
QUIT;



/*Catégorie pour chaque grossesse associée au diag via le BNA:
-"hors-suivi" si >1 diag dans la clean window
-"non-cas" si diag hors clean window et "PP"
-"cas" si aucun diag dans clean window et >1 durant "PP"*/

PROC SQL;
   CREATE TABLE WORK.TMP4 AS 
   SELECT DISTINCT  T1.INDSEJ,
   
            (CASE WHEN (MAX(t1.clean_window))=1 THEN 'hors-suivi'
				  WHEN (MAX(t1.clean_window))=0 then
							(CASE WHEN(MAX(t1.during))=1 THEN 'cas' 
									ELSE 'non-cas' END)END) AS CAT

      FROM WORK.TMP3 t1
      GROUP BY   t1.indsej;
QUIT;

title &diag.;
PROC FREQ data=work.tmp4; tables cat;run;


/*Garder premiere date de diag durant T0-T3 pour les "cas"*/
PROC SQL;
   CREATE TABLE WORK.cas AS 
   SELECT DISTINCT t1.indsej, 
     	t3.cat,
          /* MIN_of_date_diag */
            (MIN(t1.date_diag)) FORMAT=DDMMYY10. AS date_diag
      FROM WORK.TMP3 t1
           INNER JOIN WORK.TMP4 t3 ON (t1.indsej = t3.indsej)
      WHERE t3.CAT = 'cas' AND t1.during = 1
      GROUP BY t1.BEN_NIR_ANO,
               t1.indsej,
               t1.lmp_calc,
               t1.dat_evt;
QUIT;

/*retirer grossesses "hors suivi" et garder date_diag pour les grossesses "cas"*/
PROC SQL;
   CREATE TABLE WORK.tmp5 AS 
   SELECT t1.indsej, 
          t2.date_diag
      FROM WORK.TMP4 t1
           LEFT JOIN WORK.CAS t2 ON (t1.indsej = t2.indsej)
      WHERE t1.CAT NOT = 'hors-suivi';
QUIT;




/* Pour les grossesses de tmp5:
Ajout duree de follow up (FU) par trimestre sans prise en compte de la date de l'AESI*/
PROC SQL;
   CREATE TABLE WORK.tmp6 AS 
   SELECT t1.indsej,
t3.date_diag,
   t1.age_ann,
          t1.dureeG, 
          t1.lmp_calc, 
          t1.dat_evt, 

		  t1.lmp_calc-28 FORMAT=ddmmyy10. as dbt_FUT0,
			t1.lmp_calc+14 FORMAT=ddmmyy10. as fin_FUT0,

			t1.lmp_calc+14 FORMAT=ddmmyy10. as dbt_FUT1,  
			min(t1.lmp_calc+98,t1.dat_evt) FORMAT=ddmmyy10. as fin_FUT1,

			CASE WHEN t1.dat_evt>t1.lmp_calc+98
			THEN t1.lmp_calc+98 ELSE . END FORMAT=ddmmyy10. as dbt_FUT2,
			CASE WHEN t1.dat_evt>t1.lmp_calc+98
			THEN min(t1.lmp_calc+196,t1.dat_evt) ELSE . END FORMAT=ddmmyy10. as fin_FUT2,

			CASE WHEN t1.dat_evt>t1.lmp_calc+196
			THEN t1.lmp_calc+196 ELSE . END FORMAT=ddmmyy10. as dbt_FUT3,
			CASE WHEN t1.dat_evt>t1.lmp_calc+196
			THEN t1.dat_evt-1 ELSE . END FORMAT=ddmmyy10. as fin_FUT3

      FROM baseg.baseg t1 
INNER JOIN work.tmp5 t3 on t1.indsej=t3.indsej;
QUIT;


/*Ajout phase de grossesse au moment du diag 
+ recalculer les periodes de suivi pour chaque phase en arretant au diag*/

PROC SQL;
   CREATE TABLE WORK.tmp7 AS 
   SELECT t1.AGE_ANN, 
          t1.dat_evt, 
          t1.date_diag, 
          /* phase */
            (CASE WHEN t1.date_diag is not missing THEN
            (CASE WHEN t1.date_diag BETWEEN t1.dbt_FUT0 AND t1.fin_FUT0 THEN 'T0'
            WHEN t1.date_diag BETWEEN t1.dbt_FUT1 AND t1.fin_FUT1 THEN 'T1'
            WHEN t1.date_diag BETWEEN t1.dbt_FUT2 AND t1.fin_FUT2 THEN 'T2'
            WHEN t1.date_diag BETWEEN t1.dbt_FUT3 AND t1.fin_FUT3 THEN 'T3'
            END)        
            END
            ) AS phase, 

 t1.dbt_FUT0,
(CASE WHEN t1.date_diag is not missing 
		THEN MIN(t1.date_diag,t1.FIN_FUT0) ELSE t1.FIN_FUT0 END) FORMAT=ddmmyy10. as  fin_FUT0,
	

(CASE WHEN t1.date_diag is not missing and t1.date_diag<=dbt_FUT1
THEN . ELSE t1.dbt_FUT1 END) FORMAT=ddmmyy10. as  dbt_FUT1,
 

(CASE WHEN t1.date_diag is not missing and t1.date_diag<=dbt_FUT1 THEN .
WHEN t1.date_diag is not missing and t1.fin_FUT1 is not missing and t1.date_diag>dbt_FUT1
THEN MIN(t1.date_diag,t1.FIN_FUT1) ELSE t1.fin_FUT1 END) FORMAT=ddmmyy10. as  fin_FUT1,


(CASE WHEN t1.date_diag is not missing and t1.date_diag<=dbt_FUT2
THEN . ELSE t1.dbt_FUT2 END) FORMAT=ddmmyy10. as  dbt_FUT2,


(CASE WHEN t1.date_diag is not missing and t1.date_diag<=dbt_FUT2 THEN .
WHEN t1.date_diag is not missing and t1.fin_FUT2 is not missing and t1.date_diag>dbt_FUT2
THEN MIN(t1.date_diag,t1.FIN_FUT2) ELSE t1.fin_FUT2 END) FORMAT=ddmmyy10. as  fin_FUT2,


(CASE WHEN t1.date_diag is not missing and t1.date_diag<=dbt_FUT3
THEN . ELSE t1.dbt_FUT3 END) FORMAT=ddmmyy10. as  dbt_FUT3,


(CASE WHEN t1.date_diag is not missing and t1.date_diag<=dbt_FUT3 THEN .
WHEN t1.date_diag is not missing and t1.fin_FUT3 is not missing and t1.date_diag>dbt_FUT3
THEN MIN(t1.date_diag,t1.FIN_FUT3) ELSE t1.fin_FUT3 END) FORMAT=ddmmyy10. as  fin_FUT3,
 
          t1.dureeG,  
          t1.indsej, 
          t1.lmp_calc
      FROM WORK.TMP6 t1;
QUIT;



/*Ajout de TMP1: vaccins et periodes de FU post vaccinal (calculées dans script INF_COVID)
Calcul intersection entre duree de FU par trimestre et FU post vaccinal
(on ne prend pas en compte la veille de l'EOPE)*/

PROC SQL;
   CREATE TABLE WORK.TMP8 AS 
   SELECT t1.indsej, 
          t2.dbt_FUvac, 
          t2.fin_FUvac, 
		  t2.trimester,
		  t1.date_diag, 
t1.phase,

          /* dbt_FUvacT0 */
            (CASE WHEN t2.dbt_FUvac<t1.fin_FUT0  AND t2.fin_FUvac>=t1.dbt_FUT0
            THEN
            max(t1.dbt_FUT0,t2.dbt_FUvac)
             ELSE . END) FORMAT=DDMMYY10. AS dbt_FUvacT0, 
          /* fin_FUvacT0 */
            (CASE WHEN t2.dbt_FUvac<t1.fin_FUT0  AND t2.fin_FUvac>=t1.dbt_FUT0
            THEN
            min(t1.fin_FUT0,t2.fin_FUvac)
             ELSE . END) FORMAT=DDMMYY10. AS fin_FUvacT0, 

          /* Calcul FU T0 */
            ((CASE WHEN t2.dbt_FUvac<t1.fin_FUT0  AND t2.fin_FUvac>=t1.dbt_FUT0
            THEN
            min(t1.fin_FUT0,t2.fin_FUvac)
             ELSE . END) - (CASE WHEN t2.dbt_FUvac<t1.fin_FUT0  AND t2.fin_FUvac>=t1.dbt_FUT0
            THEN
            max(t1.dbt_FUT0,t2.dbt_FUvac)
             ELSE . END)) AS FUT0, 

          /* dbt_FUvacT1 */
            ((CASE WHEN t2.dbt_FUvac<t1.fin_FUT1  AND t2.fin_FUvac>=t1.dbt_FUT1
                        THEN
                        max(t1.dbt_FUT1,t2.dbt_FUvac)
                         ELSE . END) ) FORMAT=DDMMYY10. AS dbt_FUvacT1, 
          /* fin_FUvacT1 */
            ((CASE WHEN t2.dbt_FUvac<t1.fin_FUT1  AND t2.fin_FUvac>=t1.dbt_FUT1
                        THEN
                        min(t1.fin_FUT1,t2.fin_FUvac,t1.dat_evt-1)
                         ELSE . END)  ) FORMAT=DDMMYY10. AS fin_FUvacT1, 
          /* Calcul PYRS T1 */
            (((CASE WHEN t2.dbt_FUvac<t1.fin_FUT1  AND t2.fin_FUvac>=t1.dbt_FUT1
                        THEN
                        min(t1.fin_FUT1,t2.fin_FUvac,t1.dat_evt-1)
                         ELSE . END)  ) - ((CASE WHEN t2.dbt_FUvac<t1.fin_FUT1  AND 
            t2.fin_FUvac>=t1.dbt_FUT1
                        THEN
                        max(t1.dbt_FUT1,t2.dbt_FUvac)
                         ELSE . END) )) AS FUT1,

			/* dbt_FUvacT2 */
            ((CASE WHEN t2.dbt_FUvac<t1.fin_FUT2  AND t2.fin_FUvac>=t1.dbt_FUT2
                        THEN
                        max(t1.dbt_FUT2,t2.dbt_FUvac)
                         ELSE . END) ) FORMAT=DDMMYY10. AS dbt_FUvacT2, 
          /* fin_FUvacT2 */
            ((CASE WHEN t2.dbt_FUvac<t1.fin_FUT2  AND t2.fin_FUvac>=t1.dbt_FUT2
                        THEN
                        min(t1.fin_FUT2,t2.fin_FUvac,t1.dat_evt-1)
                         ELSE . END)  ) FORMAT=DDMMYY10. AS fin_FUvacT2, 
          /* Calcul FU T2 */
            (((CASE WHEN t2.dbt_FUvac<t1.fin_FUT2  AND t2.fin_FUvac>=t1.dbt_FUT2
                        THEN
                        min(t1.fin_FUT2,t2.fin_FUvac,t1.dat_evt-1)
                         ELSE . END)  ) - ((CASE WHEN t2.dbt_FUvac<t1.fin_FUT2  AND 
            t2.fin_FUvac>=t1.dbt_FUT2
                        THEN
                        max(t1.dbt_FUT2,t2.dbt_FUvac)
                         ELSE . END) )) AS FUT2,

						 /* dbt_FUvacT3 */
            ((CASE WHEN t2.dbt_FUvac<t1.fin_FUT3  AND t2.fin_FUvac>=t1.dbt_FUT3
                        THEN
                        max(t1.dbt_FUT3,t2.dbt_FUvac)
                         ELSE . END) ) FORMAT=DDMMYY10. AS dbt_FUvacT3, 
          /* fin_FUvacT3 */
            ((CASE WHEN t2.dbt_FUvac<t1.fin_FUT3  AND t2.fin_FUvac>=t1.dbt_FUT3
                        THEN
                        min(t1.fin_FUT3,t2.fin_FUvac,t1.dat_evt-1)
                         ELSE . END)  ) FORMAT=DDMMYY10. AS fin_FUvacT3, 
          /* Calcul FUT3 */
            (((CASE WHEN t2.dbt_FUvac<t1.fin_FUT3  AND t2.fin_FUvac>=t1.dbt_FUT3
                        THEN
                        min(t1.fin_FUT3,t2.fin_FUvac,t1.dat_evt-1)
                         ELSE . END)  ) - ((CASE WHEN t2.dbt_FUvac<t1.fin_FUT3  AND 
            t2.fin_FUvac>=t1.dbt_FUT3
                        THEN
                        max(t1.dbt_FUT3,t2.dbt_FUvac)
                         ELSE . END) )) AS FUT3


      FROM WORK.TMP7 t1
INNER JOIN WORK.TMP1 t2
ON t1.indsej=t2.indsej;
QUIT;

/*Changer . en 0*/
data work.tmp9;
   set work.tmp8;
   array variablesOfInterest FUT0 FUT1 FUT2 FUT3;
   do over variablesOfInterest;
      if variablesOfInterest=. then variablesOfInterest=0;
   end;
run;

/*AJOUT AGE_ANN*/
PROC SQL;
   CREATE TABLE WORK.tmp10 AS 
   SELECT DISTINCT t1.*,
          t2.AGE_ANN
      FROM WORK.TMP9 t1
           LEFT JOIN WORK.TMP2 t2 ON (t1.indsej = t2.indsej);
QUIT;

/*AJOUT CLASSES D'AGE*/

proc format;
value agecl

12-19='[12-19]'
20-29='[20-29]'
30-39='[30-39]'
40-59='[40-59]'
;
run;
data tmp11; set WORK.tmp10;
age=put(age_ann,agecl.);
run;


/*Total par vaccin*/
PROC SQL;
   CREATE TABLE WORK.vac_fu AS 
   SELECT DISTINCT t1.indsej, 
          t1.dbt_FUvac, 
          t1.trimester, 
          t1.date_diag, 
          t1.FUT0, 
          t1.FUT1, 
          t1.FUT2, 
          t1.FUT3, 
          /* Calcul */
            (t1.FUT0+t1.FUT1+t1.FUT2+t1.FUT3) AS total
      FROM WORK.TMP11 t1
ORDER BY t1.indsej, t1.dbt_FUvac;
QUIT;

title "durée de FU post vac par vaccin";
PROC MEANS DATA=work.vac_fu
		MEAN 		STD 		MIN 		MAX 		N			Q1 		MEDIAN 		Q3	;
	VAR total;

RUN;

/*Total par grossesse*/
PROC SQL;
   CREATE TABLE WORK.vac_fu_bypreg AS 
   SELECT t1.indsej, 
          /* SUM_of_total */
            (SUM(t1.total)) AS SUM_of_total
      FROM WORK.VAC_FU t1
      GROUP BY t1.indsej;
QUIT;
title "durée de FU post vac par grossesse";
PROC MEANS DATA=work.vac_fu_bypreg
		MEAN 		STD 		MIN 		MAX 		N			Q1 		MEDIAN 		Q3;
	VAR SUM_of_total;RUN;


	/*OBSERVED*/
	/*On récupère la phase de l'evt seulement si la date de diag se situe 
	dans les bornes des périodes de FU post vac*/

	PROC SQL;
   CREATE TABLE WORK.OBS AS 
   SELECT t1.phase,
   t1.age
      FROM WORK.TMP11 t1
      WHERE t1.date_diag NE .
           AND
                       (t1.date_diag BETWEEN t1.dbt_FUvacT0 AND t1.fin_FUvacT0 OR
                       t1.date_diag BETWEEN t1.dbt_FUvacT1 AND t1.fin_FUvacT1 OR
                       t1.date_diag BETWEEN t1.dbt_FUvacT2 AND t1.fin_FUvacT2 OR
                       t1.date_diag BETWEEN t1.dbt_FUvacT3 AND t1.fin_FUvacT3 );
QUIT;


PROC SQL;
   CREATE TABLE WORK.OBS2 AS 
   SELECT t1.age, 
          /* OBS_T0 */
            (SUM(CASE WHEN t1.phase='T0' THEN 1 ELSE 0 END)) AS OBS_T0,
			(SUM(CASE WHEN t1.phase='T1' THEN 1 ELSE 0 END)) AS OBS_T1,
			(SUM(CASE WHEN t1.phase='T2' THEN 1 ELSE 0 END)) AS OBS_T2,
			(SUM(CASE WHEN t1.phase='T3' THEN 1 ELSE 0 END)) AS OBS_T3,
			(SUM(CASE WHEN t1.phase NE'T0' THEN 1 ELSE 0 END)) AS OBS_T1T3
      FROM WORK.OBS t1
      GROUP BY t1.age

UNION

SELECT "all" as age length=8,
          /* OBS_T0 */
            (SUM(CASE WHEN t1.phase='T0' THEN 1 ELSE 0 END)) AS OBS_T0,
			(SUM(CASE WHEN t1.phase='T1' THEN 1 ELSE 0 END)) AS OBS_T1,
			(SUM(CASE WHEN t1.phase='T2' THEN 1 ELSE 0 END)) AS OBS_T2,
			(SUM(CASE WHEN t1.phase='T3' THEN 1 ELSE 0 END)) AS OBS_T3,
			(SUM(CASE WHEN t1.phase NE'T0' THEN 1 ELSE 0 END)) AS OBS_T1T3
      FROM WORK.OBS t1;
QUIT;










/*Table pour calcul EXPECTED*/

/*On somme les durees de suivi par phase pour chaque grossesse incluse
+ajout age_ann*/

PROC SQL;
   CREATE TABLE WORK.tmp12 AS 
   SELECT DISTINCT t1.indsej, 
          t1.AGE, 
          /* FUT0 */
            (SUM(t1.FUT0)) AS FUT0, 
          /* FUT1 */
            (SUM(t1.FUT1)) AS FUT1, 
          /* FUT2 */
            (SUM(t1.FUT2)) AS FUT2, 
          /* FUT3 */
            (SUM(t1.FUT3)) AS FUT3
      FROM WORK.TMP11 t1
     
      GROUP BY t1.indsej, t1.age;
QUIT;



/*CALCUL PYRS POST VAC DANS CHAQUE PHASE ET CLASSE D'AGE*/
PROC SQL;
   CREATE TABLE WORK.tmp13 AS 

 SELECT  t1.age, 
          /* PYRS_T0 */
            (SUM(t1.FUT0)/365.25) AS PYRS_T0, 
          /* PYRS_T1 */
            (SUM(t1.FUT1)/365.25) AS PYRS_T1, 
          /* PYRS_T2 */
            (SUM(t1.FUT2)/365.25) AS PYRS_T2, 
          
			/* PYRS_T3 */
            (SUM(t1.FUT3)/365.25) AS PYRS_T3, 
/* PYRS_T1T3 */
            ((SUM(t1.FUT1)+SUM(t1.FUT2)+SUM(t1.FUT3))/365.25) AS PYRS_T1T3
      FROM WORK.TMP12 t1
      GROUP BY  t1.age

	  UNION

	    SELECT "all" as age length=8,
   /* PYRS_T0 */
            (SUM(t1.FUT0)/365.25) AS PYRS_T0, 
          /* PYRS_T1 */
            (SUM(t1.FUT1)/365.25) AS PYRS_T1, 
          /* PYRS_T2 */
            (SUM(t1.FUT2)/365.25) AS PYRS_T2, 
          /* PYRS_T3 */
            (SUM(t1.FUT3)/365.25) AS PYRS_T3, 
			/* PYRS_T1T3 */
            ((SUM(t1.FUT1)+SUM(t1.FUT2)+SUM(t1.FUT3))/365.25) AS PYRS_T1T3
      FROM WORK.TMP11 t1
;
QUIT;

/*Ajout TI des BGIR et calcul des attendus (EXP) + AJOUT OBSERVES*/
PROC SQL;
   CREATE TABLE WORK.tmp14 AS 
   SELECT t2.outcome, 
          t1.age, 
          t1.PYRS_T0, 
          t1.PYRS_T1, 
          t1.PYRS_T2, 
          t1.PYRS_T3, 
          t1.PYRS_T1T3, 
          t2.TI_T0, 
          t2.TI_T1, 
          t2.TI_T2, 
          t2.TI_T3, 
          t2.TI_T1T3,
		  (t1.PYRS_T0*t2.TI_T0/100000) as EXP_T0,
		  (t1.PYRS_T1*t2.TI_T1/100000) as EXP_T1,
		  (t1.PYRS_T2*t2.TI_T2/100000) as EXP_T2,
		  (t1.PYRS_T3*t2.TI_T3/100000) as EXP_T3,
		  (t1.PYRS_T1T3*t2.TI_T1T3/100000) as EXP_T1T3,
		  t3.OBS_T0, 
          t3.OBS_T1, 
          t3.OBS_T2, 
          t3.OBS_T3, 
          t3.OBS_T1T3

      FROM WORK.TMP13 t1

	  %if &DP_only. %then %do;
	  INNER JOIN REP.BGR_DP t2 ON (t1.age = t2.age)%end;
	  %else %do;
	  INNER JOIN REP.BGR_ALLDIAG t2 ON (t1.age = t2.age)%end; 

      LEFT JOIN WORK.OBS2 t3 ON (t1.age = t3.age)
      WHERE t2.outcome="&diag.";
QUIT;

data WORK.TMP14;
   set WORK.TMP14;
   array variablesOfInterest _numeric_;
   do over variablesOfInterest;
      if variablesOfInterest=. then variablesOfInterest=0;
   end;
run;

PROC SQL;
   CREATE TABLE WORK.all_&diag. AS 

SELECT * FROM WORK.TMP14

UNION

/*CALCUL avec stratification : on somme tous les attendus calculés
par classe d'age (on retire "all")*/
   SELECT t1.outcome, 
          "strat" as age,
            . AS PYRS_T0, 
         
            . AS PYRS_T1, 
          
            . AS PYRS_T2, 
          
            . AS PYRS_T3, 
          
            . AS PYRS_T1T3, 

			. AS TI_T0, 
          . AS TI_T1, 
          . AS TI_T2, 
          . AS TI_T3, 
          . AS TI_T1T3,
          
            (SUM(t1.EXP_T0)) AS EXP_T0, 
          
            (SUM(t1.EXP_T1)) AS EXP_T1, 
          
            (SUM(t1.EXP_T2)) AS EXP_T2, 
          
            (SUM(t1.EXP_T3)) AS EXP_T3, 
          
			/*calcul de EXP_T1T3 apres double stratification
			(somme des EXP de chaque phase obtenu apres stratification sur l'age)*/
            (SUM(SUM(t1.EXP_T1),SUM(t1.EXP_T2),SUM(t1.EXP_T3))) AS EXP_T1T3, 
         

            (SUM(t1.OBS_T0)) AS OBS_T0, 
          
            (SUM(t1.OBS_T1)) AS OBS_T1, 
         
            (SUM(t1.OBS_T2)) AS OBS_T2, 
          
            (SUM(t1.OBS_T3)) AS OBS_T3, 
          
            (SUM(t1.OBS_T1T3)) AS OBS_T1T3
      FROM WORK.TMP14 t1
      WHERE t1.age NOT = 'all'
      GROUP BY t1.outcome;
QUIT;


/*Ajout IC 95% bilateral et unilateral pour nombre observés (méthode exacte,Poisson)*/
PROC SQL;
   CREATE TABLE WORK.tmp16 AS 
   SELECT t1.outcome, 
          t1.OBS_T0, 
quantile('CHISQ',.025,2*OBS_T0)/2 AS inf_OBS_T0,
quantile('CHISQ',.975,2*(OBS_T0+1))/2 AS sup_OBS_T0,
quantile('CHISQ',.05,2*OBS_T0)/2 AS infu_OBS_T0,
          t1.OBS_T1, 
quantile('CHISQ',.025,2*OBS_T1)/2 AS inf_OBS_T1,
quantile('CHISQ',.975,2*(OBS_T1+1))/2 AS sup_OBS_T1,
quantile('CHISQ',.05,2*OBS_T1)/2 AS infu_OBS_T1,
          t1.OBS_T2, 
quantile('CHISQ',.025,2*OBS_T2)/2 AS inf_OBS_T2,
quantile('CHISQ',.975,2*(OBS_T2+1))/2 AS sup_OBS_T2,
quantile('CHISQ',.05,2*OBS_T2)/2 AS infu_OBS_T2,
          t1.OBS_T3, 
quantile('CHISQ',.025,2*OBS_T3)/2 AS inf_OBS_T3,
quantile('CHISQ',.975,2*(OBS_T3+1))/2 AS sup_OBS_T3,
quantile('CHISQ',.05,2*OBS_T3)/2 AS infu_OBS_T3,
          t1.OBS_T1T3, 
quantile('CHISQ',.025,2*OBS_T1T3)/2 AS inf_OBS_T1T3,
quantile('CHISQ',.975,2*(OBS_T1T3+1))/2 AS sup_OBS_T1T3,
quantile('CHISQ',.05,2*OBS_T1T3)/2 AS infu_OBS_T1T3,
          t1.EXP_T0, 
          t1.EXP_T1, 
          t1.EXP_T2, 
          t1.EXP_T3, 
          t1.EXP_T1T3
      FROM WORK.all_&diag. t1
      WHERE t1.age = 'strat';
QUIT;

/*Transposer table*/
PROC SQL;
   CREATE TABLE WORK.tmp17 AS 
   SELECT t2.outcome,
			t1.phase, 
          /* TI_periode1 */
            (CASE
			WHEN t1.phase= 'T0' THEN t2.OBS_T0
             WHEN t1.phase= 'T1' THEN t2.OBS_T1
            WHEN t1.phase= 'T2' THEN t2.OBS_T2
            WHEN t1.phase= 'T3' THEN t2.OBS_T3
        WHEN t1.phase= 'T1-T3' THEN t2.OBS_T1T3
            end) AS OBS,
			(CASE
			WHEN t1.phase= 'T0' THEN t2.inf_OBS_T0
             WHEN t1.phase= 'T1' THEN t2.inf_OBS_T1
            WHEN t1.phase= 'T2' THEN t2.inf_OBS_T2
            WHEN t1.phase= 'T3' THEN t2.inf_OBS_T3
        WHEN t1.phase= 'T1-T3' THEN t2.inf_OBS_T1T3
            end) AS inf_OBS,
			(CASE
			WHEN t1.phase= 'T0' THEN t2.sup_OBS_T0
             WHEN t1.phase= 'T1' THEN t2.sup_OBS_T1
            WHEN t1.phase= 'T2' THEN t2.sup_OBS_T2
            WHEN t1.phase= 'T3' THEN t2.sup_OBS_T3
        WHEN t1.phase= 'T1-T3' THEN t2.sup_OBS_T1T3
            end) AS sup_OBS,
			(CASE
			WHEN t1.phase= 'T0' THEN t2.infU_OBS_T0
             WHEN t1.phase= 'T1' THEN t2.infU_OBS_T1
            WHEN t1.phase= 'T2' THEN t2.infU_OBS_T2
            WHEN t1.phase= 'T3' THEN t2.infU_OBS_T3
        WHEN t1.phase= 'T1-T3' THEN t2.infU_OBS_T1T3
            end) AS infU_OBS,
			(CASE
			WHEN t1.phase= 'T0' THEN t2.EXP_T0
             WHEN t1.phase= 'T1' THEN t2.EXP_T1
            WHEN t1.phase= 'T2' THEN t2.EXP_T2
            WHEN t1.phase= 'T3' THEN t2.EXP_T3
        WHEN t1.phase= 'T1-T3' THEN t2.EXP_T1T3
            end) AS EXP

      FROM WORK.init t1, WORK.tmp16 t2
      
      ;
QUIT;

/*MODIFIER BORN INF A 0 SI OBS=0*/
PROC SQL;
   CREATE TABLE WORK.TMP18 AS 
   SELECT t1.outcome, 
          t1.phase, 
          t1.OBS, 
          /* inf_OBS */
            (CASE WHEN t1.OBS=0 THEN 0 ELSE t1.inf_OBS END) AS inf_OBS, 
		
          t1.sup_OBS, 
		  /* infU_OBS */
            (CASE WHEN t1.OBS=0 THEN 0 ELSE t1.infU_OBS END) AS infU_OBS, 
          t1.EXP
      FROM WORK.TMP17 t1;
QUIT;


/*RATIO OBSERVES ATTENDUS*/
PROC SQL;
   CREATE TABLE WORK.OE_&diag. AS 
   SELECT t1.*,        
            (t1.OBS/t1.EXP) AS OE,
			(t1.inf_OBS/t1.EXP) AS inf_OE,
			(t1.sup_OBS/t1.EXP) AS sup_OE,
			(t1.infU_OBS/t1.EXP) AS infU_OE
      FROM WORK.TMP18 t1;
QUIT;

proc datasets lib=WORK noprint;
  modify all_&diag.;
    format _numeric_ 10.2;
  run;

  proc datasets lib=WORK noprint;
  modify OE_&diag.;
    format _numeric_ 10.2;
  run;

  quit;
%mend;



/*Appliquer la macro OBSEXP à une liste de diagnostics et un regimen specifique*/

%macro OBSEXP_list(regimen=,diag_list=,DP_only=);
    %let num_diags = %sysfunc(countw(&diag_list));
    %do i = 1 %to &num_diags;
        %let diag = %scan(&diag_list, &i);       
        /* Appliquer macro OBSEXP sur chaque diag de la liste*/
        %OBSEXP(regimen=&regimen.,diag=&diag.,DP_only=&DP_only.);
  
    %end;
%mend;

/*TEST*/
%OBSEXP_list(regimen=all_MOD,DP_only=0,
diag_list= Appendicitis);

/*Pour obtenir les resultats sur la liste complete*/
%OBSEXP_list(regimen=all_MOD,DP_only=0,
diag_list= Acute_myocardial_infarction Appendicitis Bells_palsy Convulsions
Deep_vein_thrombosis Disseminated_intravasc_coag Encephalitis
GuillainBarre_syndrome Hemorrhagic_stroke Immune_thrombocytopenia
MyoPericarditis Narcolepsy Nonhemorrhagic_stroke
Pulmonary_embolism Transverse_myelitis Unusual_site_thrombosis
/*negative control outcomes*/
Animal_bite_wound Ankle_ulcer Cholelithiasis External_burn_and_corrosion
Feet_contusion Helminthiases Lumbar_pelvic_sprain Venomous_animals);



/*SAUVEGARDER TABLES COMPLETES*/
/*!!! Changer noms si besoin*/
/*Resultats*/
data rep.OE_alldiag_MOD42COVOUT_all ;
		set WORK.ALL_:;
	run;

/*Donnees intermediaires*/
data rep.OE_alldiag_MOD42COVOUT ;
		set WORK.OE_:;
	run;

/*Pour afficher table pour negative outcomes OU AESI seulement*/
/*outcome NOT IN ('Animal_bite_wound','Ankle_ulcer','Cholelithiasis','External_burn_and_corrosion','Feet_contusion','Helminthiases','Lumbar_pelvic_sprain','Venomous_animals')

