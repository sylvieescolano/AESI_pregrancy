/*CALCUL DES BACKGROUND RATES (TAUX D'INCIDENCE PAR TRIMESTRE ET PAR GROUPE D'AGE)*/

/*Supprimer tables de WORK*/
proc datasets lib=work kill;
run;quit;  

/*Definir la librairie ObservesAttendusAESI*/
libname rep '/home/sas/42a000245310899/sasdata/REPMEDGR/Base_Grossesse/OE_TEST';

/*Definir la librairie base grossesse*/
libname baseg '/home/sas/42a000245310899/sasdata/REPMEDGR/Base_Grossesse/Construction_base_20132024';





/*Définition d'une macro de calcul des taux d'incidence 
d'un AESI défini (diag) 
recherché pour tous types de diagnostic (DP_only=0) ou principal seulement (DP_only=1)
pour une période définie (dbtperiode-finperiode)
parmi les grossesses selon classe d'age et trimestres
avec intervalles de confiance à95%*/

 %macro TI_AESI (dbtperiode=,finperiode=,diag =,DP_only=);

 /*Choix classes d'age*/
 proc format;
value agecl
12-19='[12-19]'
20-29='[20-29]'
30-39='[30-39]'
40-59='[40-59]'
;
run;

/*Duree de follow up (FU) par trimestre sans prise en compte de l'evt*/
PROC SQL;
   CREATE TABLE WORK.FU1 AS 
   SELECT t1.indsej, 
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

      FROM baseg.baseg t1;
QUIT;



/*CALCUL DES TAUX D'INCIDENCE PERIODE 1 (dbtperiode-finperiode)*/


/*Table des grossesses : sélection des grossesses 
(avec LMP-28<finperiode)*/

PROC SQL;
   CREATE TABLE WORK.tmp1 AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
   t1.age_ann,
   			T1.INDSEJ,
			t1.dureeG,
          t1.lmp_calc, 
          t1.dat_evt
      FROM baseg.baseG t1
	  where t1.dat_evt>&dbtperiode. and t1.lmp_calc-28<&finperiode.
;
QUIT;


/* RECHERCHE DES DIAG à partir de la table des CIM
(La table des CIM comprend tous les CIM <=31dec2024 
pour les BEN_NIR_ANO(BNA) de la base grossesses)
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
%end; 
;
QUIT;


/*Grossesses dont le BNA est associé a >1 diagnostic + Ajout des variables:
- "during" (=1 si diagnostic au cours de T0-T3 : LMP-28 / Evt -1)
-"clean_window" (=1 si diagnostic dans les 365j précédant T0 pour chaque AESI)*/

PROC SQL;
   CREATE TABLE WORK.tmp2 AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
	t1.age_ann,
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

      FROM WORK.TMP1 t1
           left JOIN WORK.DIAG t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO);
QUIT;

/*Catégorie pour chaque grossesse associée au diag via le BNA:
-"hors-suivi" si >1 diag dans la clean window
-"non-cas" si diag hors clean window et "PP"
-"cas" si aucun diag dans clean window et >1 durant "PP"*/

PROC SQL;
   CREATE TABLE WORK.TMP3 AS 
   SELECT DISTINCT  T1.INDSEJ,
   
            (CASE WHEN (MAX(t1.clean_window))=1 THEN 'hors-suivi'
				  WHEN (MAX(t1.clean_window))=0 then
							(CASE WHEN(MAX(t1.during))=1 THEN 'cas' 
									ELSE 'non-cas' END)END) AS CAT

      FROM WORK.TMP2 t1
      GROUP BY   t1.indsej;
QUIT;

title &diag.;
PROC FREQ data=work.tmp3; tables cat;run;

/*Garder premiere date de diag durant T0-T3 pour les "cas"*/
PROC SQL;
   CREATE TABLE WORK.cas AS 
   SELECT DISTINCT t1.indsej, 
     	t3.cat,
          /* MIN_of_date_diag */
            (MIN(t1.date_diag)) FORMAT=DDMMYY10. AS date_diag
      FROM WORK.TMP2 t1
           INNER JOIN WORK.TMP3 t3 ON (t1.indsej = t3.indsej)
      WHERE t3.CAT = 'cas' AND t1.during = 1
      GROUP BY t1.BEN_NIR_ANO,
               t1.indsej,
               t1.lmp_calc,
               t1.dat_evt;
QUIT;

/* Variable "out" : diag en dehors de la periode d'etude,
+definir trimestre du diag,
+ajouter durées de suivi, +retirer grossesses "hors-suivi"*/


PROC SQL;
   CREATE TABLE WORK.tmp4 AS 
   SELECT t2.indsej, 
          t2.AGE_ANN, 
          t2.dureeG, 
          t2.dat_evt, 
          t2.dbt_FUT0, 
          t2.fin_FUT0, 
          t2.dbt_FUT1, 
          t2.fin_FUT1, 
          t2.dbt_FUT2, 
          t2.fin_FUT2, 
          t2.dbt_FUT3, 
          t2.fin_FUT3, 
          CASE WHEN t1.date_diag is not missing AND
			(t1.date_diag<&dbtperiode. 
			OR t1.date_diag>&finperiode.) THEN 1 END AS out, 
          t1.date_diag, 

          /* trim_diag */
case when t1.date_diag IS NOT MISSING THEN
(CASE when t1.date_diag BETWEEN t2.dbt_FUT0 and t2.fin_FUT0 THEN "T0"  
 when t1.date_diag BETWEEN t2.dbt_FUT1 and t2.fin_FUT1 THEN "T1" 
when t1.date_diag BETWEEN t2.dbt_FUT2 and t2.fin_FUT2 THEN "T2" 
when t1.date_diag BETWEEN t2.dbt_FUT3 and t2.fin_FUT3 THEN "T3" END) END
AS trim_diag

      FROM WORK.CAS t1
           RIGHT JOIN WORK.FU1 t2 ON (t1.indsej = t2.indsej)

		   /*retirer grossesses avec evt "hors-suivi"*/
INNER JOIN (SELECT DISTINCT indsej 
              FROM WORK.TMP3 
              WHERE CAT ne 'hors-suivi') t3
   ON t3.indsej = t2.indsej 

;
QUIT;

/*compter nb grossesses avec diag or periode d'etude 
(a retirer pour compte des evts)*/
PROC SQL;
   CREATE TABLE WORK.out_&diag. AS 
   SELECT /* COUNT_of_indsej */
            (COUNT(t1.indsej)) AS COUNT_of_indsej
      FROM WORK.TMP4 t1
      WHERE t1.out = 1;
QUIT;



/*Calcul des périodes de suivi 
(comprises dans periode d'etude et tronquees a la date du diag)*/


PROC SQL;
   CREATE TABLE WORK.tmp5 AS 
   SELECT t1.indsej, 
          t1.dureeG, 
          t1.dbt_FUT0, 
          t1.fin_FUT0, 
          t1.dbt_FUT1, 
          t1.fin_FUT1, 
          t1.dbt_FUT2, 
          t1.fin_FUT2, 
          t1.dbt_FUT3, 
          t1.fin_FUT3, 
          t1.out, 
          t1.date_diag, 
          t1.trim_diag, 
          /* FUT0 */
            (MAX(0,(
            MIN(t1.fin_FUT0,t1.date_diag,&finperiode.)-max(t1.dbt_FUT0,&dbtperiode.)
            ))) AS FUT0,
			/* FUT1 */
            (MAX(0,(
            MIN(t1.fin_FUT1,t1.date_diag,&finperiode.)-max(t1.dbt_FUT1,&dbtperiode.)
            ))) AS FUT1,
			/* FUT2 */
			CASE WHEN t1.dbt_FUT2 IS NOT MISSING THEN
            (MAX(0,(
            MIN(t1.fin_FUT2,t1.date_diag,&finperiode.)-max(t1.dbt_FUT2,&dbtperiode.)
            ))) ELSE 0 END AS FUT2,
			/* FUT3 */
			CASE WHEN t1.dbt_FUT3 IS NOT MISSING THEN
            (MAX(0,(
            MIN(t1.fin_FUT3,t1.date_diag,&finperiode.)-max(t1.dbt_FUT3,&dbtperiode.)
            ))) ELSE 0 END AS FUT3
      FROM WORK.tmp4 t1
;
QUIT;

/*Table de comptage : retirer out*/
PROC SQL;
   CREATE TABLE WORK.TMP6 AS 
   SELECT t1.indsej, 
          t1.FUT0, 
          t1.FUT1, 
          t1.FUT2, 
          t1.FUT3, 
          /* T0 */
            (t1.trim_diag='T0' AND NOT  t1.out) AS T0, 
          /* T1 */
            (t1.trim_diag='T1' AND NOT  t1.out) AS T1, 
          /* T2 */
            (t1.trim_diag='T2' AND NOT  t1.out) AS T2, 
          /* T3 */
            (t1.trim_diag='T3' AND NOT  t1.out) AS T3
      FROM WORK.TMP5 t1;
QUIT;

/*ajouter variable AGE_ANN pour groupe d'ages*/
PROC SQL;
   CREATE TABLE WORK.tmp7 AS 
   SELECT t1.indsej, 
          t1.FUT0, 
          t1.FUT1, 
          t1.FUT2, 
          t1.FUT3, 
          t1.T0, 
          t1.T1, 
          t1.T2, 
          t1.T3, 
          t3.AGE_ANN
      FROM WORK.TMP6 t1
           LEFT JOIN WORK.TMP1 t3 ON (t1.indsej = t3.indsej);
QUIT;


/*CLASSES D'AGE*/
data tmp8; set WORK.tmp7;
age=put(age_ann,agecl.);
run;

/*TOUS AGES*/
PROC SQL;
   CREATE TABLE WORK.tmp9 AS 
   SELECT "all" as age length=7,
   /* PYRS_T0 */
            (SUM(t1.FUT0)/365.25) AS PYRS_T0, 
          /* PYRS_T1 */
            (SUM(t1.FUT1)/365.25) AS PYRS_T1, 
          /* PYRS_T2 */
            (SUM(t1.FUT2)/365.25) AS PYRS_T2, 
          /* PYRS_T3 */
            (SUM(t1.FUT3)/365.25) AS PYRS_T3, 
			/* PYRS_T1T3 */
            ((SUM(t1.FUT1)+SUM(t1.FUT2)+SUM(t1.FUT3))/365.25) AS PYRS_T1T3, 
          /*EVTS_T0 */
            (SUM(t1.T0)) AS EVTS_T0, 
          /*EVTS_T1 */
            (SUM(t1.T1)) AS EVTS_T1, 
          /*EVTS_T2 */
            (SUM(t1.T2)) AS EVTS_T2, 
          /*EVTS_T3 */
            (SUM(t1.T3)) AS EVTS_T3,
			/*EVTS_T1T3 */
            (SUM(t1.T1)+SUM(t1.T2)+SUM(t1.T3)) AS EVTS_T1T3
      FROM WORK.TMP8 t1;
QUIT;


/*par sous groupe d'age*/

PROC SQL;
   CREATE TABLE WORK.tmp10 AS 
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
            ((SUM(t1.FUT1)+SUM(t1.FUT2)+SUM(t1.FUT3))/365.25) AS PYRS_T1T3, 
          /*EVTS_T0 */
            (SUM(t1.T0)) AS EVTS_T0, 
          /*EVTS_T1 */
            (SUM(t1.T1)) AS EVTS_T1, 
          /*EVTS_T2 */
            (SUM(t1.T2)) AS EVTS_T2, 
          /*EVTS_T3 */
            (SUM(t1.T3)) AS EVTS_T3,
			/*EVTS_T1T3 */
            (SUM(t1.T1)+SUM(t1.T2)+SUM(t1.T3)) AS EVTS_T1T3
      FROM WORK.TMP8 t1
      GROUP BY  t1.age;
QUIT;


/*joindre les tables*/
data TMP11; set TMP9 TMP10;RUN;

/*calcul des taux d'incidence*/

PROC SQL;
   CREATE TABLE WORK.TI_&diag. AS 
   SELECT "&diag." as outcome,
t1.age, 
          t1.EVTS_T0, 
          t1.EVTS_T1, 
          t1.EVTS_T2, 
          t1.EVTS_T3, 
		  t1.EVTS_T1T3, 
          t1.PYRS_T0, 
          t1.PYRS_T1, 
          t1.PYRS_T2, 
          t1.PYRS_T3,
			t1.PYRS_T1T3, 
          /* TI_T0 */
            (t1.EVTS_T0 / t1.PYRS_T0*100000) AS TI_T0, 
          /* TI_T1 */
            (t1.EVTS_T1 / t1.PYRS_T1*100000) AS TI_T1, 
          /* TI_T2 */
            (t1.EVTS_T2 / t1.PYRS_T2*100000) AS TI_T2, 
          /* TI_T3 */
            (t1.EVTS_T3 / t1.PYRS_T3*100000) AS TI_T3,
			/* TI_T1T3 */
            (t1.EVTS_T1T3 / t1.PYRS_T1T3*100000) AS TI_T1T3
      FROM WORK.TMP11 t1;
QUIT;



/*Intervalles de confiance*/
data end_&DIAG.; set WORK.TI_&diag.;
/*IC 95% nb evts*/
          low_EVTST0 = quantile('CHISQ',.025,2*EVTS_T0)/2;
          up_EVTST0 = quantile('CHISQ',.975,2*(EVTS_T0+1))/2;
		  low_EVTST1 = quantile('CHISQ',.025,2*EVTS_T1)/2;
          up_EVTST1 = quantile('CHISQ',.975,2*(EVTS_T1+1))/2;
		  low_EVTST2 = quantile('CHISQ',.025,2*EVTS_T2)/2;
          up_EVTST2 = quantile('CHISQ',.975,2*(EVTS_T2+1))/2;
		  low_EVTST3 = quantile('CHISQ',.025,2*EVTS_T3)/2;
          up_EVTST3 = quantile('CHISQ',.975,2*(EVTS_T3+1))/2;
		  low_EVTST1T3 = quantile('CHISQ',.025,2*EVTS_T1T3)/2;
          up_EVTST1T3 = quantile('CHISQ',.975,2*(EVTS_T1T3+1))/2;
/*IC 95% TI*/
		  low_TIT0=low_EVTST0/PYRS_T0*100000;
		  up_TIT0=up_EVTST0/PYRS_T0*100000;
		  low_TIT1=low_EVTST1/PYRS_T1*100000;
		  up_TIT1=up_EVTST1/PYRS_T1*100000;
		  low_TIT2=low_EVTST2/PYRS_T2*100000;
		  up_TIT2=up_EVTST2/PYRS_T2*100000;
		  low_TIT3=low_EVTST3/PYRS_T3*100000;
		  up_TIT3=up_EVTST3/PYRS_T3*100000;
		  low_TIT1T3=low_EVTST1T3/PYRS_T1T3*100000;
		  up_TIT1T3=up_EVTST1T3/PYRS_T1T3*100000;
               run;
proc datasets lib=WORK noprint;
  modify end_&diag.;
    format _numeric_ 10.1;
  run;
quit;
%mend;




/*Appliquer la macro TI_AESI à une liste de diagnostics
(periode 2015-2019, modifiable)*/

%macro TI_AESI_list(diag_list=,DP_only=);
    %let num_diags = %sysfunc(countw(&diag_list));
    %do i = 1 %to &num_diags;
        %let diag = %scan(&diag_list, &i);
        
        /* Appliquer macro TI_AESI sur chaque diag de la liste
		+choix de la période*/
        %TI_AESI(dbtperiode='01jan2015'd,finperiode='31dec2019'd,diag=&diag.,DP_only=&DP_only.);
  
    %end;
%mend;

/*TEST*/
/*%TI_AESI_list(diag_list=Appendicitis, DP_only=0);*/

/*Pour obtenir les resultats sur la liste complete*/
%TI_AESI_list(DP_only=0,
diag_list=Acute_myocardial_infarction Appendicitis Bells_palsy Convulsions
Deep_vein_thrombosis Disseminated_intravasc_coag Encephalitis
GuillainBarre_syndrome Hemorrhagic_stroke Immune_thrombocytopenia
MyoPericarditis Narcolepsy Nonhemorrhagic_stroke
Pulmonary_embolism Transverse_myelitis Unusual_site_thrombosis
/*negative control outcomes*/
Animal_bite_wound Ankle_ulcer Cholelithiasis External_burn_and_corrosion
Feet_contusion Helminthiases Lumbar_pelvic_sprain Venomous_animals);


/*enregistrer tous les resultats(changer nom en fct parametres)*/

data rep.BGR_ALLDIAG ;
		set WORK.END_:;
	run;


/*changer . en 0*/
data rep.BGR_ALLDIAG;
   set rep.BGR_ALLDIAG;
   array variablesOfInterest _numeric_;
   do over variablesOfInterest;
      if variablesOfInterest=. then variablesOfInterest=0;
   end;
run;

/*Pour afficher table pour negative outcomes OU AESI seulement*/
/*outcome NOT IN ('Animal_bite_wound','Ankle_ulcer','Cholelithiasis','External_burn_and_corrosion','Feet_contusion','Helminthiases','Lumbar_pelvic_sprain','Venomous_animals')

