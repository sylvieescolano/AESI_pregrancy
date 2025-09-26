/*Creation de la table des vaccinations incluses dans la période d'etude
à partir de la table VACCOVID
+ Définition des profils vaccinaux et de la période de follow-up*/


/*Supprimer tables de WORK*/
proc datasets lib=work kill;
run;quit;  

/*Definir la librairie ObservesAttendusAESI*/
libname rep '/home/sas/42a000245310899/sasdata/REPMEDGR/Base_Grossesse/OE_TEST';

/*Definir la librairie base grossesse*/
libname baseg '/home/sas/42a000245310899/sasdata/REPMEDGR/Base_Grossesse/Construction_base_20132024';


/*PP (pregnancy period) = [LMP-70 ; dat_evt[*/


/*Compter le nombre grossesses potentiellement vaccinées durant la PP et avant 23/12/2023*/
PROC SQL;
   CREATE TABLE WORK.QUERY AS 
   SELECT (COUNT (DISTINCT (t1.indsej))) AS nb
      FROM baseg.vaccovid t1
      WHERE t1.dat_evt > '27Dec2020'd AND t1.lmp_calc-70< '23Dec2023'd;
QUIT;


/*Selectionner les vaccinations durant PP pour ces grossesses 
+ ajouter la phase "preT0" (dans les 42j avant T0)
(Les trimestres T0-T3 sont deja dans la table BASEG.VACCOVID)*/
PROC SQL;
   CREATE TABLE WORK.VACCOV1 AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          t1.dat_evt, 
          t1.EXE_SOI_DTD, 
          t1.issue, 
          t1.lmp_calc, 
          t1.nom_vac, 
          t1.num_vac, 
          t1.PHA_PRS_C13, 
          t1.timing, 
          CASE WHEN t1.trimester IS MISSING THEN "preT0" ELSE t1.trimester END AS trimester, 
          t1.type_vac, 
          t1.indsej
      FROM baseg.vaccovid t1
      WHERE  t1.dat_evt>t1.exe_soi_dtd>=t1.lmp_calc-70
	AND t1.dat_evt > '27Dec2020'd AND t1.lmp_calc-70< '23Dec2023'd;
QUIT;

/*Compter les grossesses et les BEN_NIR_ANO (BNA)*/
PROC SQL;
   CREATE TABLE WORK.count1 AS 
   SELECT /* COUNT_DISTINCT_of_BEN_NIR_ANO */
            (COUNT(DISTINCT(t1.BEN_NIR_ANO))) AS COUNT_DISTINCT_of_BEN_NIR_ANO, 
          /* COUNT_DISTINCT_of_indsej */
            (COUNT(DISTINCT(t1.indsej))) AS COUNT_DISTINCT_of_indsej
      FROM WORK.VACCOV1 t1;
QUIT;


/*TABLE FINALE DES VACCINS : garder les vaccinations <23/12/23*/
PROC SQL;
   CREATE TABLE WORK.VACCOV AS 
   SELECT DISTINCT t1.*
      FROM work.vaccov1 t1
      WHERE t1.exe_soi_dtd< '23Dec2023'd;
QUIT;

/*Compter les grossesses et les BEN_NIR_ANO (BNA)*/
PROC SQL;
   CREATE TABLE WORK.count2 AS 
   SELECT /* COUNT_DISTINCT_of_BEN_NIR_ANO */
            (COUNT(DISTINCT(t1.BEN_NIR_ANO))) AS COUNT_DISTINCT_of_BEN_NIR_ANO, 
          /* COUNT_DISTINCT_of_indsej */
            (COUNT(DISTINCT(t1.indsej))) AS COUNT_DISTINCT_of_indsej
      FROM WORK.VACCOV t1
	  /*si on souhaite compter pour les vaccinations durant T1-T3*/
	  /*WHERE t1.trimester IN ( 'T1', 'T2',  'T3'  )*/;
QUIT;




/*Verifier la coherence des dates*/
PROC SQL;
   CREATE TABLE WORK.QUERY2 AS 
   SELECT /* MIN_of_lmp_calc */
            (MIN(t1.lmp_calc)) FORMAT=DDMMYY8. AS MIN_of_lmp_calc, 
          /* MAX_of_lmp_calc */
            (MAX(t1.lmp_calc)) FORMAT=DDMMYY8. AS MAX_of_lmp_calc, 
          /* MIN_of_dat_evt */
            (MIN(t1.dat_evt)) FORMAT=DDMMYY8. AS MIN_of_dat_evt, 
          /* MAX_of_dat_evt */
            (MAX(t1.dat_evt)) FORMAT=DDMMYY8. AS MAX_of_dat_evt, 
          /* MIN_of_EXE_SOI_DTD */
            (MIN(t1.EXE_SOI_DTD)) FORMAT=DDMMYYS10. AS MIN_of_EXE_SOI_DTD, 
          /* MAX_of_EXE_SOI_DTD */
            (MAX(t1.EXE_SOI_DTD)) FORMAT=DDMMYYS10. AS MAX_of_EXE_SOI_DTD
      FROM WORK.VACCOV t1;
QUIT;

/*Décrire le numero de dose, le type de vaccin et le trimestre*/
title 'Numeros de doses';
proc freq data=work.vaccov; tables num_vac;run;

title 'Type de vaccin';
proc freq data=work.vaccov; tables nom_vac;run;

title 'Trimestre';
proc freq data=work.vaccov; tables trimester;run;


/*Ajouter les vaccins précédant PP pour définir les profils de vaccination 
en incluant tous les vaccins jusqu'a celui de la PP*/

/*Profils complets*/
PROC SQL;
   CREATE TABLE WORK.before AS 
   SELECT DISTINCT t2.indsej, 
          t2.nom_vac, 
          t2.num_vac, 
          t2.EXE_SOI_DTD
      FROM WORK.VACCOV t1
           INNER JOIN baseg.vaccovid t2 ON (t1.indsej = t2.indsej)
      WHERE t2.timing = 'before'

UNION
SELECT DISTINCT t2.indsej, 
          t2.nom_vac, 
          t2.num_vac, 
          t2.EXE_SOI_DTD
      FROM WORK.vaccov t2 ;
QUIT;

/*Ajouter trimestre de vaccin si existant*/
PROC SQL;
   CREATE TABLE WORK.BEFORE AS 
   SELECT DISTINCT t1.EXE_SOI_DTD, 
          t1.indsej, 
          t1.nom_vac, 
          t1.num_vac, 
          t2.trimester
      FROM WORK.BEFORE t1
           LEFT JOIN WORK.VACCOV t2 ON (t1.EXE_SOI_DTD = t2.EXE_SOI_DTD) AND (t1.indsej = t2.indsej);
QUIT;


/*Definir les profils de vaccination*/
proc sort data=work.before; by indsej exe_soi_dtd;run;

DATA work.profils;
SET WORK.before;
BY indsej  exe_soi_dtd;
length v_profile $42.;
IF  FIRST.indsej THEN DO; 
v_profile=trim(nom_vac)||put(num_vac,1.);
retain v_profile;
END;
else do;
v_profile=trim(v_profile)||trim(nom_vac)||put(num_vac,1.); 
retain v_profile;
end;
run;


/*PROFILS SIMPLIFIES (vaccin non ARN='OTH')*/
PROC SQL;
   CREATE TABLE WORK.BEFORE2 AS 
   SELECT DISTINCT t1.EXE_SOI_DTD, 
          t1.indsej, 
          t1.num_vac, 
          t1.trimester, 
          /* nom_vac */
            (CASE WHEN t1.nom_vac LIKE '%MOD%' OR t1.nom_vac LIKE'%BNT%' THEN t1.nom_vac ELSE 'OTH' END) AS nom_vac
      FROM WORK.BEFORE t1;
QUIT;
proc sort data=work.before2; by indsej exe_soi_dtd;run;

DATA work.profils2;
SET WORK.before2;
BY indsej  exe_soi_dtd;
length v_profile $42.;
IF  FIRST.indsej THEN DO; 
v_profile=trim(nom_vac)||put(num_vac,1.);
retain v_profile;
END;
else do;
v_profile=trim(v_profile)||trim(nom_vac)||put(num_vac,1.); 
retain v_profile;
end;
run;

title'Repartition des profils pour les vaccins pdt PP';

proc freq data=work.profils2 (where=(trimester ne ""));
tables v_profile;run;


/*Ajouter variable "regimen" 
(all_MOD si seulement des vaccins Moderna, all_BNT si seulement Pfizer,
mix_RNA si seulement Moderna et Pfizer, other si au moins un autre vaccin)
et "dose_num" (3+ si >2)*/

PROC SQL;
   CREATE TABLE WORK.regimen AS 
   SELECT DISTINCT t1.indsej, 
          t1.EXE_SOI_DTD, 
          t1.num_vac, 
          t1.v_profile, 
          /* regimen */
            ( CASE WHEN t1.v_profile  LIKE '%OTH%' THEN 'other'
            ELSE
            (CASE WHEN t1.v_profile NOT LIKE '%BNT%' AND t1.v_profile LIKE '%MOD%' THEN 'all_MOD'
             WHEN t1.v_profile  LIKE '%BNT%' AND t1.v_profile NOT LIKE '%MOD%' THEN 'all_BNT'
            ELSE 'mix_RNA' END)
            END) AS regimen,
			(CASE WHEN t1.num_vac<3 THEN put(t1.num_vac,1.) ELSE '3+' END) AS dose_num,
			t1.trimester
      FROM WORK.PROFILS2 t1
      WHERE t1.trimester ne '';
QUIT;


title 'Regimen';
proc freq data=work.regimen;
tables regimen*dose_num /NOCOL NOROW;run;

title 'Trimester';
proc freq data=work.regimen;
tables trimester /NOCOL NOROW;run;

/*Comptage par regimen, trimestre, dose_num*/
PROC SQL;
   CREATE TABLE WORK.count AS 
   SELECT t1.regimen, 
          t1.trimester, 
          t1.dose_num, 
          /* COUNT_of_indsej */
            (COUNT(t1.indsej)) AS COUNT_of_indsej
      FROM WORK.REGIMEN t1
      GROUP BY t1.regimen,
               t1.trimester,
               t1.dose_num;
QUIT;




/*Ajouter date du vaccin suivant (nextvac)*/
proc expand data = regimen out=regimen2 method=none;
   by indsej;
   id exe_soi_dtd;
   convert exe_soi_dtd=nextvac / transformout=(lead 1);
run;

/*Delai moyen avant nextvac selon dose_num*/
PROC SQL;
   CREATE TABLE WORK.delai AS 
   SELECT t1.dose_num, 
          /* MEAN_of__Calcul */
            (MEAN(t1.nextvac-t1.EXE_SOI_DTD)) AS MEAN_of__Calcul
      FROM WORK.REGIMEN2 t1
      WHERE t1.nextvac NOT IS MISSING
      GROUP BY t1.dose_num;
QUIT;



/*******Tables finales*******/

/*Definir la periode a risque post vaccinale (en jours)*/
%let duree=42;

/*Dates de debut et fin follow_up (FU) post-vac (tronquées au vaccin suivant si existant)*/
PROC SQL;
   CREATE TABLE WORK.regimen3 AS 
   SELECT DISTINCT t1.indsej, 
t1.dose_num,
          t1.regimen, 
          t1.trimester, 
		   t1.EXE_SOI_DTD as dbt_FUvac, 
          /* fin_FUvac */
            (CASE WHEN t1.nextvac IS NOT MISSING THEN
            MIN (t1.nextvac,t1.exe_soi_dtd+&duree.) ELSE t1.EXE_SOI_DTD+&duree. END) FORMAT=ddmmyy10. AS fin_FUvac
      FROM WORK.REGIMEN2 t1
      ORDER BY t1.indsej,
               t1.EXE_SOI_DTD;
QUIT;


DATA rep.allvac_42;set work.regimen3;run;

/*Pour enregistrer la table avec duree=7j*/
/*DATA rep.allvac_7;set work.regimen3;run;
