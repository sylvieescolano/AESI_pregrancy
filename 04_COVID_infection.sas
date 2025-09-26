/*Objectif: repérer les infections COVID (test ou hospitalisation)
pour les patientes de la base,
définir les périodes post-infection COVID (90j),
calculer les périodes post vac (table allvac_42_covout) hors post inf COVID*/

/*Supprimer tables de WORK et ORAUSER*/
proc datasets lib=work kill;
run;quit;   
proc datasets lib=orauser kill;
run;quit;

/*Definir la librairie ObservesAttendusAESI*/
libname rep '/home/sas/42a000245310899/sasdata/REPMEDGR/Base_Grossesse/OE_TEST';

/*Definir la librairie base grossesse*/
libname baseg '/home/sas/42a000245310899/sasdata/REPMEDGR/Base_Grossesse/Construction_base_20132024';


/*REPERAGE VIA TABLE DES DEPISTAGES (DE_DEP_F)*/

/*AJOUT IDENTIFIANT BEN_NIR_ANO (BNA) à la table des vaccinations
+Les derniers resultats d'infection COVID dans DE_DEP sont datés du 23/06/23 :
On retire les vaccinations dont l'information serait potentiellement manquante
sur la période post vaccinale (date de vaccination max : 42j avant 23/06/23)*/

PROC SQL;
   CREATE TABLE WORK.TMP1 AS 
   SELECT DISTINCT t2.BEN_NIR_ANO,
		T1.INDSEJ, 
          t1.dbt_FUvac, 
          t1.fin_FUvac
    
      FROM rep.allvac_42 t1
           INNER JOIN BASEG.BASEG t2 ON (t1.indsej = t2.indsej)
WHERE t1.dbt_FUvac <"23jun2023"d-42;
QUIT;


/*Créer la table des BNA format ORACLE POUR JOINTURE*/
PROC SQL;
   CREATE TABLE ORAUSER.BASE AS 
   SELECT DISTINCT t1.BEN_NIR_ANO
      FROM work.tmp1 t1;
QUIT;


/*Jointure avec table des depistages*/
PROC SQL;
   CREATE TABLE WORK.dep_cov AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          t2.TES_RES_COD, 
          DATEPART(t2.EXE_SOI_DTD) FORMAT=DDMMYY10. AS EXE_SOI_DTD
      FROM ORAUSER.BASE t1
           LEFT JOIN ORAVUE.DE_DEP_F t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO);
QUIT;

/*Resultats positifs*/
PROC SQL;
   CREATE TABLE WORK.COV_LAB AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          /* cov_dte */
            t1.EXE_SOI_DTD  AS cov_dte, 
			'LAB' AS cov_methode
      FROM WORK.DEP_COV t1
      WHERE t1.TES_RES_COD = 'P';
QUIT;

/*VERIFIER DATE MAX DEPISTAGE*/
PROC SQL;
   CREATE TABLE max AS 
   SELECT /* MAX_of_cov_dte */
            (MAX(t1.cov_dte)) FORMAT=DDMMYY10. AS MAX_of_cov_dte
      FROM WORK.COV_LAB t1;
QUIT;



/*REPERAGE COVID PARMI HOSPIT (CODES CIM)*/

/*Reperage parmi la table des CIM-10 de la base grossesse
U07.1 Maladie à coronavirus 2019 [COVID-19]**/

data WORK.COV_CIM (drop= cim diag_type date_diag);
	set baseG.cim_MAT;

if substr(CIM, 1, 4) ='U071' then
		do;	cov_DTE=date_diag;
			cov_methode='CIM';
			format cov_DTE ddmmyy10.;
		end;
	else delete;

RUN;

/*BNA en commun avec tmp1*/
PROC SQL;
   CREATE TABLE WORK.COV_CIM2 AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          t1.cov_DTE, 
          t1.cov_methode
      FROM WORK.COV_CIM t1
           INNER JOIN WORK.TMP1 t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO);
QUIT;


data work.cov; set cov_lab cov_cim2;run;



/*Calculer date 90j post diagnostic COVID (periode post infectieuse)*/
PROC SQL;
   CREATE TABLE WORK.elig_start AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          t1.cov_dte as start, 
          /* + 6 mois */
            (t1.cov_dte+90) FORMAT=ddmmyy10. AS end
      FROM work.cov t1
      ORDER BY t1.BEN_NIR_ANO,
               t1.cov_dte;
QUIT;

/*Merger les periodes avec overlap dans les 90j*/
data work.merge (drop= start end);
set work.elig_start;
by ben_nir_ano start end;
retain postinf_start postinf_end;
format postinf_start postinf_end ddmmyy10.;
if first.ben_nir_ano then do;   *clear the retained variables for first record of new id;
    postinf_end=.;
    postinf_start=.;
end;
if start > postinf_end then do;   *if there is a gap, or it is the first record of a new id;
    if not (first.ben_nir_ano) then output;
    postinf_start=start;
    postinf_end=end;
end;
else if end > postinf_end then do;  *update the postinf_rent period end;
    postinf_end=end;
end;
if last.ben_nir_ano then output;  *output the final record for each ID;
run;


/*Pour chaque vaccin et periode post inf,
recalculer dbt et fin de periode suivi post vac en integrant les periodes post inf
Si une des periodes post inf recouvre totalement la periode post vac, 
on ajoute des bornes "virtuelles" (01jan1600-01jan2100)*/

PROC SQL;
   CREATE TABLE WORK.tmp2 AS 
   SELECT DISTINCT t2.BEN_NIR_ANO, 
   t2.indsej,
          t2.dbt_FUvac, 
          t2.fin_FUvac, 
          t1.postinf_start, 
          t1.postinf_end, 
          /* FUvac */
            (t2.fin_FUvac - t2.dbt_FUvac) AS FUvac, 
          /* new_start */
            (CASE WHEN t1.postinf_start IS MISSING THEN t2.dbt_FUvac
            ELSE
            CASE WHEN t1.postinf_start<t2.dbt_FUvac and t1.postinf_end>t2.fin_FUvac
            THEN "01jan2100"d ELSE
            CASE WHEN t1.postinf_end <t2.fin_FUvac
            THEN MAX(t1.postinf_end,t2.dbt_FUvac) ELSE t2.dbt_FUvac END
            END END 
            ) FORMAT=DDMMYY10. AS new_start, 
          /* new_end */
            ((CASE WHEN t1.postinf_start IS MISSING THEN t2.fin_FUvac
                        ELSE
                        CASE WHEN t1.postinf_start<t2.dbt_FUvac and t1.postinf_end>t2.fin_FUvac
                        THEN "01jan1600"d ELSE
                        CASE WHEN t1.postinf_start>t2.dbt_FUvac
                        THEN MIN(t1.postinf_START,t2.fin_FUvac) ELSE t2.fin_FUvac END
                        END END 
                        )) FORMAT=ddmmyy10. AS new_end
      FROM WORK.MERGE t1
           RIGHT JOIN WORK.TMP1 t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO);
QUIT;


/*Pour chaque vaccin on récupere le max de la nouvelle date de dbt FU calculée,
et le min de la nouvelle date de fin FU calculée (bornes de la nouvelle période de suivi)
sauf si au moins une nouvelle duree<0 (periode post vac entierement recouverte)*/

PROC SQL;
   CREATE TABLE WORK.TMP3 AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
   t1.indsej,
          t1.dbt_FUvac, 
            t1.FUvac, 
          /* MAX_of_new_start */
            (MAX(t1.new_start)) FORMAT=DDMMYY10. AS MAX_of_new_start, 
          /* MIN_of_new_end */
            (MIN(t1.new_end)) FORMAT=DDMMYY10. AS MIN_of_new_end, 
          /* newFU */
            MAX(0,(MIN(t1.new_end) - MAX(t1.new_start))) AS newFU
      FROM WORK.tmp2 t1
      GROUP BY t1.BEN_NIR_ANO,
	  t1.indsej,
               t1.dbt_FUvac;
QUIT;


/*Evolution de la duree moyenne de suivi*/
PROC SQL;
   CREATE TABLE WORK.TMP4 AS 
   SELECT /* MEAN_of_FUvac */
            (MEAN(t1.FUvac)) AS MEAN_of_FUvac, 
          /* MEAN_of_newFU */
            (MEAN(t1.newFU)) AS MEAN_of_newFU
      FROM WORK.TMP3 t1;
QUIT;


/*Enregistrer la table avec les nouvelles périodes de FU et données sur les vaccins
(on retire si newFU=0)*/
PROC SQL;
   CREATE TABLE rep.allvac_42_COVOUT AS 
   SELECT DISTINCT t2.indsej, 
          t2.regimen, 
          t2.trimester, 
          t1.MAX_of_new_start LABEL="Debut FU recalculé" AS dbt_FUvac, 
          t1.MIN_of_new_end LABEL="Fin recalculee" AS fin_FUvac
      FROM WORK.TMP3 t1, REP.ALLVAC_42 t2
      WHERE (t1.indsej = t2.indsej AND t1.dbt_FUvac = t2.dbt_FUvac) 
	AND t1.newFU NE 0;
QUIT;

