---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_transfert
--- Description : squelette de fonction permettant le transfert de données d'une base Postgresql à une autre, en gérant les correspondances
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_transfert () RETURNS setof varchar AS
$BODY$
DECLARE libSchema varchar;
DECLARE hostaddr varchar;
DECLARE port varchar;
DECLARE dbname varchar;
DECLARE usr varchar;
DECLARE password varchar;
DECLARE connexion varchar;
DECLARE transferer varchar;
DECLARE commande varchar;
DECLARE libTable varchar;
DECLARE libChamp varchar;
DECLARE listChamp varchar;
DECLARE listChampType varchar;
BEGIN
--- Variables-----------------------------------------------------------
-- Nom du schema sur lequel vous travaillez (hub en local)
libSchema	= '';	
-- DANS LE CAS OU les données sont dans une autre base, renseignez ces champ (sinon, laissez les vide)
hostaddr 	= '';		-- hote
port 		= '';		-- port
dbname 		= '';		-- nom de la base données
usr 		= '';		-- utilisateur
password 	= '';		-- mot de passe
CASE WHEN hostaddr 	= '' THEN
	connexion = null;
ELSE
	connexion 	= 'hostaddr='||hostaddr||' port='||port||' dbname='||dbname||' user='||usr||' password='||password;
END CASE;

--- Gestion des correspondances -----------------------------------------------------------
FOR libTable IN EXECUTE 'SELECT cd_table FROM ref.fsd GROUP BY cd_table;' LOOP
	/*liste des champs pour construire la requête*/
	EXECUTE 'SELECT string_agg(chp.cd_champ,'','') FROM (SELECT cd_champ FROM ref.fsd WHERE cd_table = '''||libTable||''' ORDER BY ordre_champ) as chp;' INTO listChamp;
	EXECUTE 'SELECT string_agg(chp.cd_champ,'','') FROM (SELECT cd_champ||'' ''||format as cd_champ FROM ref.fsd WHERE cd_table = '''||libTable||''' ORDER BY ordre_champ) as chp;' INTO listChampType;
	
	CASE libTable 
		WHEN 'metadonnees' THEN
			transferer = 'non';
			commande = '
			SELECT 
			''xxx'' 													as cd_jdd_perm,
			''xxx'' 													as cd_jdd,
			''xxx'' 													as typ_jdd,
			''xxx'' 													as lib_jdd,
			''xxx'' 													as desc_jdd,
			''xxx'' 													as date_publication,
			''xxx'' 													as rmq,
			;';
		WHEN 'metadonnees_acteur' THEN
			transferer = 'non';
			commande = '
			SELECT 
			''xxx'' 													as cd_jdd_perm,
			''xxx'' 													as cd_jdd,
			''xxx'' 													as typ_acteur,
			''xxx'' 													as nmo_acteur,
			''xxx'' 													as lib_orgm,
			''xxx'' 													as mail_acteur,
			null 														as cd_acteur,
			null 														as cd_orgm,
			''xxx'' 													as rmq,
			;';
		WHEN 'metadonnees_territoire' THEN
			transferer = 'non';
			commande = '
			SELECT 
			''xxx'' 													as cd_jdd_perm,
			''xxx'' 													as cd_jdd,
			''xxx'' 													as typ_geo,
			''xxx'' 													as cd_geo,
			''xxx'' 													as cd_refgeo,
			''xxx'' 													as version_refgeo,
			''xxx'' 													as rmq,
			;';
		WHEN 'observation' THEN
			transferer = 'non';
			commande = '
			SELECT 
			xxx 														as cd_jdd,
			xxx 														as cd_jdd_perm,
			xxx															as cd_releve,
			xxx 														as cd_releve_perm,
			xxx			 												as cd_obs_mere,
			xxx					 										as cd_obs_perm,
			xxx 														as typ_ent,
			xxx			  												as cd_ent_mere,
			xxx															as nom_ent_mere,
			xxx		 													as cd_nom, 
			xxx		 													as cd_ref, 
			xxx		 													as cd_reftaxo, 
			xxx 														as version_reftaxo, 
			xxx		 													as nom_ent_ref, 
			xxx 														as cd_obs_orig, 
			xxx 														as cd_ent_orig,
			xxx 														as nom_ent_orig, 
			xxx		 													as cd_jdd_orig, 
			xxx		 													as lib_jdd_orig, 
			xxx				 											as typ_source, 
			xxx		 													as cd_biblio,
			xxx		 													as lib_biblio, 
			xxx 														as url_biblio, 
			xxx		 													as cd_herbier, 
			xxx		 													as lib_herbier, 
			xxx 														as cd_validite,  
			xxx 														as cd_sensi, 
			xxx									 						as lib_refsensi, 
			xxx 														as version_refsensi, 
			xxx 														as propriete_obs, 
			xxx		 													as statut_pop,  
			xxx 														as typ_denombt, 
			xxx 														as denombt_min, 
			xxx 														as denombt_max, 
			xxx 														as objet_denombt, 
			xxx		 													as rmq
			FROM xxx
			WHERE xxx
			;';
		WHEN 'releve' THEN
			transferer = 'non';
			commande = '
			SELECT 
			xxx  														as cd_jdd,
			xxx		 													as cd_releve,
			xxx			 												as cd_releve_perm,
			xxx		 													as date_debut,
			xxx															as date_fin,
			xxx															as nature_date,
			xxx		 													as meth_releve,
			xxx															as typ_protocole,
			xxx		 													as rmq
			FROM xxx
			WHERE xxx
			;';
		WHEN 'releve_acteur' THEN
			transferer = 'non';
			commande = '
			SELECT 
			xxx  														as cd_jdd,
			xxx 														as cd_releve,
			xxx			 												as cd_releve_perm,
			xxx															as typ_acteur, 
			xxx						 									as nom_acteur, 
			xxx		 													as lib_orgm,
			xxx 														as mail_acteur,
			xxx 														as cd_acteur, 
			xxx 														as cd_orgm,
			xxx 														as rmq
			FROM xxx
			WHERE xxx
			;';
		WHEN 'releve_territoire' THEN
			transferer = 'non';
			commande = '
			SELECT 
			xxx		  													as cd_jdd,
			xxx		 													as cd_releve,
			xxx			 												as cd_releve_perm,
			xxx		 													as typ_geo,
			xxx			 												as cd_refgeo,
			xxx		 													as version_refgeo,
			xxx		 													as cd_geo,
			xxx		 													as lib_geo,
			xxx 														as confiance_geo,
			xxx 														as moyen_geo,
			xxx 														as nature_geo,
			xxx		 													as origine_geo,
			xxx 														as precision_geo,
			xxx 														as rmq
			FROM xxx
			WHERE xxx
			;';
		WHEN 'entite' THEN
			transferer = 'non';
			commande = '
			SELECT
			xxx															as cd_jdd_perm
			xxx															as cd_jdd
			xxx															as cd_ent_perm
			xxx															as cd_ent_mere
			xxx															as typ_ent
			xxx															as nom_ent_mere
			xxx															as cd_rang
			xxx															as cd_sup
			xxx															as famille
			xxx															as ordre
			xxx															as classe
			xxx															as phylum
			xxx															as regne
			xxx															as rmq
			FROM xxx
			WHERE xxx
			;';
		WHEN 'entite_biblio' THEN
			transferer = 'non';
			commande = '
			SELECT
			xxx															as cd_jdd
			xxx															as cd_ent_perm
			xxx															as cd_ent_mere
			xxx															as typ_biblio
			xxx															as cd_biblio
			xxx															as lib_biblio
			xxx															as url_biblio
			xxx															as rmq

			FROM xxx
			WHERE xxx
			;';
		WHEN 'entite_referentiel' THEN
			transferer = 'non';
			commande = '
			SELECT
			xxx															as cd_jdd
			xxx															as cd_ent_perm
			xxx															as cd_ent_mere
			xxx															as cd_nom
			xxx															as cd_ref
			xxx															as nom_ent_ref
			xxx															as cd_reftaxo
			xxx															as version_reftaxo
			xxx															as rmq
			FROM xxx
			WHERE xxx
			;';
		WHEN 'entite_statut' THEN
			transferer = 'non';
			commande = '
			SELECT
			xxx															as cd_jdd
			xxx															as cd_ent_perm
			xxx															as cd_ent_mere
			xxx															as typ_statut
			xxx															as cd_statut
			xxx															as critere_statut
			xxx															as meth_statut
			xxx															as annee_statut
			xxx															as metrique_statut
			xxx															as valeur_statut
			xxx															as typ_geo
			xxx															as cd_geo
			xxx															as cd_refgeo
			xxx															as version_refgeo
			xxx															as lib_geo
			xxx															as rmq
			FROM xxx
			WHERE xxx
			;';
		WHEN 'entite_verna' THEN
			transferer = 'non';
			commande = '
			SELECT
			xxx															as cd_jdd
			xxx															as cd_ent_perm
			xxx															as cd_ent_mere
			xxx															as nom_ent_verna
			xxx															as rmq
			FROM xxx
			WHERE xxx
			;';
		ELSE 
			transferer = 'non';
			commande = null;
		END CASE;

--- Lancement des commandes -----------------------------------------------------------		
	CASE WHEN (commande IS NOT null AND transferer = 'oui') THEN 
		EXECUTE 'DELETE FROM '||libSchema||'.temp_'||libTable||';';
		CASE WHEN connexion IS NOT NULL	THEN -- cas d'utilisation d'une autre base de données pour renseigner l'information.
			/*liste des champs pour construire la requête*/
			EXECUTE 'SELECT string_agg(chp.cd_champ,'','') FROM (SELECT cd_champ FROM ref.fsd WHERE cd_table = '''||libTable||''' ORDER BY ordre_champ) as chp;' INTO listChamp;
			EXECUTE 'SELECT string_agg(chp.cd_champ,'','') FROM (SELECT cd_champ||'' ''||format as cd_champ FROM ref.fsd WHERE cd_table = '''||libTable||''' ORDER BY ordre_champ) as chp;' INTO listChampType;
			EXECUTE 'INSERT INTO '||libSchema||'.temp_'||libTable||' ('||listChamp||') SELECT * FROM dblink('''||connexion||''','''||commande||''') as t1 ('||listChampType||');';
		ELSE 	-- les données sont déjà présente dans le hub sous une autre forme.
			EXECUTE 'INSERT INTO '||libSchema||'.temp_'||libTable||' ('||listChamp||') '||commande||';';
		END CASE;
		RETURN NEXT libTable||' - OK';
	WHEN (commande IS NOT null AND transferer <> 'oui') THEN 
		RETURN NEXT libTable||' - non transféré';
	ELSE
		RETURN NEXT libTable||' - pas de requête';
	END CASE;
	
END LOOP;
	
END;$BODY$ LANGUAGE plpgsql;