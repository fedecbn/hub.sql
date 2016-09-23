-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
---- FONCTIONS LOCALES ET GLOBALES POUR LE PARTAGE DE DONNÉES AU SEIN DU RESEAU DES CBN
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
--- Script pl/pgsql permettant de construire et manipuler une hub. Celui-ci permet de construire, importer, 
--- vérifier, exporter des données dans une base de données Postgresql dans le Format Standard de Données 
--- du réseau des CBN.
-------------------------------------------------------------------------------------------------------
--- Marche à suivre pour générer un hub fonctionnel
-------------------------------------------------------------------------------------------------------
--- 0. Créer une base de données pour y installer le hub (ou sélectionner la base de données Postgres existante)
--- CREATE DATABASE hub ENCODING = 'UTF8';

--- 1. Lancer le fichier hub.sql
--- La fonction hub_admin_init sera lancée automatiquement.

--- 2. Installer le hub							
--- SELECT * FROM hub_connect_ref([hote], '5433','si_flore_national',[utilisateur],[mdp],'all')
--- SELECT * FROM hub_admin_clone('hub')

--- 3. Importer un jeu de données (ex :  TAXA)
--- SELECT * FROM hub_import('hub','taxa',[path]);

--- 4. Vérifier le jeu de données (ex : TAXA)
--- SELECT * FROM hub_verif('hub','taxa');

--- 5. Pousser les données dans la partie propre (ex : TAXA)
--- SELECT * FROM hub_push('hub','taxa');

--- 6. Envoyer les données sur le hub national (ex : TAXA)
--- SELECT * FROM hub_connect([hote], '5433','si_flore_national',[utilisateur],[mdp], 'taxa', 'hub', [trigramme_cbn]);

--- 7. Récupérer des données depuis le hub national vers le hub local(ex : TAXA)
--- SELECT * FROM hub_connect([hote], '5433','si_flore_national',[utilisateur],[mdp], 'taxa', [trigramme_cbn], 'hub');


-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
--- Fonction Admin------------------------------------------------------------------------------------- 
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

---si zz_log a déjà été créé et qu'un message d'erreur apparait à propos de la variable out.user_log, lancer la requête suivante:
--ALTER TABLE public.zz_log add column user_log varchar;
--ALTER TABLE [libelle_schema_hub].zz_log add column user_log varchar;

--- Initiations de tables indispensables pour le fonctionement du hub
CREATE TABLE IF NOT EXISTS public.zz_log (lib_schema character varying,lib_table character varying,lib_champ character varying,typ_log character varying,lib_log character varying,nb_occurence character varying,date_log timestamp,user_log varchar);
CREATE TABLE IF NOT EXISTS public.bilan (uid integer NOT NULL,lib_cbn character varying,data_nb_releve integer,data_nb_observation integer,data_nb_taxon integer,taxa_nb_taxon integer,temp_data_nb_releve integer,temp_data_nb_observation integer,temp_data_nb_taxon integer,temp_taxa_nb_taxon integer,derniere_action character varying, date_derniere_action date,CONSTRAINT bilan_pkey PRIMARY KEY (uid));
DROP TABLE IF EXISTS twocol CASCADE;	CREATE TABLE public.twocol (col1 varchar, col2 varchar);
DROP TABLE IF EXISTS threecol CASCADE;	CREATE TABLE public.threecol (col1 varchar, col2 varchar, col3 varchar);

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_admin_init
--- Description : initialise les fonction du hub (supprime toutes les fonctions) et  initialise certaines tables (zz_log et bilan)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_admin_init() RETURNS void  AS 
$BODY$ 
DECLARE cmd varchar;
DECLARE fonction varchar;
DECLARE listFunction varchar[];
DECLARE schem varchar;
DECLARE tabl varchar;
DECLARE exist varchar;
DECLARE date_log timestamp;
BEGIN 
--- Variable
SELECT CURRENT_TIMESTAMP INTO date_log;	
--- Récupération de toutes les fonction du hub
cmd = 'SELECT routine_name||''(''||string_agg(data_type ,'','')||'')'' FROM(
	SELECT routine_name, z.specific_name, 
		CASE WHEN z.data_type  = ''ARRAY'' THEN z.udt_name||''[]'' 
		WHEN z.data_type  = ''USER-DEFINED'' THEN z.udt_name 
		ELSE z.data_type END as data_type
	FROM information_schema.routines a
	JOIN information_schema.parameters z ON a.specific_name = z.specific_name
	WHERE  routine_name LIKE ''hub_%''
	ORDER BY routine_name,ordinal_position
	) as one
	GROUP BY routine_name, specific_name';
--- Suppression de ces fonctions
FOR fonction IN EXECUTE cmd
   LOOP EXECUTE 'DROP FUNCTION '||fonction||';';
   END LOOP;

-- Fonctions utilisées par le hub
listFunction = ARRAY['dblink','uuid-ossp','postgis'];
FOREACH fonction IN ARRAY listFunction LOOP
	EXECUTE 'SELECT extname from pg_extension WHERE extname = '''||fonction||''';' INTO exist;
	CASE WHEN exist IS NULL THEN EXECUTE 'CREATE EXTENSION "'||fonction||'";';
	ELSE END CASE;
END LOOP;

/* emailing queue - pour la fonction de notification (=> utilisation d'un cron)*/
CREATE TABLE IF NOT EXISTS public.emailing_queue (lib_schema character varying,action character varying,date_log timestamp,user_log varchar);
GRANT SELECT, INSERT ON TABLE public.emailing_queue TO public;
/* publicating queue - pour la fonction de hub_to_siflore (=> utilisation d'un cron)*/
CREATE TABLE IF NOT EXISTS public.publicating_queue (lib_schema character varying,jdd varchar, version integer);
GRANT SELECT, INSERT ON TABLE public.publicating_queue TO public;

/*ajout du user_log dans le zzlog*/
SELECT 1 into exist FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'zz_log' AND column_name = 'user_log';
CASE WHEN exist IS NULL THEN
	ALTER TABLE public.zz_log add column user_log varchar;
ELSE END CASE;

END;$BODY$ LANGUAGE plpgsql;

--- Lancé à chaque fois pour réinitialier les fonctions
SELECT * FROM hub_admin_init();

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_admin_clone 
--- Description : Création d'un hub complet
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_admin_clone(libSchema varchar, typ varchar = 'all') RETURNS setof zz_log AS 
$BODY$ 
DECLARE out zz_log%rowtype; 
DECLARE result threecol%rowtype; 
DECLARE flag integer; 
DECLARE typjdd varchar; 
DECLARE cd_table varchar; 
DECLARE list_champ varchar; 
DECLARE list_champ_sans_format varchar; 
DECLARE list_contraint varchar; 
DECLARE schema_lower varchar; 
DECLARE valeurs varchar; 
BEGIN
--- Variable
schema_lower = lower(libSchema);
EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = '''||schema_lower||''';' INTO flag;
--- Commande
CASE WHEN flag = 1 THEN
	out.lib_log := 'Schema '||schema_lower||' existe déjà';
ELSE 
	EXECUTE 'CREATE SCHEMA "'||schema_lower||'";';

	--- Création d'un schema
	FOR typjdd IN SELECT typ_jdd FROM ref.fsd GROUP BY typ_jdd
	LOOP
		FOR cd_table IN EXECUTE 'SELECT cd_table FROM ref.fsd WHERE typ_jdd = '''||typjdd||''' GROUP BY cd_table'
		LOOP
			SELECT * INTO out FROM hub_admin_create(schema_lower,cd_table, typ);
		END LOOP;
	END LOOP;

	--- LISTE TAXON
	EXECUTE '
	CREATE TABLE "'||schema_lower||'".zz_log_liste_taxon  (cd_ref character varying, nom_valide character varying);
	CREATE TABLE "'||schema_lower||'".zz_log_liste_taxon_et_infra  (cd_ref_demande character varying, nom_valide_demande character varying, cd_ref_cite character varying, nom_complet_cite character varying, rang_cite character varying, cd_taxsup_cite character varying);
	';
	--- LOG
	EXECUTE 'CREATE TABLE "'||schema_lower||'".zz_log  AS SELECT * FROM public.zz_log LIMIT 0;';
	out.lib_log := 'Schema '||schema_lower||' créé';
END CASE;
--- Output&Log
out.lib_schema := schema_lower;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_admin_clone';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
PERFORM hub_log (schema_lower, out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_admin_drop
--- Description : Supprimer un hub dans sa totalité (ATTENTION, NON REVERSIBLE)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_admin_drop(libSchema varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE flag integer;
BEGIN
--- Commandes
EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = '''||libSchema||''';' INTO flag;
CASE flag WHEN 1 THEN
	EXECUTE 'DROP SCHEMA IF EXISTS "'||libSchema||'" CASCADE;';
	out.lib_log := 'Schema '||libSchema||' supprimé';
ELSE out.lib_log := 'Schema '||libSchema||' inexistant pas dans le Hub';
END CASE;
--- Output&Log
out.lib_schema := libSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_admin_drop';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log ('public', out);RETURN next out;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_admin_ref 
--- Description : Création des référentiels
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_admin_ref(typAction varchar, path varchar = '/home/hub/00_ref/', ref varchar = null, version varchar = '3.2') RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE flag1 integer;
DECLARE flag2 integer;
DECLARE champFonction varchar;
DECLARE libTable varchar;
DECLARE delimitr varchar;
DECLARE structure varchar;
BEGIN
--- Output
out.lib_schema := '-';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_admin_ref';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
---Variables

--- Commandes 
CASE WHEN typAction = 'drop' THEN	--- Suppression
	EXECUTE 'SELECT DISTINCT 1 FROM information_schema.schemata WHERE schema_name = ''ref''' INTO flag1;
	CASE WHEN flag1 = 1 THEN DROP SCHEMA IF EXISTS ref CASCADE; out.lib_log := 'Schema ref supprimé';RETURN next out;
	ELSE out.lib_log := 'Schéma ref inexistant';RETURN next out;END CASE;

WHEN typAction = 'delete' THEN	--- Suppression
	EXECUTE 'SELECT DISTINCT 1 FROM information_schema.schemata WHERE schema_name = ''ref''' INTO flag1;
	CASE WHEN flag1 = 1 THEN
		CASE WHEN ref IS NULL THEN 
			FOR libTable IN EXECUTE 'SELECT nom_ref FROM ref.aa_meta GROUP BY nom_ref ORDER BY nom_ref' LOOP
				EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = ''ref'' AND tablename = '''||libTable||''';' INTO flag2;
				CASE WHEN flag2 = 1 THEN EXECUTE 'DROP TABLE ref."'||libTable||'" CASCADE';  out.lib_log := 'Table '||libTable||' supprimée';RETURN next out;
				ELSE out.lib_log := 'Table '||libTable||' inexistante'; END CASE;
			END LOOP;
		ELSE EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = ''ref'' AND tablename = '''||ref||''';' INTO flag2;
			CASE WHEN flag2 = 1 THEN EXECUTE 'DROP TABLE ref."'||libTable||'" CASCADE';  out.lib_log := 'Table '||libTable||' supprimée';RETURN next out;
			ELSE out.lib_log := 'Table '||libTable||' inexistante'; END CASE;
		END CASE;
	ELSE out.lib_log := 'Schéma ref inexistant';RETURN next out;END CASE;

WHEN typAction = 'create' THEN	--- Creation
	EXECUTE 'SELECT DISTINCT 1 FROM information_schema.schemata WHERE schema_name =  ''ref''' INTO flag1;
	CASE WHEN flag1 = 1 THEN out.lib_log := 'Schema ref déjà créés';RETURN next out;ELSE CREATE SCHEMA "ref"; out.lib_log := 'Schéma ref créés';RETURN next out;END CASE;
	--- Initialisation du meta-référentiel
	CREATE TABLE IF NOT EXISTS ref.aa_meta(id serial NOT NULL, nom_ref varchar, typ varchar, ordre integer, libelle varchar, format varchar, CONSTRAINT aa_meta_pk PRIMARY KEY(id));
	TRUNCATE ref.aa_meta;
	EXECUTE 'COPY ref.aa_meta (nom_ref, typ, ordre, libelle, format) FROM '''||path||'aa_meta.csv'' HEADER CSV ENCODING ''UTF8'' DELIMITER E''\t'';';
	--- Tables
	CASE WHEN ref IS NULL THEN 
		FOR libTable IN EXECUTE 'SELECT nom_ref FROM ref.aa_meta GROUP BY nom_ref  ORDER BY nom_ref' LOOP
			EXECUTE 'SELECT * FROM hub_ref_create('''||libTable||''','''||path||''')';
		END LOOP;
	ELSE 
		libTable = ref;
		EXECUTE 'SELECT * FROM hub_ref_create('''||libTable||''','''||path||''')';
	END CASE;
	-- Mise à jour des séquences
	PERFORM hub_reset_sequence();

WHEN typAction = 'update' THEN	--- Mise à jour
	EXECUTE 'SELECT DISTINCT 1 FROM information_schema.schemata WHERE schema_name =  ''ref''' INTO flag1;
	CASE WHEN flag1 = 1 THEN out.lib_log := 'Schema ref déjà créés';RETURN next out;ELSE CREATE SCHEMA "ref"; out.lib_log := 'Schéma ref créés';RETURN next out;END CASE;
	--- Tables
	CASE WHEN ref IS NULL THEN 
		FOR libTable IN EXECUTE 'SELECT nom_ref FROM ref.aa_meta GROUP BY nom_ref' LOOP 
			EXECUTE 'SELECT * FROM hub_ref_update('''||libTable||''','''||path||''')';
		END LOOP;
	ELSE 
		libTable = ref;
		EXECUTE 'SELECT * FROM hub_ref_update('''||libTable||''','''||path||''')';
	END CASE;
	-- Mise à jour des séquences
	PERFORM hub_reset_sequence();	

WHEN typAction = 'export_xml' THEN	--- Mise à jour
	--- Tables
	FOR libTable IN SELECT distinct cd_table FROM ref.fsd
	LOOP
	--- Partie temp
	EXECUTE 'COPY (SELECT
	''<?xml version="1.0" encoding="UTF-8"?><schema dbmsId="postgres_id">''||string_agg("XML",'''')||''</schema>'' FROM
	(
	SELECT xmlelement(name column, 
		xmlattributes('''' as "comment",'''' as "default",cd_champ as "label",255 as "length",cd_champ as "originalDbColumnName",'''' as "pattern",0 as "precision",
			false as "key",
			true as "nullable",
			''id_String'' as "talendType",
			''VARCHAR'' as "type")
	)::varchar as "XML"
	FROM ref.fsd a WHERE cd_table = '''||libTable||''' ORDER BY ordre_champ) as one)
	TO '''||path||'st_talend_temp_'||libTable||'_'||version||'.xml'' ENCODING ''UTF8'';';

	--- Partie propre
	EXECUTE 'COPY (SELECT
	''<?xml version="1.0" encoding="UTF-8"?><schema dbmsId="postgres_id">''||string_agg("XML",'''')||''</schema>'' FROM
	(
	SELECT xmlelement(name column, 
		xmlattributes('''' as "comment",'''' as "default",cd_champ as "label",255 as "length",cd_champ as "originalDbColumnName",'''' as "pattern",0 as "precision",
			CASE unicite WHEN ''Oui'' THEN true ELSE false END as "key",
			CASE unicite WHEN ''Oui'' THEN false ELSE true END as "nullable",
			CASE format WHEN  ''character varying'' THEN ''id_String'' WHEN ''float'' THEN ''id_Float'' WHEN ''date'' THEN ''id_Date'' WHEN ''integer'' THEN ''id_Integer'' WHEN ''boolean'' THEN ''id_Boolean'' ELSE format END as "talendType",
			CASE format WHEN  ''character varying'' THEN ''VARCHAR'' WHEN ''float'' THEN ''FLOAT8'' WHEN ''date'' THEN ''DATE'' WHEN ''integer'' THEN ''INT4'' WHEN ''boolean'' THEN ''BOOL'' ELSE format END as "type")
	)::varchar as "XML"
	FROM ref.fsd a WHERE cd_table = '''||libTable||''' ORDER BY ordre_champ) as one)
	TO '''||path||'/st_talend_'||libTable||'_'||version||'.xml'' ENCODING ''UTF8'';';
	out.lib_log := 'st_talend_'||libTable||'_'||version||'.xml exporté';RETURN next out;
	END LOOP;
	
WHEN typAction = 'export' THEN	--- Mise à jour
	--- meta-table
	EXECUTE 'COPY (SELECT * FROM ref.aa_meta) TO '''||path||'/aa_meta.csv'' CSV HEADER DELIMITER E'';'' ENCODING ''UTF8'' ;';
	--- Tables
	FOR libTable IN SELECT tablename FROM pg_tables WHERE schemaname = 'ref' AND tablename <> 'aa_meta'
	LOOP
	EXECUTE 'SELECT CASE WHEN libelle = ''virgule'' THEN '','' WHEN libelle = ''tab'' THEN ''\t'' WHEN libelle = ''point_virgule'' THEN '';'' ELSE '';'' END as delimiter FROM ref.aa_meta WHERE nom_ref = '''||libTable||''' AND typ = ''delimiter''' INTO delimitr;
	EXECUTE 'COPY (SELECT * FROM ref.'||libTable||') TO '''||path||'/ref_'||libTable||'.csv'' CSV HEADER DELIMITER E'''||delimitr||''' ENCODING ''UTF8'' ;';
	out.lib_log := 'ref_'||libTable||'.csv exporté';RETURN next out;
	END LOOP;
ELSE out.lib_log := 'Action non reconnue';RETURN next out;
END CASE;

--- Log
PERFORM hub_log ('public', out); 
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_admin_maj
--- Description : Mise à jour du hub
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_admin_maj(libSchema varchar, utilisateur varchar, mot_de_passe varchar, epsg integer = 2154) RETURNS setof zz_log  AS 
$BODY$ 
DECLARE out zz_log%rowtype;
DECLARE test varchar;
BEGIN
-- mise à jour des référentiels
SELECT * into out FROM hub_connect_ref('94.23.218.10', '5433', 'si_flore_national',utilisateur,mot_de_passe,'all');
PERFORM hub_log (libSchema, out);RETURN next out;

-- mise à jour de la structure.
EXECUTE 'UPDATE ref.fsd SET srid_geom = '||epsg||' WHERE srid_geom = 2154';
SELECT * into out FROM hub_admin_refresh(libSchema,'maj_structure');
PERFORM hub_log (libSchema, out);RETURN next out;

--- ATTENTION : si des données sont dans la partie propre et qu'elle ne sont pas conforme au niveau du vocabulaire contrôlé, la fonction retournera une erreur. Récupérez les données dans la partie temporaire et videz la partie propre avant de lancer cette commande


-- Cas particulier -permet de transformer les champ date_debut et date_fin en format date (initialement, ces champs était au format texte).
SELECT data_type into test FROM information_schema.columns where table_name = 'releve' AND table_schema = 'hub' AND column_name = 'date_debut';
CASE WHEN test = 'character varying' THEN
	SELECT * FROM hub_admin_refresh(libSchema,'date');PERFORM hub_log (libSchema, out);RETURN next out;
ELSE END CASE;
--- ATTENTION : si des données sont dans la partie propre et qu'elle ne sont pas conforme au niveau des dates, la fonction retournera une erreur. Récupérez les données dans la partie temporaire et videz la partie propre avant de lancer cette commande

--- Output&Log
out.lib_log := 'Mise à jour réalisée';
out.lib_schema := libSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_admin_maj';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log (libSchema, out);RETURN next out;
END; $BODY$ LANGUAGE plpgsql;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_admin_refresh
--- Description : Pour mettre à jour la structure du FSD lors de changement benin (type de donnée, clé primaire)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_admin_refresh(libSchema varchar, typ varchar = null) RETURNS setof zz_log  AS 
$BODY$ 
DECLARE out zz_log%rowtype;
DECLARE result threecol%rowtype;
DECLARE result2 twocol%rowtype;
DECLARE list_champ varchar;
DECLARE list_contraint varchar;
DECLARE list_champ_sans_format varchar;
DECLARE valeurs varchar;
DECLARE libTable varchar;
DECLARE sch_from varchar;
DECLARE sch_to varchar;
BEGIN
CASE WHEN typ = 'date' THEN
	EXECUTE 'ALTER TABLE '||libSchema||'.releve ALTER COLUMN date_debut SET DATA TYPE date USING date_debut::date;
		ALTER TABLE '||libSchema||'.releve ALTER COLUMN date_fin SET DATA TYPE date USING date_fin::date;';
	out.lib_log := 'Refresh date OK';
WHEN typ = 'check' THEN
	FOR libTable IN SELECT cd_table FROM ref.fsd GROUP BY cd_table LOOP
		PERFORM hub_add_constraint_check(libSchema, libTable);
	END LOOP;
	out.lib_log := 'Refresh CHECK OK';
WHEN typ = 'geom' THEN
	FOR libTable IN SELECT cd_table FROM ref.fsd GROUP BY cd_table LOOP
		PERFORM hub_add_constraint_geom(libSchema, libTable);
	END LOOP;
	out.lib_log := 'Refresh GEOM srid OK';
WHEN typ = 'maille' THEN
	EXECUTE 'UPDATE '||libSchema||'.releve_territoire SET cd_geo = ''10kmL93''||cd_geo WHERE typ_geo = ''m10'' AND cd_geo NOT LIKE ''10kmL93%'';';
	EXECUTE 'UPDATE '||libSchema||'.temp_releve_territoire SET cd_geo = ''10kmL93''||cd_geo WHERE typ_geo = ''m10'' AND cd_geo NOT LIKE ''10kmL93%'';';
	EXECUTE 'UPDATE '||libSchema||'.releve_territoire SET cd_geo = ''5kmL93''||cd_geo WHERE typ_geo = ''m5'' AND cd_geo NOT LIKE ''5kmL93%'';';
	EXECUTE 'UPDATE '||libSchema||'.temp_releve_territoire SET cd_geo = ''5kmL93''||cd_geo WHERE typ_geo = ''m5'' AND cd_geo NOT LIKE ''5kmL93%'';';
	out.lib_log := 'Refresh maille OK';
WHEN typ = 'lib_commune' THEN
	EXECUTE 'UPDATE '||libSchema||'.releve_territoire SET lib_geo = nom_comm FROM (SELECT insee_comm, nom_comm FROM ref.geo_commune) com WHERE com.insee_comm = cd_geo AND typ_geo = ''com'' AND (lib_geo IS NULL OR lib_geo = ''I'');';
	EXECUTE 'UPDATE '||libSchema||'.temp_releve_territoire SET lib_geo = nom_comm FROM (SELECT insee_comm, nom_comm FROM ref.geo_commune) com WHERE com.insee_comm = cd_geo AND typ_geo = ''com'' AND (lib_geo IS NULL OR lib_geo = ''I'');';
	out.lib_log := 'Refresh commune OK';
WHEN typ = 'maj_structure' THEN
	/*Création des tables inexistantes*/
	FOR libTable IN EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd LEFT JOIN information_schema.tables ON table_name = cd_table AND table_schema = '''||libSchema||''' WHERE table_name IS NULL ORDER BY cd_table;' LOOP
		PERFORM hub_admin_create(libSchema, libTable);
	END LOOP;
	/*Création des champs inexistantes*/
	FOR result IN EXECUTE 'SELECT cd_table, cd_champ, format FROM ref.fsd LEFT JOIN information_schema.columns ON table_name = cd_table AND column_name = cd_champ AND table_schema = '''||libSchema||''' WHERE column_name IS NULL;' LOOP
		EXECUTE 'ALTER TABLE '||libSchema||'.'||result.col1||' ADD COLUMN '||result.col2||' '||result.col13||';';
	END LOOP;
	/*mise à jour de toutes les contraintes check*/
	FOR libTable IN SELECT cd_table FROM ref.fsd GROUP BY cd_table LOOP
		PERFORM hub_add_constraint_check(libSchema, libTable);
		PERFORM hub_add_constraint_geom(libSchema, libTable);
	END LOOP;
	/*zzlog user_log*/
	PERFORM hub_add_champ(libSchema, 'zz_log', 'user_log', 'varchar');
	out.lib_log := 'maj_structure OK';
ELSE out.lib_log := 'No refresh';
END CASE;

--- Output&Log
out.lib_schema := libSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_admin_refresh';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log (libSchema, out);RETURN next out;
END; $BODY$ LANGUAGE plpgsql;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_admin_user_add
--- Description : Ajouter un utilisateur et lui donne les droits nécessaires
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_admin_user_add(utilisateur varchar, mdp varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
declare db_nam varchar;
declare flag integer;
BEGIN
SELECT catalog_name INTO db_nam FROM information_schema.information_schema_catalog_name;

EXECUTE 'SELECT DISTINCT 1 FROM information_schema.enabled_roles WHERE role_name <> '''||utilisateur||''';' INTO flag;
CASE WHEN flag = 1 THEN 
EXECUTE 'CREATE USER "'||utilisateur||'" PASSWORD '''||mdp||''';
	GRANT CONNECT ON DATABASE '||db_nam||' TO "'||utilisateur||'" ;
	';
	out.lib_log := utilisateur||' ajouté';
ELSE out.lib_log := 'ERREUR : '||utilisateur||' existe déjà';
END CASE;

out.lib_schema := 'public';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_admin_userdrop';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log ('public', out);RETURN next out;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_admin_user_drop
--- Description : Supprime un utilisateur
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_admin_user_drop(utilisateur varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE db_nam varchar;
DECLARE listSchem varchar;
BEGIN
SELECT catalog_name INTO db_nam FROM information_schema.information_schema_catalog_name;
EXECUTE 'SELECT * FROM hub_admin_right_drop('''||utilisateur||''')';
EXECUTE 'DROP USER IF EXISTS "'||utilisateur||'";';
--- Output&Log
out.lib_log := utilisateur||' supprimé';out.lib_schema := 'public';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_admin_user_drop';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log ('public', out);RETURN next out;
END;$BODY$LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_admin_right_add
--- Description : Ajouter des droits à un utilisateur
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_admin_right_add(utilisateur varchar, schma varchar, role varchar = 'lecteur') RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
declare db_nam varchar;
declare exist varchar;
declare listSchem varchar;
BEGIN
SELECT catalog_name INTO db_nam FROM information_schema.information_schema_catalog_name;

--- Tables hub
FOR listSchem in SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema <> 'pg_catalog' AND table_schema <> 'information_schema'
	LOOP
		EXECUTE 'GRANT USAGE ON SCHEMA "'||listSchem||'" TO "'||utilisateur||'";';
		CASE WHEN listSchem = schma AND role = 'gestionnaire' THEN
			EXECUTE 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "'||listSchem||'" TO "'||utilisateur||'";';
			EXECUTE 'SELECT * FROM hub_admin_right_dblink('''||utilisateur||''');';
		ELSE EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA "'||listSchem||'" TO "'||utilisateur||'";';
		END CASE;
	END LOOP;

--- Tables zz_log
EXECUTE 'SELECT schema_name FROM information_schema.schemata WHERE schema_name = '''||schma||''';' INTO exist;
CASE WHEN exist IS NOT NULL AND role = 'lecteur' THEN
	EXECUTE '
		GRANT INSERT ON TABLE '||schma||'.zz_log TO "'||utilisateur||'";
		GRANT INSERT,DELETE ON TABLE '||schma||'.zz_log_liste_taxon TO "'||utilisateur||'";
		GRANT INSERT,DELETE ON TABLE '||schma||'.zz_log_liste_taxon_et_infra TO "'||utilisateur||'";
		';
	ELSE END CASE;

out.lib_log := utilisateur||' a les droits de '||role||' sur '||schma;out.lib_schema := 'public';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_admin_right_add';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log ('public', out);RETURN next out;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_admin_right_drop
--- Description : Supprime des droits à un utilisateur
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_admin_right_drop(utilisateur varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE db_nam varchar;
DECLARE listSchem varchar;
BEGIN
SELECT catalog_name INTO db_nam FROM information_schema.information_schema_catalog_name;
EXECUTE '
	REVOKE ALL PRIVILEGES ON DATABASE '||db_nam||' FROM "'||utilisateur||'";
	---REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA "public" FROM "'||utilisateur||'";
	---REVOKE ALL PRIVILEGES ON SCHEMA "public" FROM "'||utilisateur||'";
	---REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA "ref" FROM "'||utilisateur||'";
	---REVOKE ALL PRIVILEGES ON SCHEMA "ref" FROM "'||utilisateur||'";
	';
FOR listSchem in SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema != 'pg_catalog' AND table_schema != 'information_schema'
	LOOP
		EXECUTE '
		REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA "'||listSchem||'" FROM "'||utilisateur||'";
		REVOKE ALL PRIVILEGES ON SCHEMA "'||listSchem||'" FROM "'||utilisateur||'";
		';
	END LOOP;

EXECUTE 'SELECT * FROM hub_admin_right_dblink ('''||utilisateur||''', false)';

--- Output&Log
out.lib_log := 'Droits supprimés pour '||utilisateur;out.lib_schema := 'public';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_admin_right_drop';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log ('public', out);RETURN next out;
END;$BODY$LANGUAGE plpgsql;





-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
--- Fonction Data management --------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_aggregate
--- Description : Met à jour le schema agregation
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_aggregate(libSchema varchar = 'all', jdd varchar = 'all') RETURNS setof zz_log  AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE typJdd varchar;
DECLARE libTable varchar;
DECLARE lschema varchar;
DECLARE wher varchar;
DECLARE ct integer;
DECLARE ct2 integer;
BEGIN
--- Output&Log
out.lib_schema := libSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_aggregate';out.nb_occurence := '-'; SELECT CURRENT_TIMESTAMP INTO out.date_log; out.user_log := current_user;
--- variables
CASE WHEN jdd = 'data' OR jdd = 'taxa' 
	THEN typJdd = jdd; wher = 'WHERE typ_jdd = '''||typjdd||''' OR typ_jdd = ''meta''';
WHEN jdd = 'all' 
	THEN typJdd = jdd; wher = ''; 
ELSE EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'".metadonnees WHERE cd_jdd = '''||jdd||''';' INTO typJdd; wher = 'WHERE typ_jdd = '''||typjdd||''' OR typ_jdd = ''meta''';
END CASE;
--- Commandes
CASE WHEN (jdd = 'all' AND libSchema = 'all') THEN
	SELECT * INTO out FROM hub_truncate('agregation', 'propre');	
	FOR libTable IN EXECUTE 'SELECT cd_table FROM ref.fsd '||wher||' GROUP BY cd_table' LOOP
		FOR lschema IN SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('agregation','ref','public','ref','information_schema') AND schema_name NOT LIKE 'pg_%' ORDER BY schema_name LOOP
			ct = ct+1;
			SELECT * INTO out FROM hub_add(lschema,'agregation', libTable, libTable , jdd, 'push_total'); 
				CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; PERFORM hub_log (lschema, out); ELSE ct2 = ct2+1; END CASE;
		END LOOP;
	END LOOP;	
 WHEN jdd = 'all' THEN
	SELECT * INTO out FROM hub_clear_plus(libSchema,'agregation', 'data', 'propre', 'propre');
	SELECT * INTO out FROM hub_clear_plus(libSchema,'agregation', 'taxa', 'propre', 'propre');
	FOR libTable IN EXECUTE 'SELECT cd_table FROM ref.fsd '||wher||' GROUP BY cd_table' LOOP
		ct = ct+1;
		SELECT * INTO out FROM hub_add(libSchema,'agregation', libTable, libTable , 'data', 'push_total'); 
		SELECT * INTO out FROM hub_add(libSchema,'agregation', libTable, libTable , 'taxa', 'push_total'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; PERFORM hub_log (libSchema, out); ELSE ct2 = ct2+1; END CASE;
	END LOOP;
WHEN libSchema = 'all' THEN
	EXECUTE 'SELECT *  FROM hub_clear(''agregation'', '''||typJdd||''', ''propre'');' INTO out;
	FOR libTable IN EXECUTE 'SELECT cd_table FROM ref.fsd '||wher||' GROUP BY cd_table' LOOP
		FOR lschema IN SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('agregation','ref','public','ref','information_schema') AND schema_name NOT LIKE 'pg_%' ORDER BY schema_name LOOP
			ct = ct+1;
			SELECT * INTO out FROM hub_add(lschema,'agregation', libTable, libTable , jdd, 'push_total'); 
				CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; PERFORM hub_log (lschema, out); ELSE ct2 = ct2+1; END CASE;
		END LOOP;
	END LOOP;	
 ELSE
	SELECT * INTO out FROM hub_clear_plus(libSchema,'agregation', jdd, 'propre', 'propre');
	FOR libTable IN EXECUTE 'SELECT cd_table FROM ref.fsd '||wher||' GROUP BY cd_table' LOOP
		ct = ct+1;
		SELECT * INTO out FROM hub_add(libSchema,'agregation', libTable, libTable , jdd, 'push_total'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; PERFORM hub_log (libSchema, out); ELSE ct2 = ct2+1; END CASE;
	END LOOP;
END CASE;
--- Output&Log
CASE 
	WHEN (ct = ct2) THEN out.lib_log := 'Partie propre CBN vide - jdd = '||jdd; out.nb_occurence := jdd; 
	WHEN (ct <> ct2) THEN out.lib_log := 'Données poussées - jdd = '||jdd; out.nb_occurence := jdd;
ELSE END CASE;
CASE WHEN libSchema = 'all' THEN  libSchema = 'public'; ELSE END CASE;
PERFORM hub_log (libSchema, out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_bilan
--- Description : Met à jour le bilan sur les données disponibles dans un schema
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_bilan(libSchema varchar) RETURNS setof zz_log  AS 
$BODY$
DECLARE out zz_log%rowtype;
BEGIN
--- Output&Log
out.lib_schema := libSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_bilan';out.nb_occurence := '-'; SELECT CURRENT_TIMESTAMP INTO out.date_log; out.user_log := current_user;
--- Commandes
EXECUTE '
UPDATE public.bilan SET 
data_nb_releve = (SELECT count(*) FROM '||libSchema||'.releve),
data_nb_observation = (SELECT count(*) FROM '||libSchema||'.observation),
data_nb_taxon = (SELECT count(DISTINCT cd_ref) FROM '||libSchema||'.observation),
taxa_nb_taxon = (SELECT count(*) FROM '||libSchema||'.entite),
temp_data_nb_releve = (SELECT count(*) FROM '||libSchema||'.temp_releve),
temp_data_nb_observation = (SELECT count(*) FROM '||libSchema||'.temp_observation),
temp_data_nb_taxon = (SELECT count(DISTINCT cd_ref) FROM '||libSchema||'.temp_observation),
temp_taxa_nb_taxon = (SELECT count(*) FROM '||libSchema||'.temp_entite),
derniere_action = (SELECT typ_log||'' : ''||lib_log FROM '||libSchema||'.zz_log WHERE date_log = (SELECT max(date_log) FROM '||libSchema||'.zz_log) GROUP BY typ_log,lib_log,date_log LIMIT 1),
date_derniere_action = (SELECT max(date_log) FROM '||libSchema||'.zz_log)
WHERE lib_cbn = '''||libSchema||'''
	';
--- Output&Log
out.lib_log = 'bilan réalisé';
PERFORM hub_log (libSchema, out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_clear 
--- Description : Nettoyage simple des tables (partie temporaires ou propre)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_clear(libSchema varchar, jdd varchar, partie varchar = 'temp') RETURNS setof zz_log  AS 
$BODY$ 
DECLARE out zz_log%rowtype;
DECLARE prefixe varchar;
DECLARE wher varchar;
DECLARE flag integer;
BEGIN
--- Variables
CASE WHEN partie = 'temp' THEN prefixe = 'temp_'; WHEN partie = 'propre' THEN prefixe = ''; ELSE END CASE;
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN wher = 'typ_jdd = '''||jdd||''''; ELSE wher = 'cd_jdd = '''||jdd||''''; END CASE;
--- Commande
EXECUTE 'SELECT DISTINCT 1 FROM '||libSchema||'.'||prefixe||'metadonnees WHERE '||wher INTO flag;
CASE WHEN flag = 1 THEN
	EXECUTE 'SELECT * FROM hub_clear_plus('''||libSchema||''','''||libSchema||''','''||jdd||''','''||partie||''','''||partie||''');' INTO out;
	PERFORM (libSchema, out);RETURN NEXT out;
ELSE 
	--- Output&Log
	out.lib_schema := libSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_clear';out.nb_occurence := '-'; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
	out.lib_log = 'pas de jdd '||jdd;PERFORM hub_log (libSchema, out);RETURN NEXT out;
END CASE;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_clear_plus 
--- Description : Nettoyage complexe des tables (partie temporaires ou propre)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_clear_plus(fromlibSchema varchar, tolibSchema varchar, jdd varchar, fromPartie varchar = 'temp', toPartie varchar = 'temp') RETURNS setof zz_log  AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE fromprefixe varchar;
DECLARE toprefixe varchar;
DECLARE metasource varchar;
DECLARE libTable varchar;
DECLARE listJdd varchar;
DECLARE typJdd varchar;
DECLARE blabal varchar;
BEGIN
--- Variables 
--- les jeux de données de référence à surpprimer
CASE WHEN fromPartie = 'temp' THEN fromprefixe = 'temp_'; WHEN fromPartie = 'propre' THEN fromprefixe = ''; ELSE END CASE;
--- Où les supprimer (permet de supprimer des jeu de données de la partie propre depuis la partie temporaire ==> intéressant pour l'agrégation).
CASE WHEN toPartie = 'temp' THEN toprefixe = 'temp_'; WHEN toPartie = 'propre' THEN toprefixe = ''; ELSE END CASE;
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN 
	EXECUTE 'SELECT CASE WHEN string_agg(''''''''||cd_jdd||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||cd_jdd||'''''''','','') END FROM '||fromlibSchema||'.'||fromprefixe||'metadonnees WHERE typ_jdd = '''||jdd||''';' INTO listJdd;
	typJdd := jdd;
ELSE 
	listJdd := ''''||jdd||'''';
	EXECUTE 'SELECT typ_jdd FROM "'||fromlibSchema||'".'||fromprefixe||'metadonnees WHERE cd_jdd = '''||jdd||''';' INTO typJdd;
END CASE;
--- Output&Log
out.lib_schema := tolibSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_clear_plus';out.nb_occurence := '-'; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
--- Commandes
CASE WHEN listJdd <> 'vide' THEN
	FOR libTable IN EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd WHERE typ_jdd = '''||typJdd||''' OR typ_jdd = ''meta'';'
		LOOP 
		EXECUTE 'DELETE FROM '||tolibSchema||'.'||toprefixe||libTable||' WHERE cd_jdd IN ('||listJdd||');'; 
		END LOOP;
	---log---
	out.lib_log = jdd||' effacé de la partie '||toPartie;
WHEN listJdd = '''vide''' THEN out.lib_log = 'jdd vide '||jdd;
ELSE out.lib_log = 'ERREUR : mauvais typPartie : '||toPartie;
END CASE;
--- Output&Log
CASE WHEN fromlibSchema <> tolibSchema THEN
	PERFORM hub_log (fromlibSchema, out);RETURN NEXT out;
	PERFORM hub_log (tolibSchema, out);RETURN NEXT out;
ELSE
	PERFORM hub_log (fromlibSchema, out);RETURN NEXT out;
END CASE;

END; $BODY$ LANGUAGE plpgsql;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_connect 
--- Description :  Copie du Hub vers un serveur distant
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_connect(hote varchar, port varchar,dbname varchar,utilisateur varchar,mdp varchar, jdd varchar, libSchema_from varchar, libSchema_to varchar, limite integer = 0) RETURNS setof zz_log  AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE connction varchar;
DECLARE libTable varchar;
DECLARE list_champ varchar;
DECLARE listJdd varchar;
DECLARE la_limite varchar;
DECLARE typJdd varchar;
DECLARE isvid varchar;
DECLARE connecte_list varchar[];
DECLARE connecte varchar;
DECLARE cmd varchar;
BEGIN
--- Variables
connction = 'hostaddr='||hote||' port='||port||' dbname='||dbname||' user='||utilisateur||' password='||mdp||'';
out.lib_schema := libSchema_to;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_connect';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
CASE WHEN limite = 0 THEN la_limite = ''; ELSE la_limite = ' LIMIT '||limite; END CASE;

--- Vérification qu'aucune connexion n'est déjà ouverte.
SELECT dblink_get_connections() INTO connecte_list;
CASE WHEN connecte_list IS NOT NULL THEN
	FOREACH connecte IN ARRAY connecte_list LOOP 
		CASE WHEN connecte = 'link' THEN PERFORM dblink_disconnect('link'); ELSE END CASE;
	END LOOP;
ELSE END CASE;
EXECUTE 'SELECT * FROM dblink_connect_u(''link'','''||connction||''');';

CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN    
   cmd = 'SELECT CASE WHEN string_agg(''''''''''''''''''''''''||cd_jdd||'''''''''''''''''''''''','''','''') IS NULL THEN ''''vide'''' ELSE string_agg(''''''''''''''''''''''''||cd_jdd||'''''''''''''''''''''''','''','''') END FROM "'||libSchema_from||'".metadonnees WHERE typ_jdd = '''''||jdd||'''''';
   EXECUTE 'SELECT * FROM dblink_send_query(''link'','''||cmd||''')';
   EXECUTE 'SELECT * FROM dblink_get_result(''link'') as t1(listJdd varchar);' INTO listJdd;
   PERFORM dblink_disconnect('link');
   typJdd = jdd;
ELSE 
   listJdd := ''''''||jdd||'''''';
   EXECUTE 'SELECT * FROM dblink_send_query(''link'',''SELECT CASE WHEN typ_jdd IS NULL THEN ''''vide'''' ELSE typ_jdd END FROM "'||libSchema_from||'".metadonnees WHERE cd_jdd = '''''||jdd||''''''');';
   EXECUTE 'SELECT * FROM dblink_get_result(''link'') as t1(typJdd varchar);' INTO typJdd;
   PERFORM dblink_disconnect('link');
END CASE;

--- Commande
FOR libTable IN EXECUTE 'SELECT cd_table FROM ref.fsd WHERE typ_jdd = '''||typJdd||''' OR typ_jdd = ''meta'' GROUP BY cd_table' 
	LOOP
	EXECUTE 'SELECT * FROM dblink_connect_u(''link'','''||connction||''');';
	EXECUTE 'SELECT string_agg(one.cd_champ||'' ''||one.format,'','') FROM (SELECT cd_champ, format FROM ref.fsd WHERE (typ_jdd = '''||typjdd||''' OR typ_jdd = ''meta'') AND cd_table = '''||libTable||''' ORDER BY ordre_champ) as one;' INTO list_champ;
	EXECUTE 'SELECT * from dblink_send_query(''link'',''SELECT * FROM '||libSchema_from||'.'||libTable||' WHERE cd_jdd IN ('||listJdd||') '||la_limite||' '');';
	EXECUTE 'INSERT INTO '||libSchema_to||'.temp_'||libTable||' SELECT * FROM dblink_get_result(''link'') as t1 ('||list_champ||');';
	PERFORM dblink_disconnect('link');
END LOOP;

--- Output&Log
out.lib_log := jdd||' importé';
out.lib_schema := libSchema_to;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_connect';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
PERFORM hub_log (libSchema_to, out);RETURN next out;

END;$BODY$ LANGUAGE plpgsql;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_connect_simple  
--- Description :  Copie du Hub vers un serveur distant
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_connect_simple(jdd varchar, libSchema_from varchar, libSchema_to varchar, conect varchar  = 'dbname=si_flore_national port=5433') RETURNS setof zz_log  AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE connction varchar;
DECLARE libTable varchar;
DECLARE list_champ varchar;
DECLARE listJdd varchar;
DECLARE typJdd varchar;
DECLARE isvid varchar;
DECLARE connecte_list varchar[];
DECLARE connecte varchar;
DECLARE cmd varchar;
BEGIN
--- Variables
connction = conect;
out.lib_schema := libSchema_to;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_connect_simple';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;

--- Vérification qu'aucune connexion n'est déjà ouverte.
SELECT dblink_get_connections() INTO connecte_list;
CASE WHEN connecte_list IS NOT NULL THEN
	FOREACH connecte IN ARRAY connecte_list LOOP 
		CASE WHEN connecte = 'link' THEN PERFORM dblink_disconnect('link'); ELSE END CASE;
	END LOOP;
ELSE END CASE;
EXECUTE 'SELECT * FROM dblink_connect_u(''link'','''||connction||''');';

CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN    
   cmd = 'SELECT CASE WHEN string_agg(''''''''''''''''''''''''||cd_jdd||'''''''''''''''''''''''','''','''') IS NULL THEN ''''vide'''' ELSE string_agg(''''''''''''''''''''''''||cd_jdd||'''''''''''''''''''''''','''','''') END FROM "'||libSchema_from||'".metadonnees WHERE typ_jdd = '''''||jdd||'''''';
   EXECUTE 'SELECT * FROM dblink_send_query(''link'','''||cmd||''')';
   EXECUTE 'SELECT * FROM dblink_get_result(''link'') as t1(listJdd varchar);' INTO listJdd;
   PERFORM dblink_disconnect('link');
   typJdd = jdd;
ELSE 
   listJdd := ''''''||jdd||'''''';
   EXECUTE 'SELECT * FROM dblink_send_query(''link'',''SELECT CASE WHEN typ_jdd IS NULL THEN ''''vide'''' ELSE typ_jdd END FROM "'||libSchema_from||'".metadonnees WHERE cd_jdd = '''''||jdd||''''''');';
   EXECUTE 'SELECT * FROM dblink_get_result(''link'') as t1(typJdd varchar);' INTO typJdd;
   PERFORM dblink_disconnect('link');
END CASE;

--- Commande
FOR libTable IN EXECUTE 'SELECT cd_table FROM ref.fsd WHERE typ_jdd = '''||typJdd||''' OR typ_jdd = ''meta'' GROUP BY cd_table' 
	LOOP
	EXECUTE 'SELECT * FROM dblink_connect_u(''link'','''||connction||''');';
	EXECUTE 'SELECT string_agg(one.cd_champ||'' ''||one.format,'','') FROM (SELECT cd_champ, format FROM ref.fsd WHERE (typ_jdd = '''||typjdd||''' OR typ_jdd = ''meta'') AND cd_table = '''||libTable||''' ORDER BY ordre_champ) as one;' INTO list_champ;
	EXECUTE 'SELECT * from dblink_send_query(''link'',''SELECT * FROM '||libSchema_from||'.'||libTable||' WHERE cd_jdd IN ('||listJdd||')'');';
	EXECUTE 'INSERT INTO '||libSchema_to||'.'||libTable||' SELECT * FROM dblink_get_result(''link'') as t1 ('||list_champ||');';
	PERFORM dblink_disconnect('link');
END LOOP;

--- Output&Log
out.lib_log := jdd||' importé';
out.lib_schema := libSchema_to;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_connect';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
PERFORM hub_log (libSchema_to, out);RETURN next out;

END;$BODY$ LANGUAGE plpgsql;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_connect_ref
--- Description :  Mise à jour du référentiel FSD depuis un serveur distant
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_connect_ref(hote varchar, port varchar,dbname varchar,utilisateur varchar,mdp varchar,refPartie varchar = 'fsd') RETURNS setof zz_log  AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE connction varchar;
DECLARE flag integer;
DECLARE libTable varchar;
DECLARE structure varchar;
DECLARE les_champs varchar;
DECLARE bdlink_structure varchar;
BEGIN
--- Variables
connction = 'hostaddr='||hote||' port='||port||' dbname='||dbname||' user='||utilisateur||' password='||mdp||'';
--- Log
out.lib_schema := '-';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_admin_ref';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;


--- Case all
CASE WHEN refPartie = 'all' THEN DROP SCHEMA IF EXISTS ref CASCADE; ELSE END CASE;

-- Création du schema ref
SELECT DISTINCT 1 INTO flag FROM pg_tables WHERE schemaname = 'ref';
CASE WHEN flag IS NULL THEN CREATE SCHEMA ref; ELSE END CASE;

-- Création et mise à jour de la meta-table référentiel
DROP TABLE IF EXISTS ref.aa_meta;
CREATE TABLE ref.aa_meta(id serial NOT NULL, nom_ref varchar, typ varchar, ordre integer, libelle varchar, format varchar, CONSTRAINT aa_meta_pk PRIMARY KEY(id));
EXECUTE 'INSERT INTO ref.aa_meta (id, nom_ref, typ, ordre, libelle, format) SELECT * FROM dblink('''||connction||''', ''SELECT * FROM ref.aa_meta'') as t1 (id integer, nom_ref character varying, typ character varying, ordre integer, libelle character varying, format character varying)';

--- Les référentiels
FOR libTable IN EXECUTE 'SELECT nom_ref FROM ref.aa_meta GROUP BY nom_ref ORDER BY nom_ref'
	LOOP
	EXECUTE 'SELECT ''(''||champs||'',''||contrainte||'')''
		FROM (SELECT nom_ref, string_agg(one.champs,'','') as champs FROM (SELECT nom_ref, libelle||'' ''||format as champs FROM ref.aa_meta WHERE nom_ref = '''||libTable||''' AND typ = ''champ''  ORDER BY ordre ) as one GROUP BY nom_ref) as one
		JOIN (SELECT nom_ref, ''CONSTRAINT ''||nom_ref||''_pk PRIMARY KEY (''||libelle||'')'' as contrainte FROM ref.aa_meta WHERE nom_ref = '''||libTable||''' AND typ = ''cle_primaire'' ORDER BY ordre) as two ON one.nom_ref = two.nom_ref
		' INTO structure;
	EXECUTE 'SELECT string_agg(libelle,'','') as champs 
		FROM (SELECT libelle FROM ref.aa_meta WHERE nom_ref = '''||libTable||''' AND typ = ''champ'' ORDER BY ordre) as one
		' INTO les_champs;
	EXECUTE 'SELECT string_agg(libelle||'' ''||format,'','') as champs 
		FROM (SELECT nom_ref, libelle,CASE WHEN format = ''serial NOT NULL'' OR format = ''serial'' THEN ''integer'' ELSE format END as format
		FROM ref.aa_meta WHERE nom_ref = '''||libTable||''' AND typ = ''champ'' ORDER BY ordre)AS one GROUP BY nom_ref
		' INTO bdlink_structure;

	CASE WHEN refPartie = 'all' OR refPartie = libTable THEN
		EXECUTE 'DROP TABLE IF EXISTS ref.'||libTable||';';
		EXECUTE 'CREATE TABLE ref.'||libTable||' '||structure||';';	
		EXECUTE 'INSERT INTO ref.'||libTable||' ('||les_champs||')SELECT * FROM dblink('''||connction||''', ''SELECT * FROM ref.'||libTable||''') as t1 ('||bdlink_structure||')';
		
		--- Index geo
		CASE WHEN substr(libTable,1,3) = 'geo' THEN EXECUTE 'CREATE INDEX '||libTable||'_gist ON ref.'||libTable||' USING GIST (geom);'; ELSE END CASE;

		out.lib_log := libTable||' : données importées';RETURN next out;
	ELSE END CASE;
END LOOP;

-- Mise à jour des séquences
PERFORM hub_reset_sequence();

--- Output&Log
out.lib_log := 'ref mis à jour';out.lib_schema := 'ref';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_connect_ref';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log ('public', out);RETURN next out;
END;$BODY$ LANGUAGE plpgsql;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_connect_ref_simple
--- Description :  Mise à jour du référentiel FSD depuis le hub
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_connect_ref_simple(refPartie varchar = 'fsd',connction varchar = 'dbname=si_flore_national port=5433') RETURNS setof zz_log  AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE connction varchar;
DECLARE flag integer;
DECLARE libTable varchar;
DECLARE structure varchar;
DECLARE bdlink_structure varchar;
BEGIN
--- Variables

--- Log
out.lib_schema := '-';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_admin_ref';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;


--- Case all
CASE WHEN refPartie = 'all' THEN DROP SCHEMA IF EXISTS ref CASCADE; ELSE END CASE;

-- Création du schema ref
SELECT DISTINCT 1 INTO flag FROM pg_tables WHERE schemaname = 'ref';
CASE WHEN flag IS NULL THEN CREATE SCHEMA ref; ELSE END CASE;
-- Création et mise à jour de la meta-table référentiel
SELECT DISTINCT 1 INTO flag FROM pg_tables WHERE schemaname = 'ref' AND tablename = 'aa_meta';
CASE WHEN flag IS NULL THEN CREATE TABLE ref.aa_meta(id serial NOT NULL, nom_ref varchar, typ varchar, ordre integer, libelle varchar, format varchar, CONSTRAINT aa_meta_pk PRIMARY KEY(id)); ELSE END CASE;

TRUNCATE ref.aa_meta;
EXECUTE 'INSERT INTO ref.aa_meta SELECT * FROM dblink('''||connction||''', ''SELECT * FROM ref.aa_meta'') as t1 (id integer, nom_ref character varying, typ character varying, ordre integer, libelle character varying, format character varying)';

--- Les référentiels
FOR libTable IN EXECUTE 'SELECT nom_ref FROM ref.aa_meta GROUP BY nom_ref ORDER BY nom_ref'
	LOOP
	EXECUTE 'SELECT ''(''||champs||'',''||contrainte||'')''
		FROM (SELECT nom_ref, string_agg(libelle||'' ''||format,'','') as champs FROM ref.aa_meta WHERE nom_ref = '''||libTable||''' AND typ = ''champ'' GROUP BY nom_ref) as one
		JOIN (SELECT nom_ref, ''CONSTRAINT ''||nom_ref||''_pk PRIMARY KEY (''||libelle||'')'' as contrainte FROM ref.aa_meta WHERE nom_ref = '''||libTable||''' AND typ = ''cle_primaire'') as two ON one.nom_ref = two.nom_ref
		' INTO structure;
	EXECUTE 'SELECT string_agg(libelle||'' ''||format,'','') as champs 
		FROM (SELECT nom_ref, libelle,CASE WHEN format = ''serial NOT NULL'' OR format = ''serial'' THEN ''integer'' ELSE format END as format
		FROM ref.aa_meta WHERE nom_ref = '''||libTable||''' AND typ = ''champ'')AS one GROUP BY nom_ref
		' INTO bdlink_structure;

	CASE WHEN refPartie = 'all' OR refPartie = libTable THEN
		EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = ''ref'' AND tablename = '''||libTable||''';' INTO flag ;
		CASE WHEN flag IS NULL THEN EXECUTE 'CREATE TABLE ref.'||libTable||' '||structure||';'; out.lib_log := libTable||' créée';RETURN next out; ELSE END CASE;
		EXECUTE 'TRUNCATE ref.'||libTable||';';
		
		EXECUTE 'INSERT INTO ref.'||libTable||' SELECT * FROM dblink('''||connction||''', ''SELECT * FROM ref.'||libTable||''') as t1 ('||bdlink_structure||')';

		--- Index geo
		CASE WHEN substr(libTable,1,3) = 'geo' THEN EXECUTE 'CREATE INDEX '||libTable||'_gist ON ref.'||libTable||' USING GIST (geom);'; ELSE END CASE;

		out.lib_log := libTable||' : données importées';RETURN next out;
	ELSE END CASE;
END LOOP;

--- Output&Log
out.lib_log := 'ref mis à jour';out.lib_schema := 'ref';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_connect_ref_simple';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log ('public', out);RETURN next out;
END;$BODY$ LANGUAGE plpgsql;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_export 
--- Description : Exporter les données depuis un hub
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_export(libSchema varchar,jdd varchar,path varchar,format varchar = 'fcbn',source varchar = '') RETURNS setof zz_log  AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE libTable varchar;
DECLARE typJdd varchar; 
DECLARE listJdd varchar; 
BEGIN
--- Variables Jdd
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN 	
	typJdd := 'WHERE typ_jdd = '''||jdd||''' OR typ_jdd = ''meta''';
	EXECUTE 'SELECT CASE WHEN string_agg(''''''''||cd_jdd||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||cd_jdd||'''''''','','') END FROM "'||libSchema||'"."temp_metadonnees" WHERE typ_jdd = '''||jdd||''';' INTO listJdd;
	listJdd := 'WHERE cd_jdd IN ('||listJdd||')';
WHEN jdd = 'all' THEN 
	typJdd := ''; listJdd = '';
WHEN jdd = 'list_taxon' THEN 
	format = 'list_taxon';
ELSE
	EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'".temp_metadonnees WHERE cd_jdd = '''||jdd||''';' INTO typJdd;
	typJdd := 'WHERE typ_jdd = '''||typJdd||''' OR typ_jdd = ''meta''';
	listJdd := 'WHERE cd_jdd IN ('''||jdd||''')';
END CASE;
CASE WHEN source = 'temp' THEN source = 'temp_'; ELSE END CASE;
--- Output&Log
out.lib_schema := libSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_export';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
--- Commandes
CASE WHEN format = 'fcbn' THEN
	FOR libTable in EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd '||typJdd||''
		LOOP EXECUTE 'COPY (SELECT * FROM  '||libSchema||'.'||source||libTable||' '||listJdd||') TO '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';'; END LOOP;
	out.lib_log :=  'Tous les jdd ont été exporté au format '||format;
WHEN format = 'sinp' THEN
	--- TABLE SujetObservation
	EXECUTE 'COPY (SELECT
		a.cd_jdd||''-''||cd_obs_mere 	as "cleObs",
		a.cd_jdd||''-''||a.cd_releve	as "cleGrp",
		cd_obs_perm		as "identifiantPermanent",
		''Présent'' 		as "statutObservation",
		nom_ent_mere 		as "nomCite",
		null 			as "objetGeo",
		date_debut 		as "dateDebut",
		date_fin		as "dateFin",
		null 			as "altitudeMin",
		null 			as "altitudeMax",
		null 			as "habitat",
		null 			as "profondeurMin",
		null			as "profondeurMax",
		denombt_min		as "denbrMin",
		denombt_max		as "denbrMax",
		objet_denombt		as "objDenbr",
		null			as "denombrement",
		observateur.nom		as "identiteObs",
		null			as "mailObs",
		organisme_obs.nom	as "organismeObs",
		null			as "profondeurMoyenne",
		cd_nom			as "cdNom",
		cd_ref			as "cdRef",
		version_reftaxo		as "versionTAXREF",
		determinateur.nom 	as "identiteDet",
		null			as "mailDet",
		organisme_det.nom	as "organismeDet",
		null 			as dateDetermination,
		validateur.nom		as "IdentiteVal",
		null			as "mailVal",
		organisme_val.nom	as "organismeVal",
		organisme_sat.nom	as "organismeStandard",
		a.rmq 			as "commentaire"
		FROM '||libSchema||'.observation a
		JOIN '||libSchema||'.releve z ON a.cd_releve = z.cd_releve AND a.cd_jdd = z.cd_jdd
		LEFT JOIN (SELECT cd_jdd||''-''||cd_releve as id, string_agg(nom_acteur,'', '') as "nom" FROM '||libSchema||'.releve_acteur WHERE typ_acteur = ''obs'' GROUP BY cd_jdd,cd_releve) as observateur ON observateur.id = a.cd_jdd||''-''||a.cd_releve
		LEFT JOIN (SELECT cd_jdd||''-''||cd_releve as id, string_agg(lib_orgm,'', '') as "nom" FROM '||libSchema||'.releve_acteur WHERE typ_acteur = ''obs'' GROUP BY cd_jdd,cd_releve) as organisme_obs ON organisme_obs.id = a.cd_jdd||''-''||a.cd_releve
		LEFT JOIN (SELECT cd_jdd||''-''||cd_releve as id, string_agg(nom_acteur,'', '') as "nom" FROM '||libSchema||'.releve_acteur WHERE typ_acteur = ''det'' GROUP BY cd_jdd,cd_releve) as determinateur ON determinateur.id = a.cd_jdd||''-''||a.cd_releve
		LEFT JOIN (SELECT cd_jdd||''-''||cd_releve as id, string_agg(lib_orgm,'', '') as "nom" FROM '||libSchema||'.releve_acteur WHERE typ_acteur = ''det'' GROUP BY cd_jdd,cd_releve) as organisme_det ON organisme_det.id = a.cd_jdd||''-''||a.cd_releve
		LEFT JOIN (SELECT cd_jdd||''-''||cd_releve as id, string_agg(nom_acteur,'', '') as "nom" FROM '||libSchema||'.releve_acteur WHERE typ_acteur = ''val'' GROUP BY cd_jdd,cd_releve) as validateur ON validateur.id = a.cd_jdd||''-''||a.cd_releve
		LEFT JOIN (SELECT cd_jdd||''-''||cd_releve as id, string_agg(lib_orgm,'', '') as "nom" FROM '||libSchema||'.releve_acteur WHERE typ_acteur = ''val'' GROUP BY cd_jdd,cd_releve) as organisme_val ON organisme_val.id = a.cd_jdd||''-''||a.cd_releve
		LEFT JOIN (SELECT cd_jdd as id, string_agg(DISTINCT lib_orgm,'', '') as "nom" FROM '||libSchema||'.releve_acteur WHERE typ_acteur = ''sta'' GROUP BY cd_jdd) as organisme_sat ON organisme_sat.id = a.cd_jdd
		GROUP BY a.cd_jdd,a.cd_releve,a.cd_obs_mere,cd_obs_perm,nom_ent_mere,date_debut,date_fin,cd_nom,cd_ref,version_reftaxo,a.rmq,denombt_min,denombt_max,objet_denombt,observateur.nom,organisme_obs.nom,determinateur.nom,organisme_det.nom,validateur.nom,organisme_val.nom,organisme_sat.nom
	) TO '''||path||'/sinp_SujetObservation.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';';

	--- TABLE Source
	EXECUTE 'COPY (SELECT
		a.cd_jdd||''-''||cd_obs_mere 	as "cleObs",
		a.cd_jdd||''-''||a.cd_releve	as "cleGrp",
		cd_obs_mere 		as "identifiantOrigine",
		propriete_obs 		as "dSPublique",
		cd_sensi 		as "diffusionNiveauPrecision",
		0 			as "diffusionFloutage",
		null 			as "sensible",
		cd_sensi 		as "sensiNiveau",
		null 			as "sensiDateAttribution",
		lib_refsensi 		as "sensiReferentiel",
		version_refsensi 	as "sensiVersionReferentiel",
		typ_source 		as "statutSource",
		a.cd_jdd 		as "jddCode",
		a.cd_jdd 		as "jddId",
		cd_jdd_orig 		as "jddSourceId",
		a.cd_jdd_perm 		as "jddMetadonneeDEEId",
		lib_orgm 		as "organismeGestionnaireDonnee",
		null 			as "codeIDCNPDispositif",
		current_date 		as "dEEDateTransformation",
		current_date 		as "dEEDateDerniereModification",
		lib_biblio 		as "referenceBiblio",
		organisme_tra.nom	as "orgTransformation"
		FROM '||libSchema||'.observation a
		JOIN '||libSchema||'.metadonnees_acteur z ON a.cd_jdd = z.cd_jdd
		LEFT JOIN (SELECT cd_jdd as id, string_agg(DISTINCT lib_orgm,'', '') as "nom" FROM '||libSchema||'.releve_acteur WHERE typ_acteur = ''tra'' GROUP BY cd_jdd) as organisme_tra ON organisme_tra.id = a.cd_jdd
		GROUP BY a.cd_jdd, a.cd_releve,cd_obs_perm,cd_obs_mere,propriete_obs,cd_sensi,lib_refsensi,version_refsensi,typ_source,
		a.cd_jdd,cd_jdd_orig,a.cd_jdd_perm,lib_orgm,current_date,lib_biblio,organisme_tra.nom
	) TO '''||path||'sinp_Source.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';';

	--- TABLE Regroupement
	EXECUTE 'COPY (SELECT
		a.cd_jdd||''-''||a.cd_releve	as "cleGrp",
		a.cd_releve_perm 		as "identifiantPermanent",
		CASE WHEN meth_releve IS NOT NULL AND typ_protocole IS NOT NULL THEN ''méthode = ''||meth_rel.libelle_valeur||'' - protocole = ''||typ_prot.libelle_valeur 
			WHEN meth_releve IS NULL AND typ_protocole IS NOT NULL THEN '' protocole = ''||typ_prot.libelle_valeur 
			WHEN meth_releve IS NOT NULL AND typ_protocole IS NULL THEN ''méthode = ''||meth_rel.libelle_valeur
			ELSE ''pas de description''
			END as "methodeRegroupement",
		CASE WHEN typ_protocole = ''st'' THEN ''INVSTA''
			WHEN meth_releve = ''lp'' OR meth_releve = ''lf'' OR meth_releve = ''lv'' THEN ''REL''
			ELSE ''OBS''
			END as "methodeRegroupement"
		FROM '||libSchema||'.releve a
		LEFT JOIN ref.voca_ctrl meth_rel ON meth_rel.cd_champ = ''meth_releve'' AND a.meth_releve = meth_rel.code_valeur
		LEFT JOIN ref.voca_ctrl typ_prot ON typ_prot.cd_champ = ''typ_protocole'' AND a.typ_protocole = typ_prot.code_valeur
	) TO '''||path||'sinp_Regroupement.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';';	
	
	--- TABLE Maille10x10
	EXECUTE 'COPY (SELECT
		a.cd_jdd||''-''||cd_obs_mere 	as "cleObs",
		a.cd_jdd||''-''||a.cd_releve	as "cleGrp",
		cd_obs_perm 			as "identifiantPermanent",
		cd_geo 				as "codeMaille",
		version_refgeo 			as "versionRef",
		cd_refgeo 			as "nomRef",
		origine_geo 			as "typeInfoGeo"
		FROM '||libSchema||'.releve_territoire a
		JOIN '||libSchema||'.observation z ON a.cd_releve = z.cd_releve AND a.cd_jdd = z.cd_jdd
		WHERE typ_geo = ''m10''
	) TO '''||path||'sinp_Maille10x10.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';';

	--- TABLE Commune
	EXECUTE 'COPY (SELECT
		a.cd_jdd||''-''||cd_obs_mere 	as "cleObs",
		a.cd_jdd||''-''||a.cd_releve	as "cleGrp",
		cd_obs_perm 			as "identifiantPermanent",
		cd_geo 				as "codeMaille",
		version_refgeo 			as "versionRef",
		cd_refgeo 			as "nomRef",
		origine_geo 			as "typeInfoGeo"
		FROM '||libSchema||'.releve_territoire a
		JOIN '||libSchema||'.observation z ON a.cd_releve = z.cd_releve AND a.cd_jdd = z.cd_jdd
		WHERE typ_geo = ''com''
	) TO '''||path||'sinp_Commune.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';';
	
	out.lib_log :=  'export au format SINP';
WHEN format = 'list_taxon' THEN
	EXECUTE 'COPY (SELECT * FROM  '||libSchema||'.'||source||libTable||') TO '''||path||'std_zz_log_liste_taxon.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';';
	EXECUTE 'COPY (SELECT * FROM  '||libSchema||'.'||source||libTable||') TO '''||path||'std_zz_log_liste_taxon_et_infra.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';';
	out.lib_log :=  'Liste de taxon exporté ';
ELSE out.lib_log :=  'format ('||format||') non implémenté ou jdd ('||jdd||') mauvais';
END CASE;
PERFORM hub_log (libSchema, out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_idperm 
--- Description : Production des identifiants uniques
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_idperm(libSchema varchar, nomDomaine varchar, champ_perm varchar, jdd varchar = 'all', rempla boolean = false) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE flag integer;
DECLARE cpt1 integer;
DECLARE cpt2 integer;
DECLARE champMere varchar;
DECLARE typ_jdd varchar;
DECLARE typ_ref varchar;
DECLARE wherejdd varchar;
DECLARE jeudedonnee varchar;
DECLARE tableRef varchar;
DECLARE listTable varchar;
DECLARE listPerm varchar;
BEGIN
--- Variables
CASE WHEN jdd = 'all' THEN wherejdd = '';  WHEN jdd = 'data' OR  jdd = 'taxa' THEN wherejdd = 'WHERE typ_jdd = '''||jdd||'''';ELSE wherejdd = 'WHERE cd_jdd = '''||jdd||''''; END CASE;
CASE 	WHEN champ_perm = 'cd_jdd_perm' 	THEN champMere = 'cd_jdd';	tableRef = 'temp_metadonnees'; 	typ_ref = 'all';	flag = 1;
	WHEN champ_perm = 'cd_ent_perm' 	THEN champMere = 'cd_ent_mere';	tableRef = 'temp_entite'; 	typ_ref = 'taxa';	flag = 1;
	WHEN champ_perm = 'cd_releve_perm' 	THEN champMere = 'cd_releve';	tableRef = 'temp_releve'; 	typ_ref = 'data';	flag = 1;
	WHEN champ_perm = 'cd_obs_perm' 	THEN champMere = 'cd_obs_mere';	tableRef = 'temp_observation'; 	typ_ref = 'data';	flag = 1;
	ELSE out.lib_log := 'ERREUR : Mauvais champ_perm';out.lib_table := '-'; PERFORM hub_log (libSchema, out); RETURN next out;flag = 1;
END CASE;
cpt1 = 0;cpt2 = 0;
--- Output
out.lib_schema := libSchema;out.lib_table := tableRef;out.lib_champ := champ_perm;out.typ_log := 'hub_idperm'; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
--- Commandes
FOR jeudedonnee IN EXECUTE 'SELECT CASE WHEN cd_jdd IS NULL THEN ''vide'' ELSE cd_jdd END as cd_jdd FROM '||libSchema||'.temp_metadonnees '||wherejdd||';'
	LOOP
	EXECUTE 'SELECT CASE WHEN typ_jdd IS NULL THEN ''vide'' ELSE typ_jdd END as typ_jdd FROM '||libSchema||'.temp_metadonnees WHERE cd_jdd = '''||jeudedonnee||''';' INTO typ_jdd;
	--- On vide les champ perm pour remplacer	
	CASE WHEN rempla = TRUE THEN EXECUTE 'UPDATE '||libSchema||'.'||tableRef||' ot SET '||champ_perm||' = ''a'' WHERE cd_jdd = '''||jeudedonnee||''';'; ELSE END CASE;
	--- CASE WHEN rempla = TRUE THEN EXECUTE 'UPDATE '||libSchema||'.'||tableRef||' ot SET '||champ_perm||' = NULL WHERE cd_jdd = '''||jeudedonnee||''';'; ELSE END CASE;
	CASE WHEN flag = 1 AND (typ_jdd = typ_ref OR champ_perm = 'cd_jdd_perm')THEN 
		FOR listPerm IN EXECUTE 'SELECT DISTINCT '||champMere||' FROM '||libSchema||'.'||tableRef||' ot WHERE cd_jdd = '''||jeudedonnee||''' AND '||champ_perm||' = ''a'';' 
		--- FOR listPerm IN EXECUTE 'SELECT DISTINCT '||champMere||' FROM '||libSchema||'.'||tableRef||' ot WHERE cd_jdd = '''||jeudedonnee||''' AND '||champ_perm||' IS NULL;'
			LOOP 
			EXECUTE 'UPDATE '||libSchema||'.'||tableRef||' ot SET '||champ_perm||' = NULL WHERE '||champMere||' = '''||listPerm||''' AND cd_jdd = '''||jeudedonnee||'''';
			EXECUTE 'UPDATE '||libSchema||'.'||tableRef||' ot SET '||champ_perm||' = ('''||nomdomaine||'/'||champ_perm||'/''||(SELECT uuid_generate_v4())) WHERE '||champ_perm||' IS NULL';
			--- EXECUTE 'UPDATE '||libSchema||'.'||tableRef||' ot SET ('||champ_perm||') = ('''||nomdomaine||'/'||champ_perm||'/''||(SELECT uuid_generate_v4())) WHERE '||champMere||' = '''||listPerm||''' AND cd_jdd = '''||jeudedonnee||''';';
			cpt1 = cpt1 + 1;
		END LOOP;	
		FOR listTable in EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd WHERE cd_champ = '''||champ_perm||''''  --- Peuplement du nouvel idPermanent dans les autres tables
			LOOP 
			--- EXECUTE 'UPDATE '||libSchema||'.temp_'||listTable||' ot SET '||champ_perm||' = o.'||champ_perm||' FROM '||libSchema||'.'||tableRef||' o WHERE o.cd_jdd = ot.cd_jdd AND o.'||champMere||' = ot.'||champMere;
			cpt2 = cpt2 + 1;
		END LOOP;
	out.lib_log := 'id_permanent produit'; out.nb_occurence := cpt1||' identifiants'; PERFORM hub_log (libSchema, out); RETURN next out;
	out.lib_log := 'id_permanent propagé'; out.nb_occurence := cpt2||' tables concernées'; PERFORM hub_log (libSchema, out);RETURN next out;
	ELSE END CASE;
	END LOOP;
END; $BODY$ LANGUAGE plpgsql;
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_import 
--- Description : Importer des données (fichiers CSV) dans un hub
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_import(libSchema varchar, jdd varchar, path varchar, rempla boolean = false, delimitr varchar = 'point_virgule', files varchar = '', champ varchar = '') RETURNS setof zz_log AS 
$BODY$
DECLARE typJdd varchar;
DECLARE listJdd varchar;
DECLARE libTable varchar;
--DECLARE delim_import varchar;
DECLARE out zz_log%rowtype;
BEGIN
--- Variable
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN 	
	typJdd := jdd;
	EXECUTE 'SELECT CASE WHEN string_agg(''''''''||cd_jdd||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||cd_jdd||'''''''','','') END FROM "'||libSchema||'"."temp_metadonnees" WHERE typ_jdd = '''||jdd||''';' INTO listJdd;
ELSE
	EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'".temp_metadonnees WHERE cd_jdd = '''||jdd||''';' INTO typJdd; 
	listJdd := ''''||jdd||'''';
END CASE;
--- delimitr
CASE WHEN delimitr = 'virgule' THEN delimitr = ''','''; WHEN delimitr = 'tab' THEN delimitr = 'E''\t'''; WHEN delimitr = 'point_virgule' THEN delimitr = ''';'''; ELSE delimitr = ''';'''; END CASE;
--- Commande
--- Cas du chargement de tous les jeux de données
CASE WHEN files <> '' THEN
	CASE WHEN rempla = TRUE THEN EXECUTE 'DELETE FROM "'||libSchema||'".temp_'||files||' WHERE cd_jdd IN ('||listJdd||');'; ELSE PERFORM 1; END CASE;
	EXECUTE 'COPY "'||libSchema||'".temp_'||files||' ('||champ||') FROM '''||path||'std_'||files||'.csv'' HEADER CSV DELIMITER '||delimitr||' ENCODING ''UTF8'';';
	CASE WHEN rempla = TRUE THEN out.lib_log := 'fichier std_'||files||'.csv remplacé'; ELSE out.lib_log := 'fichier std_'||files||'.csv ajouté'; END CASE;
WHEN jdd = 'all' THEN 
	FOR libTable in EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd;'
	LOOP 
		CASE WHEN rempla = TRUE THEN EXECUTE 'TRUNCATE "'||libSchema||'".temp_'||libTable||';';out.lib_log := ' Tous les fichiers ont été remplacé '; ELSE out.lib_log := ' Tous les fichiers ont été importé '; END CASE;
		EXECUTE 'COPY "'||libSchema||'".temp_'||libTable||' FROM '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '||delimitr||' ENCODING ''UTF8'';'; 
	END LOOP;
--- Cas du chargement global (tous les fichiers)
WHEN files = '' THEN
	FOR libTable in EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd WHERE typ_jdd = '''||typJdd||''' OR typ_jdd = ''meta'';'
	LOOP 
		CASE WHEN rempla = TRUE THEN EXECUTE 'DELETE FROM "'||libSchema||'".temp_'||libTable||' WHERE cd_jdd IN ('||listJdd||');'; ELSE PERFORM 1; END CASE;
		EXECUTE 'COPY "'||libSchema||'".temp_'||libTable||' FROM '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '||delimitr||' ENCODING ''UTF8'';'; 
	END LOOP;
	CASE WHEN rempla = TRUE THEN out.lib_log := 'jdd '||jdd||' remplacé'; ELSE out.lib_log := 'jdd '||jdd||' ajouté';END CASE;
--- Cas du chargement spécifique (un seul fichier)
ELSE
END CASE;
--- WHEN jdd <> 'data' AND jdd <> 'taxa' AND files <> '' THEN 
--- 	CASE WHEN rempla = TRUE THEN EXECUTE 'DELETE FROM "'||libSchema||'".temp_'||files||' WHERE cd_jdd IN ('||listJdd||');'; out.lib_log := 'Fichier remplacé'; ELSE out.lib_log := 'Fichier ajouté'; END CASE;
--- 	EXECUTE 'COPY "'||libSchema||'".temp_'||files||' FROM '''||path||'std_'||files||'.csv'' HEADER CSV DELIMITER '||delimitr||' ENCODING ''UTF8'';';
--- ELSE out.lib_log := 'Problème identifié (Soit le jdd soit le fichier)'; END CASE;

--- Output&Log
out.lib_schema := libSchema;out.lib_champ := '-';out.lib_table := '-';out.typ_log := 'hub_import';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log (libSchema, out);RETURN next out;
END; $BODY$  LANGUAGE plpgsql;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_import_taxon 
--- Description : Importer une liste de taxon dans un hub
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_import_taxon(libSchema varchar, path varchar, files varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
BEGIN
--- Commande
CASE WHEN files <> '' THEN 
	EXECUTE 'TRUNCATE TABLE "'||libSchema||'".zz_log_liste_taxon; TRUNCATE TABLE "'||libSchema||'".zz_log_liste_taxon_et_infra;';
	EXECUTE 'COPY "'||libSchema||'".zz_log_liste_taxon FROM '''||path||files||''' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';';
	out.lib_log := files||' importé depuis '||path;
ELSE out.lib_log := 'Paramètre "files" incorrect'; END CASE;

--- Output&Log
out.lib_schema := libSchema;out.lib_champ := '-';out.lib_table := 'zz_log_liste_taxon';out.typ_log := 'hub_import_taxon';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log (libSchema, out);RETURN next out;
END; $BODY$  LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_txinfra 
--- Description : Générer une table avec les taxon infra depuis la table zz_log_liste_taxon
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_txinfra(libSchema varchar, version_taxref integer = 7) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE i varchar;
BEGIN
--- Commande
FOR i in EXECUTE 'select cd_ref from "'||libSchema||'".zz_log_liste_taxon' 
	LOOP  
	EXECUTE
		'INSERT INTO "'||libSchema||'".zz_log_liste_taxon_et_infra (cd_ref_demande, nom_valide_demande, cd_ref_cite, nom_complet_cite,cd_taxsup_cite,rang_cite)
		select '''||i||''' as cd_ref_demande, '''' as nom_valide_demande, foo.* from 
		(WITH RECURSIVE hierarchie(cd_nom,nom_complet, cd_taxsup, rang) AS (
		SELECT cd_nom, nom_complet, cd_taxsup, rang
		FROM ref.taxref_v'||version_taxref||' t1
		WHERE t1.cd_nom = '''||i||''' AND t1.cd_nom = t1.cd_ref
		UNION
		SELECT t2.cd_nom, t2.nom_complet, t2.cd_taxsup, t2.rang
		FROM ref.taxref_v'||version_taxref||' t2
		JOIN hierarchie h ON t2.cd_taxsup = h.cd_nom
		WHERE t2.cd_nom = t2.cd_ref
		) SELECT * FROM hierarchie) as foo';
	end loop;
EXECUTE 'update  "'||libSchema||'".zz_log_liste_taxon_et_infra set nom_valide_demande = nom_valide from "'||libSchema||'".zz_log_liste_taxon where zz_log_liste_taxon_et_infra.cd_ref_demande= zz_log_liste_taxon.cd_ref ' ;
out.lib_log := 'Liste de sous taxons générée';

--- Output&Log
out.lib_schema := libSchema;out.lib_champ := '-';out.lib_table := 'zz_log_liste_taxon_et_infra';out.typ_log := 'hub_txinfra';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log (libSchema, out);RETURN next out;
END; $BODY$  LANGUAGE plpgsql;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_pull 
--- Description : Récupération d'un jeu de données depuis la partie propre vers la partie temporaire
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_pull(libSchema varchar,jdd varchar, mode integer = 1) RETURNS setof zz_log AS 
$BODY$ 
DECLARE out zz_log%rowtype; 
DECLARE flag integer; 
DECLARE ct integer; 
DECLARE ct2 integer; 
DECLARE typJdd varchar; 
DECLARE libTable varchar; 
DECLARE schemaSource varchar; 
DECLARE schemaDest varchar; 
DECLARE tableSource varchar; 
DECLARE tableDest varchar; 
DECLARE champRef varchar; 
DECLARE tableRef varchar; 
DECLARE nothing varchar; 

BEGIN
--- Variables Jdd
CASE WHEN jdd = 'data' THEN champRef = 'cd_obs_perm'; tableRef = 'table_name LIKE ''observation%'' OR table_name LIKE ''releve%'''; flag := 1;
	WHEN jdd = 'taxa' THEN champRef = 'cd_ent_perm';	tableRef = 'table_name LIKE ''entite%'''; flag := 1;
	ELSE EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'".metadonnees WHERE cd_jdd = '''||jdd||''';' INTO typJdd;
		CASE WHEN typJdd = 'data' THEN champRef = 'cd_obs_perm'; tableRef = 'table_name LIKE ''observation%'' OR table_name LIKE ''releve%'''; flag := 1;
			WHEN typJdd = 'taxa' THEN champRef = 'cd_ent_perm';	tableRef = 'table_name LIKE ''entite%'''; flag := 1;
			ELSE flag := 0;
		END CASE;
	END CASE;
--- mode 1 = intra Shema / mode 2 = entre schema et agregation / mode 3 = entre agregation et schema
CASE 	WHEN mode = 1 THEN schemaSource :=libSchema; schemaDest :=libSchema; 
	WHEN mode = 2 THEN schemaSource :=libSchema; schemaDest :='agregation'; 
	WHEN mode = 3 THEN schemaSource :='agregation'; schemaDest :=libSchema; 
	ELSE flag :=0; END CASE;

--- Commandes
--- Remplacement total (NB : equivalent au push 'replace' mais dans l'autre sens)
CASE WHEN flag = 1 THEN
	SELECT * INTO out FROM hub_clear(libSchema, jdd, 'temp'); return next out;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE ''metadonnees%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP
		ct = ct+1;
		CASE	WHEN mode = 1 THEN tableSource := libTable; tableDest := 'temp_'||libTable; 
			WHEN mode = 2 THEN tableSource := 'temp_'||libTable; tableDest := libTable; 
			WHEN mode = 3 THEN tableSource := 'temp_'||libTable; tableDest := libTable; 
			END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest , jdd, 'push_total'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE ct2 = ct2+1; END CASE;
	END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE '||tableRef||' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		ct = ct+1;
		CASE 	WHEN mode = 1 THEN tableSource := libTable; tableDest := 'temp_'||libTable; 
			WHEN mode = 2 THEN tableSource := 'temp_'||libTable; tableDest := libTable; 
			WHEN mode = 3 THEN tableSource := 'temp_'||libTable; tableDest := libTable; 
			END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest , jdd, 'push_total'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE ct2 = ct2+1; END CASE;
	END LOOP;
ELSE ---Log
	out.lib_schema := libSchema; out.lib_champ := '-'; out.typ_log := 'hub_pull';SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user; out.lib_log := 'ERREUR : sur champ jdd = '||jdd; PERFORM hub_log (libSchema, out);RETURN NEXT out;
END CASE;

---Log final
out.typ_log := 'hub_pull'; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user; out.lib_table := '-'; out.lib_champ := '-';
CASE 
WHEN (ct = ct2) THEN out.lib_log := 'Partie propre vide - jdd = '||jdd; out.nb_occurence := '-'; 
WHEN (ct <> ct2) THEN out.lib_log := 'Données tirées - jdd = '||jdd; out.nb_occurence := '-';
ELSE SELECT 1 into nothing; END CASE;
PERFORM hub_log (libSchema, out);RETURN NEXT out; 


END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_push 
--- Description : Mise à jour des données (on pousse les données)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_push(libSchema varchar,jdd varchar, typAction varchar = 'replace', mode integer = 1) RETURNS setof zz_log AS 
$BODY$ 
DECLARE out zz_log%rowtype; 
DECLARE flag integer; 
DECLARE ct integer; 
DECLARE ct2 integer; 
DECLARE typJdd varchar; 
DECLARE libTable varchar; 
DECLARE schemaSource varchar; 
DECLARE schemaDest varchar; 
DECLARE tableSource varchar; 
DECLARE tableDest varchar; 
DECLARE nothing varchar; 

BEGIN
--- Variables Jdd
ct=0;ct2=0;
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN typJdd = jdd;
ELSE 
	CASE WHEN mode = 1 THEN EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'".temp_metadonnees WHERE cd_jdd = '''||jdd||''';' INTO typJdd;
	WHEN mode = 2 THEN EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'".metadonnees WHERE cd_jdd = '''||jdd||''';' INTO typJdd; 
	ELSE flag :=0; END CASE;	
END CASE;
--- mode 1 = intra Shema / mode 2 = entre shema et agregation
CASE WHEN mode = 1 THEN schemaSource :=libSchema; schemaDest :=libSchema; WHEN mode = 2 THEN schemaSource :=libSchema; schemaDest :='agregation'; ELSE flag :=0; END CASE;

--- Commandes
--- Remplacement total = hub_clear + hub_add
CASE WHEN typAction = 'replace' THEN
	CASE WHEN mode = 1 THEN SELECT * INTO out FROM hub_clear_plus(schemaSource,schemaDest, jdd, 'temp', 'propre'); WHEN mode = 2 THEN SELECT * INTO out FROM hub_clear_plus(schemaSource,schemaDest, jdd, 'propre', 'temp'); ELSE END CASE; return next out;
	FOR libTable IN EXECUTE 'SELECT cd_table FROM ref.fsd WHERE typ_jdd = '''||typjdd||''' OR typ_jdd = ''meta'' GROUP BY cd_table' LOOP
		ct = ct+1;
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest , jdd, 'push_total'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; PERFORM hub_log (libSchema, out); ELSE ct2 = ct2+1; END CASE;
	END LOOP;

--- Ajout de la différence = hub_add + hub_update sur meta et data/taxa
WHEN typAction = 'add' THEN
	FOR libTable IN EXECUTE 'SELECT cd_table FROM ref.fsd WHERE typ_jdd = '''||typjdd||''' OR typ_jdd = ''meta'' GROUP BY cd_table' LOOP
		ct = ct+1;
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest , jdd, 'push_diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; PERFORM hub_log (libSchema, out); ELSE ct2 = ct2+1; END CASE;
		SELECT * INTO out FROM hub_update(schemaSource,schemaDest, tableSource, tableDest , jdd, 'push_diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; PERFORM hub_log (libSchema, out); ELSE ct2 = ct2+1; END CASE;
	END LOOP;
	ct2 = ct2/2;
--- Suppression de l'existant depuis la partie temporaire = hub_del
WHEN typAction = 'del' THEN
	FOR libTable IN EXECUTE 'SELECT cd_table FROM ref.fsd WHERE typ_jdd = '''||typjdd||''' OR typ_jdd = ''meta'' GROUP BY cd_table' LOOP
		ct = ct+1;
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_del(schemaSource,schemaDest, tableSource, tableDest , jdd, 'push_diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; PERFORM hub_log (libSchema, out); ELSE ct2 = ct2+1; END CASE;
	END LOOP;
ELSE ---Log
	out.lib_schema := libSchema; out.lib_champ := '-'; out.typ_log := 'hub_push';SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user; out.lib_log := 'ERREUR : sur champ action = '||jdd; PERFORM hub_log (libSchema, out);RETURN NEXT out;
END CASE;

---Log final
out.typ_log := 'hub_push'; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user; out.lib_table := '-'; out.lib_champ := '-';
CASE 
WHEN (ct = ct2) AND typAction = 'replace' THEN out.lib_log := 'Partie temporaire vide - jdd = '||jdd; out.nb_occurence := jdd; 
WHEN (ct <> ct2) AND typAction = 'replace' THEN out.lib_log := 'Données poussées - jdd = '||jdd; out.nb_occurence := jdd;
WHEN (ct = ct2) AND typAction = 'add' THEN out.lib_log := 'Aucune modification à apporter à la partie propre - jdd = '||jdd; out.nb_occurence := jdd; 
WHEN (ct <> ct2) AND typAction = 'add' THEN out.lib_log := 'Modification apportées à la partie propre - jdd = '||jdd; out.nb_occurence := jdd;
WHEN (ct = ct2) AND typAction = 'del' THEN out.lib_log := 'Aucun point commun entre les partie propre et temporaire - jdd = '||jdd; out.nb_occurence := jdd; 
WHEN (ct <> ct2) AND typAction = 'del' THEN out.lib_log := 'Partie temporaire nettoyée - jdd = '||jdd; out.nb_occurence := jdd;
ELSE SELECT 1 into nothing; END CASE;
PERFORM hub_log (libSchema, out);RETURN NEXT out; 

END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_truncate
--- Description : Truncate le hub
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_truncate(libSchema varchar,partie varchar = 'temp') RETURNS setof zz_log  AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE prefixe varchar;
DECLARE libTable varchar;
BEGIN
--- Variables 
CASE WHEN partie = 'temp' THEN prefixe = 'temp_'; WHEN partie = 'propre' THEN prefixe = ''; ELSE prefixe = 'temp_'; END CASE;
--- Commandes
FOR libTable IN EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd;' LOOP 
	EXECUTE 'TRUNCATE '||libSchema||'.'||prefixe||libTable||';'; 
END LOOP;
--- Output&Log
out.lib_schema := libSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_truncate';out.nb_occurence := '-'; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;out.lib_log = 'prefixe = '||prefixe;PERFORM hub_log (libSchema, out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_stat
--- Description : Produit des stats bilan sur les données d'un schema
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_stat(libSchema varchar = 'agregation', typ varchar = 'all', chemin varchar = '/home/hub/agregation/stat/') RETURNS setof zz_log  AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE cmd varchar;
DECLARE prefixe varchar;
DECLARE sstyp varchar;
BEGIN
--- Variables 
out.lib_schema := libSchema;out.lib_table := '-'; out.lib_champ := '-'; out.typ_log := 'hub_stat';SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;out.nb_occurence := '-';
out.lib_log := '';
prefixe := 'hub_stat_';
--- Commandes
/*NB : temps de réalisation lors du dernier bilan = 2180 sec*/
/*Bilan des taxons par organisme*/
CASE WHEN (typ = 'taxon' OR typ = 'all') THEN
	cmd = 'SELECT z.lib_orgm, count(DISTINCT cd_ent_mere) as nb_taxon FROM '||libSchema||'.entite a JOIN '||libSchema||'.metadonnees_acteur z ON a.cd_jdd = z.cd_jdd GROUP BY z.lib_orgm ORDER BY z.lib_orgm';
	sstyp = 'taxon'; SELECT * FROM hub_file(chemin, prefixe||sstyp, cmd) INTO out.lib_log;	RETURN NEXT OUT;PERFORM hub_log (libSchema, out);
ELSE END CASE;

/*Bilan des observations par organisme*/
CASE WHEN (typ = 'observation' OR typ = 'all') THEN
	cmd = 'SELECT z.lib_orgm, count(DISTINCT cd_obs_mere) as nb_obs FROM '||libSchema||'.observation a JOIN '||libSchema||'.metadonnees_acteur z ON a.cd_jdd = z.cd_jdd GROUP BY z.lib_orgm ORDER BY z.lib_orgm';
	sstyp = 'observation'; SELECT * FROM hub_file(chemin, prefixe||sstyp, cmd) INTO out.lib_log;	RETURN NEXT OUT;PERFORM hub_log (libSchema, out);
ELSE END CASE;

/*Bilan des relevé par organisme*/
CASE WHEN (typ = 'releve' OR typ = 'all') THEN
	cmd = 'SELECT z.lib_orgm, count(DISTINCT cd_releve) as nb_releve FROM '||libSchema||'.releve a JOIN '||libSchema||'.metadonnees_acteur z ON a.cd_jdd = z.cd_jdd GROUP BY z.lib_orgm ORDER BY z.lib_orgm';
	sstyp = 'releve'; SELECT * FROM hub_file(chemin, prefixe||sstyp, cmd) INTO out.lib_log;	RETURN NEXT OUT;PERFORM hub_log (libSchema, out);
ELSE END CASE;

--- Output&Log
CASE WHEN out.lib_log = '' THEN out.lib_log = 'typ mal renseigné';PERFORM hub_log (libSchema, out);RETURN NEXT out; ELSE END CASE;
END; $BODY$ LANGUAGE plpgsql;

-- SELECT * FROM hub_stat('hub');


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_update 
--- Description : Mise à jour de données (fonction utilisée par une autre fonction)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_update(schemaSource varchar,schemaDest varchar, tableSource varchar, tableDest varchar, jdd varchar, typAction varchar = 'diff') RETURNS setof zz_log  AS 
$BODY$  
DECLARE out zz_log%rowtype; 
DECLARE metasource varchar; 
DECLARE listJdd varchar; 
DECLARE cmd varchar; 
DECLARE champRef varchar; 
DECLARE champRef_guillement varchar; 
DECLARE source varchar; 
DECLARE destination varchar; 
DECLARE flag integer; 
DECLARE compte integer;
DECLARE listeChamp varchar;
DECLARE libChamp varchar;
DECLARE val varchar;
DECLARE wheres varchar;
DECLARE jointure varchar;
BEGIN
--Variable
SELECT CASE WHEN substring(tableSource from 0 for 5) = 'temp' THEN 'temp_metadonnees' ELSE 'metadonnees' END INTO metasource;
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN EXECUTE 'SELECT CASE WHEN string_agg(''''''''||cd_jdd||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||cd_jdd||'''''''','','') END FROM "'||schemaSource||'"."'||metasource||'" WHERE typ_jdd = '''||jdd||''';' INTO listJdd;
ELSE listJdd := ''''||jdd||'''';END CASE;
source := '"'||schemaSource||'"."'||tableSource||'"';
destination := '"'||schemaDest||'"."'||tableDest||'"';
--- Output
out.lib_schema := schemaSource; out.lib_table := tableSource; out.lib_champ := '-'; out.typ_log := 'hub_update';SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
--- Commande
EXECUTE 'SELECT string_agg(''a."''||cd_champ||''" = b."''||cd_champ||''"'','' AND '') FROM ref.fsd WHERE (cd_table = '''||tableSource||''' OR cd_table = '''||tableDest||''') AND unicite = ''Oui''' INTO jointure;
EXECUTE 'SELECT string_agg(''''''''||cd_champ||'''''''','', '') FROM ref.fsd WHERE (cd_table = '''||tableSource||''' OR cd_table = '''||tableDest||''') AND unicite = ''Oui''' INTO champRef_guillement;
EXECUTE 'SELECT string_agg(''a.''||cd_champ,''||'') FROM ref.fsd WHERE (cd_table = '''||tableSource||''' OR cd_table = '''||tableDest||''') AND unicite = ''Oui''' INTO champRef;
EXECUTE 'SELECT string_agg(''"''||column_name||''" = b."''||column_name||''"::''||udt_name,'','')  FROM information_schema.columns WHERE table_name = '''||tableDest||''' AND table_schema = '''||schemaDest||''' AND column_name NOT IN ('||champRef_guillement||')' INTO listeChamp;
EXECUTE 'SELECT string_agg(''a."''||column_name||''"::varchar <> b."''||column_name||''"::varchar'','' OR '')  FROM information_schema.columns where table_name = '''||tableSource||''' AND table_schema = '''||schemaSource||''' AND column_name NOT IN ('||champRef_guillement||')' INTO wheres;
EXECUTE 'SELECT count(DISTINCT '||champRef||') FROM '||source||' a JOIN '||destination||' b ON '||jointure||' WHERE a.cd_jdd IN ('||listJdd||') AND ('||wheres||');' INTO compte;

CASE WHEN (compte > 0) THEN
	CASE WHEN typAction = 'push_diff' THEN
		---EXECUTE 'SELECT string_agg(''''''''||b."'||champRef||'"||'''''''','','') FROM '||source||' a JOIN '||destination||' b ON '||jointure||' WHERE a.cd_jdd IN ('||listJdd||') AND ('||wheres||');' INTO val;
		EXECUTE 'UPDATE '||destination||' a SET '||listeChamp||' FROM (SELECT * FROM '||source||') b WHERE '||jointure||';';
		out.lib_table := tableSource; out.lib_log := 'Concept(s) modifié(s)'; out.nb_occurence := compte||' occurence(s)'; return next out; ---PERFORM hub_log (schemaSource, out);
	WHEN typAction = 'diff' THEN
		out.lib_log := 'Différences : concept(s) à modifier'; out.nb_occurence := compte||' occurence(s)';return next out; ---PERFORM hub_log (schemaSource, out); 
	WHEN typAction = 'diff_plus' THEN --- CaS utilisé pour analyser les différences en profondeur
	FOR libChamp IN EXECUTE 'SELECT cd_champ FROM ref.fsd WHERE (cd_table = '''||tableSource||''' OR cd_table = '''||tableDest||''') GROUP BY cd_champ'
		LOOP 
		EXECUTE 'SELECT count('||champRef||') FROM '||source||' a LEFT JOIN '||destination||' b ON '||jointure||' WHERE a.'||libChamp||'::varchar <> b.'||libChamp||'::varchar AND a.cd_jdd IN ('||listJdd||')' INTO compte;
		CASE WHEN (compte > 0) THEN
			cmd := 'SELECT '||champRef||', a.'||libChamp||', b.'||libChamp||' FROM '||source||' a LEFT JOIN '||destination||' b ON '||jointure||' WHERE a.'||libChamp||' <> b.'||libChamp||' AND a.cd_jdd IN ('||listJdd||');';
			out.nb_occurence := compte||' ajout'; out.lib_log := cmd; RETURN NEXT out;
		ELSE out.nb_occurence := '-'; out.lib_log := 'Aucune différence détectée'; RETURN NEXT out;
		END CASE;
	END LOOP;
	END CASE;

ELSE out.lib_log := 'Aucune différence'; out.nb_occurence := '-'; return next out; ---PERFORM hub_log (schemaSource, out);
END CASE;	
END;$BODY$ LANGUAGE plpgsql;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_verif 
--- Description : Vérification des données
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_verif(libSchema varchar, jdd varchar, typVerif varchar = 'all') RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE result twocol%rowtype;
DECLARE typJdd varchar;
DECLARE libTable varchar;
DECLARE libChamp varchar;
DECLARE typChamp varchar;
DECLARE ref threecol%rowtype;
DECLARE val varchar;
DECLARE flag integer;
DECLARE compte integer;
DECLARE version_taxref integer;
BEGIN
--- Output
out.lib_schema := libSchema;out.typ_log := 'hub_verif';SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
--- Variables
CASE WHEN jdd = 'data' OR jdd = 'taxa' OR jdd = 'meta' THEN typJdd := Jdd;
ELSE EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'"."temp_metadonnees" WHERE cd_jdd = '''||jdd||'''' INTO typJdd;
END CASE;
out.lib_log = '';

--- Test concernant l'obligation
CASE WHEN (typVerif = 'obligation' OR typVerif = 'all') THEN
FOR libTable in EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd WHERE typ_jdd = '''||typJdd||''''
LOOP		
	FOR libChamp in EXECUTE 'SELECT cd_champ FROM ref.fsd WHERE cd_table = '''||libTable||''' AND typ_jdd = '''||typJdd||''' AND obligation = ''Oui'''
	LOOP		
		compte := 0;
		EXECUTE 'SELECT count(*) FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" IS NULL' INTO compte;
		CASE WHEN (compte > 0) THEN
			--- log
			out.lib_table := libTable; out.lib_champ := libChamp;out.lib_log := 'Champ obligatoire non renseigné => SELECT * FROM hub_verif_plus('''||libSchema||''','''||libTable||''','''||libChamp||''',''obligation'');'; out.nb_occurence := compte||' champs vides'; return next out;
			out.lib_log := typJdd ||' : Champ obligatoire';PERFORM hub_log (libSchema, out);
		ELSE --- rien
		END CASE;
	END LOOP;
END LOOP;
ELSE --- rien
END CASE;

--- Test concernant le typage des champs
CASE WHEN (typVerif = 'type' OR typVerif = 'all') THEN
FOR libTable in EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd WHERE typ_jdd = '''||typJdd||''''
	LOOP
	FOR libChamp in EXECUTE 'SELECT cd_champ FROM ref.fsd WHERE cd_table = '''||libTable||''' AND typ_jdd = '''||typJdd||''''
	LOOP	
		compte := 0;
		EXECUTE 'SELECT DISTINCT format FROM ref.fsd WHERE cd_champ = '''||libChamp||'''' INTO typChamp;
		IF (typChamp = 'int') THEN --- un entier
			EXECUTE 'SELECT count(*) FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^\d+$''' INTO compte;
		ELSIF (typChamp = 'float') THEN --- un float
			EXECUTE 'SELECT count(*) FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^\-?\d+\.\d+$''' INTO compte;
		ELSIF (typChamp = 'date') THEN --- une date
			EXECUTE 'SELECT count(*) FROM "'||libSchema||'"."temp_'||libTable||'" WHERE hub_verif_date('||libChamp||') IS FALSE' INTO compte;
		ELSIF (typChamp = 'boolean') THEN --- Boolean
			EXECUTE 'SELECT count(*) FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^t$'' AND "'||libChamp||'" !~ ''^f$''' INTO compte;
		ELSE --- le reste
			compte := 0;
		END IF;
		CASE WHEN (compte > 0) THEN
			--- log
			out.lib_table := libTable; out.lib_champ := libChamp;	out.lib_log := typChamp||' incorrecte => SELECT * FROM hub_verif_plus('''||libSchema||''','''||libTable||''','''||libChamp||''',''type'');'; out.nb_occurence := compte||' occurence(s)'; return next out;
			out.lib_log := typJdd ||' : '||typChamp||' incorrecte ';PERFORM hub_log (libSchema, out);
		ELSE --- rien
		END CASE;	
		END LOOP;
	END LOOP;
ELSE --- rien
END CASE;

--- Test concernant les doublon
CASE WHEN (typVerif = 'doublon' OR typVerif = 'all') THEN
FOR libTable in EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd WHERE typ_jdd = '''||typJdd||''''
	LOOP
	FOR libChamp in EXECUTE 'SELECT string_agg(''"''||cd_champ||''"'',''||'') FROM ref.fsd WHERE cd_table = '''||libTable||''' AND unicite = ''Oui'' AND typ_jdd = '''||typJdd||''''
		LOOP	
		compte := 0;
		EXECUTE 'SELECT count('||libChamp||') FROM "'||libSchema||'"."temp_'||libTable||'" GROUP BY '||libChamp||' HAVING COUNT('||libChamp||') > 1' INTO compte;
		CASE WHEN (compte > 0) THEN
			--- log
			out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := 'doublon(s) => SELECT * FROM hub_verif_plus('''||libSchema||''','''||libTable||''','''||libChamp||''',''doublon'');'; out.nb_occurence := compte||' occurence(s)'; return next out;
			out.lib_log := typJdd ||' : doublon(s)';PERFORM hub_log (libSchema, out);			
		ELSE --- rien
		END CASE;
		END LOOP;
	END LOOP;
ELSE --- rien
END CASE;

--- Test concernant le vocbulaire controlé
CASE WHEN (typVerif = 'vocactrl' OR typVerif = 'all') THEN
	FOR result IN EXECUTE 'SELECT a.cd_champ, a.cd_table FROM ref.fsd a JOIN ref.voca_ctrl z ON a.cd_champ = z.cd_champ WHERE typ_jdd = '''||typJdd||''' GROUP BY a.cd_champ, a.cd_table;' LOOP
		compte := 0;
		EXECUTE 'SELECT count(*) FROM '||libSchema||'.temp_'||result.col2||' LEFT JOIN (SELECT code_valeur FROM ref.voca_ctrl WHERE cd_champ = '''||result.col1||''') as voca_ctrl ON '||result.col1||' = voca_ctrl.code_valeur WHERE code_valeur IS NULL'  INTO compte;
		CASE WHEN (compte > 0) THEN
			--- log
			out.lib_table := result.col2; out.lib_champ := result.col1; out.lib_log := 'Valeur(s) non listée(s) => SELECT * FROM hub_verif_plus('''||libSchema||''','''||result.col2||''','''||result.col1||''',''vocactrl'');'; out.nb_occurence := compte||' occurence(s)'; return next out;
			out.lib_log := typJdd ||' : Valeur(s) non listée(s)';PERFORM hub_log (libSchema, out);
		ELSE END CASE;
	END LOOP;
ELSE END CASE;

-- Test concernant les référentiels
CASE WHEN (typVerif = 'ref' OR typVerif = 'all') THEN
FOR libTable IN EXECUTE 'SELECT cd_table FROM ref.fsd JOIN ref.aa_meta ON libelle = cd_champ WHERE typ = ''champ_ref'' AND typ_jdd = '''||typJdd||''' GROUP BY cd_table;' LOOP
	FOR libChamp in EXECUTE 'SELECT cd_champ FROM ref.fsd JOIN ref.aa_meta ON libelle = cd_champ WHERE typ = ''champ_ref'' AND cd_table = '''||libTable||''' GROUP BY cd_champ;' LOOP
		FOR ref IN EXECUTE 'SELECT one.nom_ref, champ_corresp, condition_ref FROM (SELECT nom_ref, libelle as condition_ref FROM ref.aa_meta a WHERE typ = ''condition_ref'') as one JOIN (SELECT nom_ref, libelle as champ_ref FROM ref.aa_meta a WHERE typ = ''champ_ref'') as two ON one.nom_ref = two.nom_ref JOIN (SELECT nom_ref, libelle as champ_corresp FROM ref.aa_meta a WHERE typ = ''champ_corresp'') as trois ON one.nom_ref = trois.nom_ref WHERE champ_ref = '''||libChamp||'''' LOOP
			EXECUTE 'SELECT count(*) FROM '||libSchema||'.temp_'||libTable||' a LEFT JOIN ref.'||ref.col1||' z ON a.'||libChamp||' = z.'||ref.col2||' WHERE '||ref.col3||' AND z.'||ref.col2||' IS NULL;' INTO compte;
			CASE WHEN (compte > 0) THEN
				--- log
				out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := ref.col1||' - Valeur(s) hors référentiel => SELECT * FROM hub_verif_plus('''||libSchema||''','''||libTable||''','''||libChamp||''',''ref'');'; 
				out.nb_occurence := compte||' occurence(s)'; return next out;
				out.lib_log := typJdd ||' : Valeur(s) hors référentiel';PERFORM hub_log (libSchema, out);
			ELSE END CASE;
			END LOOP;
		END LOOP;
	END LOOP;
ELSE END CASE;

--- Test concernant la cohérence sur les dates

CASE WHEN (typVerif = 'coh_date' OR typVerif = 'all') THEN
		compte:=0;
		for libChamp in EXECUTE 'SELECT cd_releve FROM '||libSchema||'.temp_releve where cd_releve IN (SELECT cd_releve FROM '||libSchema||'.temp_releve WHERE hub_verif_date(date_debut) IS TRUE AND hub_verif_date(date_fin) IS TRUE);' LOOP
			EXECUTE 'SELECT cd_releve FROM '||libSchema||'.temp_releve WHERE cd_releve='''||libChamp||''' AND ((date_debut::date > now() OR  date_fin::date > now()) or date_debut::date > date_fin::date) ;' into val;
			CASE WHEN val is not null THEN compte:=compte +1;  ELSE END CASE;
		END LOOP;
		CASE WHEN (compte > 0) THEN
			--- log
			out.lib_table := 'releve'; out.lib_champ := 'date_debut'; out.lib_log := 'Valeur(s) non cohérente(s) => SELECT * FROM hub_verif_plus('''||libSchema||''',''releve'',''date_debut'',''coh_date'');'; out.nb_occurence := compte||' occurence(s)'; return next out;
			out.lib_log := typJdd ||' : Valeur(s) non cohérente(s)';PERFORM hub_log (libSchema, out);
		ELSE END CASE;
ELSE END CASE;


--- Test concernant la cohérence sur les taxon
CASE WHEN (typVerif = 'coh_taxref_data' OR typVerif = 'all') THEN
	FOR version_taxref IN 2..9 LOOP
		EXECUTE 'SELECT count(*) FROM '||libSchema||'.temp_observation a LEFT JOIN ref.taxref_v'||version_taxref||' z ON a.cd_ref = z.cd_nom WHERE z.cd_nom IS NULL AND cd_reftaxo = ''TAXREF'' AND version_reftaxo = '''||version_taxref||''''  INTO compte;
		CASE WHEN (compte > 0) THEN
			--- log
			out.lib_table := 'observation'; out.lib_champ := 'cd_ref'; out.lib_log := 'Valeur(s) non cohérente(s) - TAXREF v'||version_taxref||' => SELECT * FROM hub_verif_plus('''||libSchema||''',''observation'',''cd_ref'',''coh_taxref_data'');'; out.nb_occurence := compte||' occurence(s)'; return next out;
			out.lib_log := typJdd ||' : Valeur(s) non cohérente(s)';PERFORM hub_log (libSchema, out);
		ELSE END CASE;
	END LOOP;
ELSE END CASE;


--- Test concernant l'intégrité des données (ex: une observation à toujours un relevé correspondant et un relevé a toujours au moins un territoire et un acteur correspondant)
CASE WHEN (typVerif = 'integrite' OR typVerif = 'all') THEN
FOR libTable IN EXECUTE 'SELECT cd_table FROM ref.fsd WHERE fk_table <> '''' AND typ_jdd = '''||typJdd||''' GROUP BY cd_table;' LOOP
	FOR libChamp in EXECUTE 'SELECT cd_champ FROM ref.fsd WHERE cd_table = '''||libTable||''' and fk_table is not null GROUP BY cd_champ;' LOOP
		FOR result IN EXECUTE 'SELECT cd_champ, fk_table FROM ref.fsd WHERE cd_table = '''||libTable||''' and fk_table is not null and cd_champ='''||libChamp||''' GROUP BY cd_champ, fk_table;' LOOP
			---Intégrité dans un sens
			EXECUTE 'SELECT count(*) FROM '||libSchema||'.temp_'||libTable||' a LEFT JOIN '||libSchema||'.temp_'||result.col2||' z ON a.'||libChamp||' = z.'||result.col1||' AND a.cd_jdd=z.cd_jdd WHERE  z.'||result.col1||' IS NULL and (a.'||libChamp||' is not null or a.'||libChamp||'  <> '''') ;' INTO compte;
			CASE WHEN (compte > 0) THEN
				--- log
				out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := 'Problème d intégrité: la table '''||libTable||''' a '||compte||' données qui n ont pas de champ '''||libChamp||''' correspondant dans la table '''||result.col2||'''   => SELECT * FROM hub_verif_plus('''||libSchema||''','''||libTable||','||result.col2||''','''||libChamp||''',''integrite'');'; 
				out.nb_occurence := compte||' occurence(s)'; return next out;
				out.lib_log := typJdd ||' : Problème d intégrité';PERFORM hub_log (libSchema, out);
			ELSE END CASE;
			END LOOP;
		END LOOP;
	END LOOP;
ELSE END CASE;


--- Le 100%
CASE WHEN out.lib_log = '' THEN
	out.lib_table := '-'; out.lib_champ := typVerif; out.lib_log := jdd||' conformes pour '||typVerif; out.nb_occurence := '-'; PERFORM hub_log (libSchema, out); return next out;
ELSE ---Rien
END CASE;

END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_verif_plus
--- Description : Vérification des données
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_verif_plus(libSchema varchar, libTable varchar, libChamp varchar, typVerif varchar = 'all', limited integer = 50) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE champRefSelected varchar;
DECLARE champRef varchar;
DECLARE typChamp varchar;
DECLARE flag integer;
DECLARE result twocol%rowtype;
DECLARE table varchar;

BEGIN
--- Output
out.lib_schema := libSchema;out.typ_log := 'hub_verif_plus';SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
--- Variables
EXECUTE 'SELECT string_agg(cd_champ,''||'') FROM ref.fsd WHERE cd_table = '''||libTable||''' AND unicite = ''Oui'';' INTO champRef;

--- Test concernant l'obligation
CASE WHEN (typVerif = 'obligation' OR typVerif = 'all') THEN
FOR champRefSelected IN EXECUTE 'SELECT '||champRef||' FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" IS NULL LIMIT '||limited||';'
	LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  '||champRef||' = '''||champRefSelected||''''; return next out; END LOOP;
ELSE --- rien
END CASE;

--- Test concernant le typage des champs
CASE WHEN (typVerif = 'type' OR typVerif = 'all') THEN
	EXECUTE 'SELECT DISTINCT format FROM ref.fsd WHERE cd_champ = '''||libChamp||'''' INTO typChamp;
		IF (typChamp = 'int') THEN --- un entier
			FOR champRefSelected IN EXECUTE 'SELECT '||champRef||' FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^\d+$''  LIMIT '||limited||';' 
			LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  '||champRef||' = '''||champRefSelected||''''; return next out;END LOOP;
		ELSIF (typChamp = 'float') THEN --- un float
			FOR champRefSelected IN EXECUTE 'SELECT '||champRef||' FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^\-?\d+\.\d+$''  LIMIT '||limited||';'
			LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  '||champRef||' = '''||champRefSelected||''''; return next out;END LOOP;
		ELSIF (typChamp = 'date') THEN --- une date
			FOR champRefSelected IN EXECUTE 'SELECT '||champRef||' FROM "'||libSchema||'"."temp_'||libTable||'" WHERE hub_verif_date('||libChamp||') IS FALSE  LIMIT '||limited||';'
			LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  '||champRef||' = '''||champRefSelected||''''; return next out;END LOOP;
		ELSIF (typChamp = 'boolean') THEN --- Boolean
			FOR champRefSelected IN EXECUTE 'SELECT '||champRef||' FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^t$'' AND "'||libChamp||'" !~ ''^f$''  LIMIT '||limited||';'
			LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  '||champRef||' = '''||champRefSelected||''''; return next out;END LOOP;
		ELSE --- le reste
			EXECUTE 'SELECT 1';
		END IF;
ELSE --- rien
END CASE;

--- Test concernant les doublon
CASE WHEN (typVerif = 'doublon' OR typVerif = 'all') THEN
	FOR champRefSelected IN EXECUTE 'SELECT '||libChamp||' FROM "'||libSchema||'"."temp_'||libTable||'" GROUP BY '||libChamp||' HAVING COUNT('||libChamp||') > 1'
		LOOP EXECUTE 'SELECT '||champRef||' FROM "'||libSchema||'"."temp_'||libTable||'" WHERE '||libChamp||' = '''||champRefSelected||''' LIMIT '||limited||';' INTO champRefSelected;
		out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  '||champRef||' = '''||champRefSelected||''''; return next out;END LOOP;
ELSE --- rien
END CASE;

--- Test concernant le vocbulaire controlé
CASE WHEN (typVerif = 'vocactrl' OR typVerif = 'all') THEN
	EXECUTE 'SELECT DISTINCT 1 FROM ref.voca_ctrl WHERE cd_champ = '''||libChamp||''' ;' INTO flag;
		CASE WHEN flag = 1 THEN
		FOR champRefSelected IN EXECUTE 'SELECT '||champRef||' FROM "'||libSchema||'"."temp_'||libTable||'" LEFT JOIN (SELECT code_valeur FROM ref.voca_ctrl WHERE cd_champ = '''||libChamp||''') as voca_ctrl ON "'||libChamp||'" = voca_ctrl.code_valeur WHERE code_valeur IS NULL  LIMIT '||limited||';'
		LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  '||champRef||' = '''||champRefSelected||''''; return next out; END LOOP;
	ELSE ---Rien
	END CASE;
ELSE --- rien
END CASE;

CASE WHEN (typVerif = 'coh_date' OR typVerif = 'all') THEN
	FOR libChamp in EXECUTE 'SELECT cd_releve FROM '||libSchema||'.temp_releve where cd_releve IN (SELECT cd_releve FROM '||libSchema||'.temp_releve WHERE hub_verif_date(date_debut) IS TRUE AND hub_verif_date(date_fin) IS TRUE);' LOOP
		EXECUTE 'SELECT cd_releve FROM '||libSchema||'.temp_releve WHERE cd_releve='''||libChamp||''' AND  ((date_debut::date > now() OR  date_fin::date > now()) or date_debut::date > date_fin::date);' into champRefSelected;
		CASE WHEN champRefSelected is not null 
			THEN out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT cd_releve, date_debut, date_fin FROM "'||libSchema||'"."temp_releve" WHERE cd_releve = '''||champRefSelected||''''; return next out;   
		ELSE END CASE;
	END LOOP;
ELSE END CASE;


--- Test concernant le vocbulaire controlé
CASE WHEN (typVerif = 'coh_taxref_data' OR typVerif = 'all') THEN
	FOR version_taxref IN 2..9 LOOP
		FOR champRefSelected IN EXECUTE 'SELECT cd_obs_mere FROM '||libSchema||'.temp_observation a LEFT JOIN ref.taxref_v'||version_taxref||' z ON a.cd_ref = z.cd_nom WHERE z.cd_nom IS NULL AND cd_reftaxo = ''TAXREF'' AND version_reftaxo = '''||version_taxref||'''  LIMIT '||limited||';' LOOP 
		out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; 
		out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_observation" WHERE cd_obs_mere = '''||champRefSelected||''''; return next out; 
		END LOOP;
	END LOOP;
ELSE END CASE;


--- Test concernant l'intégrité des données (ex: une observation à toujours un relevé correspondant et un relevé a toujours au moins un territoire et un acteur correspondant)
CASE WHEN (typVerif = 'integrite' OR typVerif = 'all') THEN
	FOR result IN EXECUTE 'SELECT split_part('''||libTable||''','','',1) as table1,split_part('''||libTable||''','','',2) as table2;' LOOP
		--- log
		FOR champRefSelected IN EXECUTE 'SELECT a.'||libChamp||' FROM '||libSchema||'.temp_'||result.col1||' a LEFT JOIN '||libSchema||'.temp_'||result.col2||' z ON a.'||libChamp||' = z.'||libChamp||' AND a.cd_jdd=z.cd_jdd WHERE  z.'||libChamp||' IS NULL and (a.'||libChamp||' is not null or a.'||libChamp||'  <> '''');' LOOP
			out.lib_table := result.col1; out.lib_champ := libChamp; out.lib_log := champRefSelected; 
			out.nb_occurence := 'SELECT a.*, z.'||libChamp||' as '||libChamp||'_jointure FROM "'||libSchema||'"."temp_'||result.col1||'" a  LEFT JOIN '||libSchema||'.temp_'||result.col2||' z ON a.'||libChamp||' = z.'||libChamp||' AND a.cd_jdd=z.cd_jdd WHERE a.'||libChamp||' = '''||champRefSelected||''''; return next out;
		END LOOP;
	END LOOP;			
ELSE END CASE;

--- Log général
RETURN;END;$BODY$ LANGUAGE plpgsql;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_verif_all
--- Description : Chainage des vérification
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_verif_all(libSchema varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
BEGIN
SELECT * into out FROM hub_verif(libSchema,'meta','all');return next out;
SELECT * into out FROM hub_verif(libSchema,'data','all');return next out;
SELECT * into out FROM hub_verif(libSchema,'taxa','all');return next out;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Fonction support
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_admin_right_dblink
--- Description : Ajout des droits de l'utilisation de dblink aux utilisateurs
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_admin_right_dblink (login varchar, is_granted boolean = true) RETURNS varchar  AS 
$BODY$ 
DECLARE cmd varchar;
BEGIN
CASE WHEN is_granted  IS TRUE THEN
cmd = '
GRANT ALL PRIVILEGES ON FUNCTION dblink(text,text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink(text,text,boolean) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink(text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink(text,boolean) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_build_sql_delete(text,int2vector,integer,text[]) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_build_sql_insert(text,int2vector,integer,text[],text[]) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_build_sql_update(text,int2vector,integer,text[],text[]) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_cancel_query(text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_close(text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_close(text,boolean) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_close(text,text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_close(text,text,boolean) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_connect(text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_connect(text,text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_connect_u(text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_connect_u(text,text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_disconnect(text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_error_message(text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_exec(text,text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_exec(text,text,boolean) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_exec(text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_exec(text,boolean) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_fetch(text,integer) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_fetch(text,integer,boolean) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_fetch(text,text,integer) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_fetch(text,text,integer,boolean) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_get_notify() TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_get_notify(text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_get_pkey(text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_get_result(text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_get_result(text,boolean) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_is_busy(text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_open(text,text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_open(text,text,boolean) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_open(text,text,text) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_open(text,text,text,boolean) TO "'||login||'";
GRANT ALL PRIVILEGES ON FUNCTION dblink_send_query(text,text) TO "'||login||'";
';
ELSE 
cmd = '
REVOKE ALL PRIVILEGES ON FUNCTION dblink(text,text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink(text,text,boolean) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink(text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink(text,boolean) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_build_sql_delete(text,int2vector,integer,text[]) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_build_sql_insert(text,int2vector,integer,text[],text[]) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_build_sql_update(text,int2vector,integer,text[],text[]) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_cancel_query(text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_close(text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_close(text,boolean) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_close(text,text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_close(text,text,boolean) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_connect(text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_connect(text,text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_connect_u(text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_connect_u(text,text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_disconnect(text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_error_message(text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_exec(text,text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_exec(text,text,boolean) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_exec(text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_exec(text,boolean) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_fetch(text,integer) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_fetch(text,integer,boolean) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_fetch(text,text,integer) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_fetch(text,text,integer,boolean) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_get_notify() FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_get_notify(text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_get_pkey(text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_get_result(text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_get_result(text,boolean) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_is_busy(text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_open(text,text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_open(text,text,boolean) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_open(text,text,text) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_open(text,text,text,boolean) FROM "'||login||'";
REVOKE ALL PRIVILEGES ON FUNCTION dblink_send_query(text,text) FROM "'||login||'";
';
END CASE;
EXECUTE cmd;
RETURN 'OK';
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_add 
--- Description : Ajout de données (fonction utilisée par une autre fonction)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Attention listJdd null sur jdd erroné
CREATE OR REPLACE FUNCTION hub_add(schemaSource varchar,schemaDest varchar, tableSource varchar, tableDest varchar,jdd varchar, typAction varchar = 'diff') RETURNS setof zz_log  AS 
$BODY$  
DECLARE out zz_log%rowtype;
DECLARE metasource varchar;
DECLARE listJdd varchar;
DECLARE champRefa varchar; 
DECLARE champRefb varchar; 
DECLARE source varchar;
DECLARE destination varchar;
DECLARE compte integer;
DECLARE listeChamp1 varchar;
DECLARE listeChamp2 varchar; 
DECLARE libChamp varchar; 
DECLARE cmd varchar; 
DECLARE jointure varchar; 
DECLARE nothing varchar; 
BEGIN
--Variables
source := '"'||schemaSource||'"."'||tableSource||'"';
destination := '"'||schemaDest||'"."'||tableDest||'"';
--- Output&Log
out.lib_schema := schemaSource; out.lib_table := tableSource; out.lib_champ := '-'; out.typ_log := 'hub_add'; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
--- Commande
SELECT CASE WHEN substring(tableSource from 0 for 5) = 'temp' THEN 'temp_metadonnees' ELSE 'metadonnees' END INTO metasource;
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN 
	EXECUTE 'SELECT CASE WHEN string_agg(''''''''||cd_jdd||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||cd_jdd||'''''''','','') END FROM "'||schemaSource||'"."'||metasource||'" WHERE typ_jdd = '''||jdd||''';' INTO listJdd;
ELSE listJdd := ''''||jdd||'''';
END CASE;

CASE WHEN typAction = 'push_total' OR typAction = 'push_diff' THEN
	EXECUTE 'SELECT string_agg(''a."''||column_name||''"::''||udt_name,'','')  FROM information_schema.columns where table_name = '''||tableDest||''' AND table_schema = '''||schemaDest||''' ' INTO listeChamp1;
	EXECUTE 'SELECT string_agg(''"''||column_name||''"'','','')  FROM information_schema.columns where table_name = '''||tableSource||''' AND table_schema = '''||schemaSource||''' ' INTO listeChamp2;
ELSE SELECT 1 INTO nothing; END CASE;

CASE WHEN typAction = 'diff' OR typAction = 'diff_plus' OR typAction = 'push_diff' THEN
	EXECUTE 'SELECT string_agg(''a.''||cd_champ,''||'') FROM ref.fsd WHERE (cd_table = '''||tableSource||''' OR cd_table = '''||tableDest||''') AND unicite = ''Oui''' INTO champRefa;
	EXECUTE 'SELECT string_agg(''b.''||cd_champ||'' IS NULL'','' AND '') FROM ref.fsd WHERE (cd_table = '''||tableSource||''' OR cd_table = '''||tableDest||''') AND unicite = ''Oui''' INTO champRefb;
	EXECUTE 'SELECT string_agg(''a."''||cd_champ||''" = b."''||cd_champ||''"'','' AND '') FROM ref.fsd WHERE (cd_table = '''||tableSource||''' OR cd_table = '''||tableDest||''') AND unicite = ''Oui''' INTO jointure;
	EXECUTE 'SELECT count(DISTINCT '||champRefa||') FROM '||source||' a LEFT JOIN '||destination||' b ON '||jointure||' WHERE '||champRefb||' AND a.cd_jdd IN ('||listJdd||')' INTO compte;
ELSE SELECT 1 INTO nothing; END CASE;

CASE WHEN typAction = 'push_total' THEN --- CAS utilisé pour ajouter en masse.
	EXECUTE 'INSERT INTO '||destination||' ('||listeChamp2||') SELECT '||listeChamp1||' FROM '||source||' a WHERE cd_jdd IN ('||listJdd||')';
		out.nb_occurence := 'total'; out.lib_log := 'Remplacement : Jdd complet(s) ajouté(s)'; RETURN NEXT out; --- PERFORM hub_log (schemaSource, out);
WHEN typAction = 'push_diff' THEN --- CAS utilisé pour ajouter les différences
	--- Recherche des concepts (obsevation, jdd ou entite) présent dans la source et absent dans la destination
	CASE WHEN (compte > 0) THEN --- Si de nouveau concept sont succeptible d'être ajouté
		EXECUTE 'INSERT INTO '||destination||' ('||listeChamp2||') SELECT '||listeChamp1||' FROM '||source||' a LEFT JOIN '||destination||' b ON '||jointure||' WHERE '||champRefb||' AND a.cd_jdd IN ('||listJdd||')';
		out.nb_occurence := compte||' occurence(s)'; out.lib_log := 'Ajout de la différence : Concept(s) ajouté(s)'; RETURN NEXT out; ---PERFORM hub_log (schemaSource, out);
	ELSE out.nb_occurence := '-'; out.lib_log := 'Ajout de la différence : Aucune différence'; RETURN NEXT out; --- PERFORM hub_log (schemaSource, out);
	END CASE;	
WHEN typAction = 'diff' THEN --- CAS utilisé pour analyser les différences
	--- Recherche des concepts (obsevation, jdd ou entite) présent dans la source et absent dans la destination
	CASE WHEN (compte > 0) THEN --- Si de nouveau concept sont succeptible d'être ajouté
		out.nb_occurence := compte||' occurence(s)'; out.lib_log := 'Différence : concept à ajouter depuis '||tableSource||' vers '||tableDest; RETURN NEXT out; --- PERFORM hub_log (schemaSource, out);
	ELSE out.nb_occurence := '-'; out.lib_log := 'Aucune différence détectée'; RETURN NEXT out; ---PERFORM hub_log (schemaSource, out);
	END CASE;
WHEN typAction = 'diff_plus' THEN --- CAS utilisé pour analyser les différences en profondeur
	CASE WHEN (compte > 0) THEN
		cmd := 'SELECT '||champRefa||' FROM '||source||' a LEFT JOIN '||destination||' b ON '||jointure||' WHERE '||champRefb||' AND a.cd_jdd IN ('||listJdd||');';
		out.nb_occurence := compte||' ajout'; out.lib_log := cmd; RETURN NEXT out;
	ELSE out.nb_occurence := '-'; out.lib_log := 'Aucune différence détectée'; RETURN NEXT out;
	END CASE;
ELSE out.lib_champ := '-'; out.lib_log := 'ERREUR : sur champ action = '||typAction; RETURN NEXT out; ---PERFORM hub_log (schemaSource, out);
END CASE;	
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_del 
--- Description : Suppression de données (fonction utilisée par une autre fonction)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_del(schemaSource varchar,schemaDest varchar, tableSource varchar, tableDest varchar, jdd varchar, typAction varchar = 'diff') RETURNS setof zz_log  AS 
$BODY$  
DECLARE out zz_log%rowtype;
DECLARE metasource varchar;
DECLARE listJdd varchar;
DECLARE source varchar;
DECLARE destination varchar;
DECLARE champRef varchar;
DECLARE champRefb varchar;
DECLARE champRefc varchar;
DECLARE compte integer;
DECLARE jointure varchar;
DECLARE cmd varchar;
BEGIN
--Variable
SELECT CASE WHEN substring(tableSource from 0 for 5) = 'temp' THEN 'temp_metadonnees' ELSE 'metadonnees' END INTO metasource;
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN EXECUTE 'SELECT CASE WHEN string_agg(''''''''||cd_jdd||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||cd_jdd||'''''''','','') END FROM "'||schemaSource||'"."'||metasource||'" WHERE typ_jdd = '''||jdd||''';' INTO listJdd;
ELSE listJdd := ''''||jdd||'''';END CASE;
source := '"'||schemaSource||'"."'||tableSource||'"';
destination := '"'||schemaDest||'"."'||tableDest||'"';
--- Output&Log
out.lib_schema := schemaSource; out.lib_table := tableSource; out.lib_champ := '-';out.typ_log := 'hub_del'; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;

--- Commande
EXECUTE 'SELECT string_agg(''a."''||cd_champ||''" = b."''||cd_champ||''"'','' AND '') FROM ref.fsd WHERE (cd_table = '''||tableSource||''' OR cd_table = '''||tableDest||''') AND unicite = ''Oui''' INTO jointure;
--- Recherche des concepts (obsevation, jdd ou entite) présent dans la partie propre et présent dans la partie temporaire
EXECUTE 'SELECT string_agg(''b.''||cd_champ,''||'') FROM ref.fsd WHERE (cd_table = '''||tableSource||''' OR cd_table = '''||tableDest||''') AND unicite = ''Oui''' INTO champRef;
EXECUTE 'SELECT string_agg('' b.''||cd_champ||'' = b.''||cd_champ,'' AND '') FROM ref.fsd WHERE (cd_table = '''||tableSource||''' OR cd_table = '''||tableDest||''') AND unicite = ''Oui''' INTO champRefb;
EXECUTE 'SELECT string_agg(''a.''||cd_champ||'' IS NULL'','' AND '') FROM ref.fsd WHERE (cd_table = '''||tableSource||''' OR cd_table = '''||tableDest||''') AND unicite = ''Oui''' INTO champRefc;
EXECUTE 'SELECT count(DISTINCT '||champRef||') FROM '||source||' b LEFT JOIN '||destination||' a ON '||jointure||' WHERE b.cd_jdd IN ('||listJdd||')' INTO compte; 
	
CASE WHEN (compte > 0) THEN --- Si de nouveau concept sont succeptible d'être ajouté
	out.nb_occurence := compte||' occurence(s)'; ---log
	CASE WHEN typAction = 'push_diff' THEN
		EXECUTE 'DELETE FROM '||destination||' as a USING '||source||' as b WHERE '||champRefb||' AND b.cd_jdd IN ('||listJdd||')';
		---cmd = 'DELETE FROM '||destination||' USING '||source||' as b WHERE '||champRefb||' AND b.cd_jdd IN ('||listJdd||')';
		out.nb_occurence := compte||' occurence(s)';out.lib_log := 'Concepts supprimés'; RETURN NEXT out; ---PERFORM hub_log (schemaSource, out);
		---out.nb_occurence := compte||' occurence(s)';out.lib_log := cmd; RETURN NEXT out; ---PERFORM hub_log (schemaSource, out);
	WHEN typAction = 'diff' THEN
		out.nb_occurence := compte||' occurence(s)';out.lib_log := 'Points communs'; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	WHEN typAction = 'diff_plus' THEN
		CASE WHEN (compte > 0) THEN
		cmd := 'SELECT '||champRef||' FROM '||source||' a RIGHT JOIN '||destination||' b ON '||jointure||' WHERE '||champRefc||' AND a.cd_jdd IN ('||listJdd||');';
		out.nb_occurence := compte||' suppression'; out.lib_log := cmd; RETURN NEXT out;
	ELSE out.nb_occurence := '-'; out.lib_log := 'Aucune différence détectée'; RETURN NEXT out;
	END CASE;
	ELSE out.nb_occurence := compte||' occurence(s)'; out.lib_log := 'ERREUR : sur champ action = '||typAction; RETURN NEXT out; ---PERFORM hub_log (schemaSource, out);
	END CASE;
ELSE out.lib_log := 'Aucune différence';out.nb_occurence := '-'; RETURN NEXT out; --- PERFORM hub_log (schemaSource, out); 
END CASE;	
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_diff 
--- Description : Analyse des différences entre une source et une cible
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_diff(libSchema varchar, jdd varchar,typAction varchar = 'add',mode integer = 1) RETURNS setof zz_log  AS 
$BODY$ 
DECLARE out zz_log%rowtype;
DECLARE flag integer;
DECLARE ct integer;
DECLARE ct2 integer;
DECLARE typJdd varchar;
DECLARE cmd varchar;
DECLARE libTable varchar;
DECLARE tableSource varchar;
DECLARE tableDest varchar;
DECLARE schemaSource varchar;
DECLARE schemaDest varchar;
DECLARE tableRef varchar;
DECLARE nothing varchar;
BEGIN
--- Output&Log
out.lib_schema := schemaSource; 
--- Variables
ct =0; ct2=0;
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN  typJdd := jdd; flag := 1;
ELSE EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'".temp_metadonnees WHERE cd_jdd = '''||jdd||''';' INTO typJdd;
	CASE WHEN typJdd = 'data' OR typJdd = 'taxa' THEN flag := 1; ELSE flag := 0; END CASE;
END CASE;
--- mode 1 = intra Shema / mode 2 = entre shema et agregation
CASE WHEN mode = 1 THEN schemaSource :=libSchema; schemaDest :=libSchema; WHEN mode = 2 THEN schemaSource :=libSchema; schemaDest :='agregation'; ELSE flag :=0; END CASE;
--- Commandes
CASE WHEN typAction = 'add' AND flag = 1 THEN
	--- Données et metadonnées
	FOR libTable in EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd WHERE typ_jdd = '''||typJdd||''' OR typ_jdd = ''meta'' ORDER BY cd_table;' LOOP 
		ct = ct +1;
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM  hub_add(schemaSource,schemaDest, tableSource, tableDest , jdd,'diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; PERFORM hub_log (libSchema, out);ELSE ct2 = ct2 +1; END CASE;
		SELECT * INTO out FROM  hub_update(schemaSource,schemaDest, tableSource, tableDest , jdd,'diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; PERFORM hub_log (libSchema, out);ELSE ct2 = ct2 +1; END CASE;
		SELECT * INTO out FROM  hub_add(schemaDest,schemaSource, tableDest, tableSource , jdd,'diff'); --- sens inverse (champ présent dans le propre et absent dans le temporaire)
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; PERFORM hub_log (libSchema, out);ELSE ct2 = ct2 +1; END CASE;
	END LOOP;
	ct2 = ct2/3;
WHEN typAction = 'del' AND flag = 1 THEN
	--- Données et métadonnées
	FOR libTable in EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd WHERE typ_jdd = '''||typJdd||''' OR typ_jdd = ''meta'' ORDER BY cd_table;' LOOP 
		ct = ct +1;
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM  hub_del(schemaSource,schemaDest, tableSource, tableDest , jdd,'diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; PERFORM hub_log (libSchema, out);ELSE ct2 = ct2 +1; END CASE;
	END LOOP;
WHEN typAction = 'diff_plus' AND flag = 1 THEN
	FOR libTable in EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd WHERE typ_jdd = '''||typJdd||''' OR typ_jdd = ''meta'' ORDER BY cd_table;' LOOP 
		ct = ct +1;
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM  hub_add(schemaSource,schemaDest, tableSource, tableDest , jdd,'diff_plus'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE ct2 = ct2 +1; END CASE;
		SELECT * INTO out FROM  hub_update(schemaSource,schemaDest, tableSource, tableDest , jdd,'diff_plus'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE ct2 = ct2 +1; END CASE;
		SELECT * INTO out FROM  hub_add(schemaDest,schemaSource, tableDest, tableSource , jdd,'diff_plus'); --- sens inverse (champ présent dans le propre et absent dans le temporaire)
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE ct2 = ct2 +1; END CASE;
	END LOOP;
	ct2 = ct2/3;	
ELSE out.lib_table := libTable; out.lib_log := jdd||' n''est pas un jeu de données valide'; out.nb_occurence := '-'; PERFORM hub_log (libSchema, out);RETURN NEXT out;
END CASE;

--- Last log
out.typ_log := 'hub_diff'; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user; out.lib_table := '-'; out.lib_champ := '-'; out.nb_occurence := ct||' tables analysées';
CASE WHEN ct = ct2 AND typAction = 'del' THEN out.lib_log := 'Aucun point commun sur le jdd '||jdd;  
	WHEN ct <> ct2 AND typAction = 'del' THEN out.lib_log := 'Des points communs sont présents - jdd '||jdd; 
	WHEN ct = ct2 AND typAction = 'add' THEN out.lib_log := 'Aucune différence sur le jdd '||jdd;
	WHEN ct <> ct2 AND typAction = 'add' THEN out.lib_log := 'Des différences sont présentes - jdd '||jdd;
	WHEN ct = ct2 AND typAction = 'diff_plus' THEN out.lib_log := 'Aucune différence sur le jdd '||jdd;
	WHEN ct <> ct2 AND typAction = 'diff_plus' THEN out.lib_log := 'Des différences sont présentes - jdd '||jdd;
	ELSE SELECT 1 INTO  nothing; END CASE;
PERFORM hub_log (libSchema, out);RETURN NEXT out; 
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_file
--- Description : exporte un resultat dans un fichier
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_file(chemin varchar, nom varchar, cmd varchar) RETURNS varchar  AS 
$BODY$
DECLARE fichier varchar;
BEGIN
fichier = chemin||nom||'.csv';
EXECUTE 'COPY ('||cmd||') TO '''||fichier||''' CSV HEADER ENCODING ''UTF-8'' DELIMITER E'';''';
RETURN 'out = '||fichier;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_help 
--- Description : Création de l'aide et Accéder à la description d'un fonction
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_help() RETURNS setof threecol AS 
$BODY$
DECLARE out threecol%rowtype;
DECLARE nom_fonction varchar;
DECLARE cmd varchar;
BEGIN
--- Variable

--- Commande
out.col1 := '-------------------------'; out.col2 := '-------------------------'; RETURN next out; 
out.col1 := ' Pour accéder à la description d''une fonction : ';
out.col2 := 'SELECT * FROM hub_help(''fonction'');';
RETURN next out;
out.col1 := ' Pour utiliser une fonction : '; 
out.col2 := 'SELECT * FROM fonction(''variables'');';
RETURN next out;
out.col1:= ' Liste des fonctions : ';
out.col2 = 'http://wiki.fcbn.fr/doku.php?id=outil:hub:fonction:liste;';
RETURN next out;
out.col1 := '-------------------------'; out.col2 := '-------------------------'; RETURN next out; 

FOR nom_fonction IN EXECUTE 'SELECT DISTINCT routine_name FROM information_schema.routines WHERE  routine_name LIKE ''hub_%'' ORDER BY routine_name'
LOOP 
	out.col1 := nom_fonction;
	cmd = 'SELECT routine_name||''(''||string_agg(parameter_name,'','')||'')'' FROM(
	SELECT routine_name, z.specific_name, z.parameter_name
	FROM information_schema.routines a
	JOIN information_schema.parameters z ON a.specific_name = z.specific_name
	WHERE  routine_name = '''||nom_fonction||'''
	ORDER BY routine_name,ordinal_position
	) as one
	GROUP BY routine_name, specific_name';

	EXECUTE cmd INTO out.col2;
	out.col3 := 'http://wiki.fcbn.fr/doku.php?id=outil:hub:fonction:'||nom_fonction;
	RETURN next out; 
   END LOOP;


END;$BODY$ LANGUAGE plpgsql;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_log
--- Description : ecrit les output dans le Log du schema et le log global
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_log (libSchema varchar, outp zz_log, action varchar = 'write') RETURNS void AS 
$BODY$ 
DECLARE exist integer;
BEGIN

/*ajout du user_log dans le zzlog*/
EXECUTE 'SELECT 1 FROM information_schema.columns WHERE table_schema = '''||libSchema||''' AND table_name = ''zz_log'' AND column_name = ''user_log'';' INTO exist;
CASE WHEN exist IS NULL THEN
	EXECUTE 'ALTER TABLE '||libSchema||'.zz_log add column user_log varchar;';
ELSE END CASE;

CASE WHEN action = 'write' THEN
	EXECUTE 'INSERT INTO "'||libSchema||'".zz_log (lib_schema,lib_table,lib_champ,typ_log,lib_log,nb_occurence,date_log,user_log) VALUES ('''||outp.lib_schema||''','''||outp.lib_table||''','''||outp.lib_champ||''','''||outp.typ_log||''','''||outp.lib_log||''','''||outp.nb_occurence||''','''||outp.date_log||''','''||outp.user_log||''');';
	CASE WHEN libSchema <> 'public' THEN EXECUTE 'INSERT INTO "public".zz_log (lib_schema,lib_table,lib_champ,typ_log,lib_log,nb_occurence,date_log,user_log) VALUES ('''||outp.lib_schema||''','''||outp.lib_table||''','''||outp.lib_champ||''','''||outp.typ_log||''','''||outp.lib_log||''','''||outp.nb_occurence||''','''||outp.date_log||''','''||outp.user_log||''');'; 
	ELSE PERFORM 1; END CASE;
WHEN action = 'clear' THEN
	EXECUTE 'TRUNCATE "'||libSchema||'".zz_log';
ELSE SELECT 1;
END CASE;
PERFORM hub_mail(outp.lib_schema,outp.typ_log,outp.date_log,outp.user_log);
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_log_simple
--- Description : ecrit les output dans le Log du schema et le log global - version simple
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_log_simple (libSchema varchar, fction varchar, logs varchar) RETURNS void AS 
$BODY$ 
BEGIN
EXECUTE 'INSERT INTO "'||libSchema||'".zz_log (lib_schema,lib_table,lib_champ,typ_log,lib_log,nb_occurence,date_log,user_log) VALUES ('''||libSchema||''',''-'',''-'','''||fction||''','''||logs||''',''-'','''||CURRENT_TIMESTAMP||''','''||current_user||''');';
CASE WHEN libSchema <> 'public' THEN EXECUTE 'INSERT INTO "public".zz_log (lib_schema,lib_table,lib_champ,typ_log,lib_log,nb_occurence,date_log,user_log) VALUES ('''||libSchema||''',''-'',''-'','''||fction||''','''||logs||''',''-'','''||CURRENT_TIMESTAMP||''','''||current_user||''');'; ELSE END CASE;
PERFORM hub_mail(libSchema,fction,CURRENT_TIMESTAMP,current_user);
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_mail
--- Description : Renseigne la table emailing_queue - infos transmises par mail à l'admin et aux cbn
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_mail (libSchema varchar,action varchar,date_log timestamp with time zone,user_log name) RETURNS void AS 
$BODY$ 
BEGIN
CASE WHEN action = 'hub_export' OR action = 'hub_push' OR action = 'hub_connect' OR action = 'hub_publicate' OR action = 'hub_import' OR action = 'siflore_data_refresh' THEN 
	INSERT INTO emailing_queue (lib_schema, action, date_log, user_log) VALUES (libSchema,action,date_log,user_log);
ELSE END CASE;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_publicate
--- Description : Notifie la demande de publisation sur le SI FLORE des données
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_publicate (libSchema varchar, jdd varchar, version integer = 7) RETURNS setof zz_log AS 
$BODY$ 
DECLARE out zz_log%rowtype;
DECLARE verifJdd varchar;
BEGIN
--- variable
out.lib_schema := libSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_publicate';SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;out.lib_log = 'jdd à publier';
--- commande
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN
	EXECUTE 'SELECT cd_jdd FROM '||libSchema||'.metadonnees WHERE typ_jdd = '''||jdd||''' LIMIT 1' INTO verifJdd;
	CASE WHEN verifJdd IS NOT NULL THEN
		out.nb_occurence := jdd;PERFORM hub_log (libSchema, out); RETURN next out;
		/*Constitution de la liste d'attente d'untégration des données*/
		INSERT INTO public.publicating_queue(lib_schema, jdd, version) VALUES (libSchema, jdd, version);
	ELSE
		out.lib_log = 'Pas de jdd '||jdd;out.nb_occurence := '-';PERFORM hub_log (libSchema, out); RETURN next out;
	END CASE;
ELSE 
	EXECUTE 'SELECT cd_jdd FROM '||libSchema||'.metadonnees WHERE cd_jdd = '''||jdd||'''' INTO verifJdd;
	CASE WHEN verifJdd IS NOT NULL THEN 
		out.nb_occurence := jdd; PERFORM hub_log (libSchema, out); RETURN next out;
		/*Constitution de la liste d'attente d'untégration des données*/
		INSERT INTO public.publicating_queue(lib_schema, jdd, version) VALUES (libSchema, jdd, version);
	ELSE
		out.lib_log = 'Ce jdd est absent de la base : '||jdd;out.nb_occurence := '-';PERFORM hub_log (libSchema, out); RETURN next out;
	END CASE;
END CASE;

END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_ref_create
--- Description : Création et import d'un référentiel 
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_ref_create(libTable varchar, path varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE flag2 integer;
DECLARE structure varchar;
DECLARE delimitr varchar;
BEGIN
EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = ''ref'' AND tablename = '''||libTable||''';' INTO flag2;
CASE WHEN libTable = 'aa_meta' THEN
	--- Initialisation du meta-référentiel
	CREATE TABLE IF NOT EXISTS ref.aa_meta(id serial NOT NULL, nom_ref varchar, typ varchar, ordre integer, libelle varchar, format varchar, CONSTRAINT aa_meta_pk PRIMARY KEY(id));
	TRUNCATE ref.aa_meta;
	EXECUTE 'COPY ref.aa_meta (nom_ref, typ, ordre, libelle, format) FROM '''||path||'aa_meta.csv'' HEADER CSV ENCODING ''UTF8'' DELIMITER E''\t'';';
ELSE
	EXECUTE 'SELECT ''(''||champs||'',''||contrainte||'')'' FROM (SELECT nom_ref, string_agg(libelle||'' ''||format,'','') as champs FROM ref.aa_meta WHERE nom_ref = '''||libTable||''' AND typ = ''champ'' GROUP BY nom_ref) as one JOIN (SELECT nom_ref, ''CONSTRAINT ''||nom_ref||''_pk PRIMARY KEY (''||libelle||'')'' as contrainte FROM ref.aa_meta WHERE nom_ref = '''||libTable||''' AND typ = ''cle_primaire'') as two ON one.nom_ref = two.nom_ref' INTO structure;
	EXECUTE 'SELECT CASE WHEN libelle = ''virgule'' THEN '','' WHEN libelle = ''tab'' THEN ''\t'' WHEN libelle = ''point_virgule'' THEN '';'' ELSE '';'' END as delimiter FROM ref.aa_meta WHERE nom_ref = '''||libTable||''' AND typ = ''delimiter''' INTO delimitr;
	EXECUTE 'CREATE TABLE ref.'||libTable||' '||structure||';'; out.lib_log := libTable||' créée';RETURN next out;
	EXECUTE 'COPY ref.'||libTable||' FROM '''||path||'ref_'||libTable||'.csv'' HEADER CSV DELIMITER E'''||delimitr||''' ENCODING ''UTF8'';';
	--- Index geo
	CASE WHEN substr(libTable,1,3) = 'geo' THEN EXECUTE 'CREATE INDEX '||libTable||'_gist ON ref.'||libTable||' USING GIST (geom);'; ELSE END CASE;
	out.lib_log := libTable||' : données importées';RETURN next out;
END CASE;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_ref_update
--- Description : Mise à jour référentiel 
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_ref_update(libTable varchar, path varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE flag2 integer;
DECLARE delimitr varchar;
DECLARE prefixe varchar;
BEGIN
EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = ''ref'' AND tablename = '''||libTable||''';' INTO flag2;
CASE WHEN libTable = 'aa_meta' THEN 
	TRUNCATE ref.aa_meta;
	EXECUTE 'COPY ref.aa_meta (nom_ref, typ, ordre, libelle, format) FROM '''||path||'aa_meta.csv'' HEADER CSV ENCODING ''UTF8'' DELIMITER E''\t'';';
ELSE 
	EXECUTE 'SELECT CASE WHEN libelle = ''virgule'' THEN '','' WHEN libelle = ''tab'' THEN ''\t'' WHEN libelle = ''point_virgule'' THEN '';'' ELSE '';'' END as delimiter FROM ref.aa_meta WHERE nom_ref = '''||libTable||''' AND typ = ''delimiter''' INTO delimitr; 
	CASE WHEN flag2 = 1 THEN
		EXECUTE 'TRUNCATE ref.'||libTable;
		EXECUTE 'COPY ref.'||libTable||' FROM '''||path||'ref_'||libTable||'.csv'' HEADER CSV DELIMITER E'''||delimitr||''' ENCODING ''UTF8'';';
		--- Index geo
		CASE WHEN substr(libTable,1,3) = 'geo' THEN 
			EXECUTE 'DROP INDEX IF EXISTS '||libTable||'_gist';
			EXECUTE 'CREATE INDEX '||libTable||'_gist ON ref.'||libTable||' USING GIST (geom);'; 
		ELSE END CASE;
		out.lib_log := 'Mise à jour de la table '||libTable;RETURN next out;
	ELSE out.lib_log := 'Les tables doivent être créée auparavant : SELECT * FROM hub_admin_ref(''create'',path)';RETURN next out;
	END CASE;
END CASE;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_verif_date
--- Description : Fonction de vérification du format date d'un champ
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_verif_date (champ varchar) returns boolean as $$
begin
     if ($1 is null) then
         return FALSE;
     end if;
     PERFORM $1::date;
     return TRUE;
exception when others then
     return FALSE;
end;
$$ language plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_admin_create
--- Description : Création des tables à partir du fsd
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION hub_admin_create(libSchema varchar, cd_table varchar, typ varchar= 'all') RETURNS void AS 
$BODY$
DECLARE list_champ varchar; 
DECLARE list_champ_sans_format varchar; 
DECLARE list_contraint varchar; 
DECLARE valeurs varchar; 
DECLARE result threecol%rowtype; 
BEGIN
/*Les tables propre et temp*/
CASE WHEN typ = 'all' OR typ = 'propre' THEN
	EXECUTE 'SELECT string_agg(one.cd_champ||'' ''||one.format,'','') FROM (SELECT cd_champ, format FROM ref.fsd WHERE cd_table = '''||cd_table||''' ORDER BY ordre_champ) as one;' INTO list_champ;
	EXECUTE 'SELECT ''CONSTRAINT ''||cd_table||''_pkey PRIMARY KEY (''||string_agg(cd_champ,'','')||'')'' FROM ref.fsd WHERE cd_table = '''||cd_table||''' AND unicite = ''Oui'' GROUP BY cd_table' INTO list_contraint ;
	EXECUTE 'CREATE TABLE '||libSchema||'.'||cd_table||' ('||list_champ||','||list_contraint||');';
	/*contrainte check - voca_ctrl*/
	PERFORM hub_add_constraint_check(libSchema, cd_table);
	/*contrainte check - geometry*/
	PERFORM hub_add_constraint_geom(libSchema, cd_table);

ELSE END CASE;
CASE WHEN typ = 'all' OR typ = 'temp' THEN
	EXECUTE 'SELECT string_agg(one.cd_champ||'' character varying'','','') FROM (SELECT cd_champ, format FROM ref.fsd WHERE cd_table = '''||cd_table||''' ORDER BY ordre_champ) as one;' INTO list_champ_sans_format;
	EXECUTE 'CREATE TABLE '||libSchema||'.temp_'||cd_table||' ('||list_champ_sans_format||');';
ELSE END CASE;

END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_add_constraint_check
--- Description : ajout des contrainte check au hub
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION hub_add_constraint_check(libSchema varchar, cd_table varchar) RETURNS void AS 
$BODY$
DECLARE valeurs varchar; 
DECLARE result threecol%rowtype; 
BEGIN
FOR result IN EXECUTE 'SELECT a.cd_champ, z.cd_table, z.format FROM ref.voca_ctrl a JOIN ref.fsd z ON a.cd_champ = z.cd_champ WHERE z.cd_table = '''||cd_table||''' GROUP BY a.cd_champ, z.cd_table, z.format;' LOOP
	CASE WHEN result.col3 = 'integer' THEN 
		EXECUTE 'SELECT ''(''||string_agg(code_valeur,'','')||'')'' FROM ref.voca_ctrl WHERE cd_champ = '''||result.col1||'''' INTO valeurs;
		ELSE EXECUTE 'SELECT replace(''(''''''||string_agg(code_valeur,'','')||'''''')'','','','''''','''''') FROM ref.voca_ctrl WHERE cd_champ = '''||result.col1||'''' INTO valeurs;  
	END CASE;
	EXECUTE 'ALTER TABLE '||libSchema||'.'||result.col2||' DROP CONSTRAINT IF EXISTS '||result.col1||'_check;';
	EXECUTE 'ALTER TABLE '||libSchema||'.'||result.col2||' ADD CONSTRAINT '||result.col1||'_check CHECK ('||result.col1||' IN '||valeurs||');';
END LOOP;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_add_constraint_geom
--- Description : ajout des contrainte geometrique au hub
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION hub_add_constraint_geom(libSchema varchar, cd_table varchar) RETURNS void AS 
$BODY$
DECLARE result threecol%rowtype; 
BEGIN
FOR result IN EXECUTE 'SELECT cd_champ, cd_table, srid_geom FROM ref.fsd WHERE cd_table = '''||cd_table||''' AND srid_geom IS NOT NULL GROUP BY cd_champ, cd_table, srid_geom;' LOOP
	EXECUTE 'ALTER TABLE '||libSchema||'.'||result.col2||' DROP CONSTRAINT IF EXISTS '||result.col1||'_srid;';
	EXECUTE 'ALTER TABLE '||libSchema||'.'||result.col2||' ADD CONSTRAINT '||result.col1||'_srid CHECK (st_srid('||result.col1||') = '||result.col3||');';
END LOOP;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_add_champ
--- Description : ajout de champ direct au hub
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION hub_add_champ(libSchema varchar, cd_table varchar, cd_champ varchar, typ_champ varchar) RETURNS void AS 
$BODY$
DECLARE exist integer; 
BEGIN
EXECUTE 'SELECT 1 FROM information_schema.columns WHERE table_schema = '''||libSchema||''' AND table_name = '''||cd_table||''' AND column_name = '''||cd_champ||''';' into exist;
CASE WHEN exist IS NULL THEN
	EXECUTE 'ALTER TABLE '||libSchema||'.'||cd_table||' add column '||cd_champ||' '||typ_champ||';';
ELSE END CASE;
END;$BODY$ LANGUAGE plpgsql;
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_reset_sequence
--- Description : Générique - met à jour les séquence au max de l'identifiant
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION hub_reset_sequence() RETURNS void AS 
$BODY$
DECLARE _sql VARCHAR := '';
DECLARE result threecol%rowtype; 
BEGIN
FOR result IN 
WITH fq_objects AS (SELECT c.oid,n.nspname || '.' ||c.relname AS fqname ,c.relkind, c.relname AS relation FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace ),
	sequences AS (SELECT oid,fqname FROM fq_objects WHERE relkind = 'S'),
	tables    AS (SELECT oid, fqname FROM fq_objects WHERE relkind = 'r' )
SELECT
       s.fqname AS sequence,
       t.fqname AS table,
       a.attname AS column
FROM
     pg_depend d JOIN sequences s ON s.oid = d.objid
                 JOIN tables t ON t.oid = d.refobjid
                 JOIN pg_attribute a ON a.attrelid = d.refobjid and a.attnum = d.refobjsubid
WHERE
     d.deptype = 'a' 
LOOP
     EXECUTE 'SELECT setval('''||result.col1||''', COALESCE((SELECT MAX('||result.col3||')+1 FROM '||result.col2||'), 1), false);';
END LOOP;
END;$BODY$ LANGUAGE plpgsql;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- FIN : Affichage de la réinitialisation
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
INSERT INTO public.zz_log (lib_schema,lib_table,lib_champ,typ_log,lib_log,nb_occurence,date_log,user_log) 
	VALUES ('public','-','-','hub_admin_init','Initialisation de hub.sql','1',CURRENT_TIMESTAMP,current_user);
SELECT * FROM public.zz_log ORDER BY date_log desc LIMIT 1;