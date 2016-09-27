-------------------------------------------------------------
-------------------------------------------------------------
--- Pas à pas pour la mise à jour du CODEX
-------------------------------------------------------------
--- 1. Initialisation
-- La création du codex est géré à partir de l'outil codex (https://github.com/fedecbn/codex)
-- Récupérer les fonction en lançant les script hub.sql

--- 2. Création d'un hub pour récupérer les données
-- SELECT * FROM hub_connect_ref_simple('all');
-- SELECT * FROM hub_admin_clone('hub');

-------------------------------------------------------------
-------------------------------------------------------------
--- Pas à pas pour la mise à jour des données dans le CODEX -
-------------------------------------------------------------
--- 1. Mise à jour des données (HUB => HUB CODEX => CODEX)
-- SELECT * FROM codex_data_refresh('alp');

-------------------------------------------------------------
-------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.zz_log (lib_schema character varying,lib_table character varying,lib_champ character varying,typ_log character varying,lib_log character varying,nb_occurence character varying,date_log timestamp,user_log character varying);
CREATE TABLE IF NOT EXISTS public.threecol (col1 varchar, col2 varchar, col3 varchar);
CREATE TABLE IF NOT EXISTS public.twocol (col1 varchar, col2 varchar);

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : codex_admin_init
--- Description : initialise les fonctions codex_to_codex (supprime toutes les fonctions) et initialise certaines tables (zz_log et bilan)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION codex_admin_init() RETURNS void  AS 
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
	WHERE  routine_name LIKE ''codex_%''
	ORDER BY routine_name,ordinal_position
	) as one
	GROUP BY routine_name, specific_name';
	
--- Suppression de ces fonctions
FOR fonction IN EXECUTE cmd
   LOOP EXECUTE 'DROP FUNCTION '||fonction||';';
   END LOOP;

-- Fonctions utilisées par le codex
listFunction = ARRAY['dblink'];
FOREACH fonction IN ARRAY listFunction LOOP
	EXECUTE 'SELECT extname from pg_extension WHERE extname = '''||fonction||''';' INTO exist;
	CASE WHEN exist IS NULL THEN EXECUTE 'CREATE EXTENSION "'||fonction||'";';
	ELSE END CASE;
END LOOP;
END;$BODY$ LANGUAGE plpgsql;

--- Lancé à chaque fois pour réinitialier les fonctions
SELECT * FROM codex_admin_init();


-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction codex_insert
--- Description : Met à jour toutes les données du CODEXE depuis la copie du hub dans le CODEX
--- Sauf partie REUNION
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION codex_insert(version integer = 7) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE jdd threecol%rowtype;
DECLARE cmd varchar;
DECLARE ct integer;
DECLARE list_jdd varchar;
DECLARE test_jdd integer;
BEGIN 
--- log
out.lib_schema := 'hub';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'codex_insert';SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;


--- Suivi des mises à jour
PERFORM codex_data_log();

END; $BODY$ LANGUAGE plpgsql;


-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction codex_insert_reunion
--- Description : Met à jour toutes les données du CODEX depuis la copie du hub dans le codex
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION codex_insert_reunion(version integer = 7) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE jdd threecol%rowtype;
DECLARE cmd varchar;
DECLARE ct integer;
DECLARE list_jdd varchar;
DECLARE test_jdd integer;
BEGIN 
--- log
out.lib_schema := 'hub';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'codex_insert_reunion';SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;

--- Suivi des mises à jour
PERFORM codex_data_log();

END; $BODY$ LANGUAGE plpgsql;

-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction codex_data_log
--- Description : Enregistre la date de mise à jour des données
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION codex_data_log() RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE list_jdd varchar;
DECLARE test_jdd integer;
BEGIN 
--- Variable

--- Commande

-- log
out.lib_schema := 'hub';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'codex_data_log';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
out.lib_log = 'Date enregistrée'; 
PERFORM hub_log ('hub', out); RETURN next out;
END; $BODY$ LANGUAGE plpgsql;

-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction codex_data_refresh
--- Description : Met à jour les données CODEX
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION codex_data_refresh(libSchema varchar, jdd varchar = 'data', version integer = 7) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE connction varchar;
DECLARE listJdd varchar;
DECLARE cmd varchar;
BEGIN 
--- Variable
--connction = 'dbname=hub_fcbn port=5433';
connction = 'dbname=si_flore_national port=5433';
--- Commande
-- 1. ... HUB FCBN => HUB codex - on récupère sur le HUB CODEX les données provenant du schéma du CBN choisi (tables propres) dans le HUB FCBN
SELECT * INTO out FROM hub_truncate('hub','propre'); RETURN next out;
EXECUTE 'SELECT * FROM hub_connect_simple('''||jdd||''', '''||libSchema||''', ''hub'', '''||connction||''');' into out; RETURN next out;

/*problèmes code maille*/
--UPDATE hub.releve_territoire SET cd_geo = '10kmL93'||cd_geo WHERE typ_geo = 'm10' AND cd_geo NOT LIKE '10kmL93%';
--UPDATE hub.releve_territoire SET cd_geo = '5kmL93'||cd_geo WHERE typ_geo = 'm5' AND cd_geo NOT LIKE '5kmL93%';

-- 2. ... (codex) on pousse les données au sein du hub CODEX (suppression + ajout)
/*Suppression*/
--codex_data_drop?

/*Insertion des données dans les tables CODEX*/
CASE WHEN libSchema = 'mas' THEN
	SELECT * INTO out FROM codex_insert_reunion(version); RETURN next out;
ELSE
	SELECT * INTO out FROM codex_insert(version); RETURN next out;
END CASE;


-- log
out.lib_schema := 'hub';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'codex_data_refresh';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;out.lib_log = 'mise à jour OK : '||libSchema; 
PERFORM hub_log ('hub', out); RETURN next out;
END; $BODY$ LANGUAGE plpgsql;
