--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
---- FONCTIONS LOCALES ET GLOBALES POUR LE PARTAGE DE DONNÉES AU SEIN DU RESEAU DES CBN ----
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------

--- Création de la table générale de log
CREATE TABLE IF NOT EXISTS public.zz_log (lib_schema character varying,lib_table character varying,lib_champ character varying,typ_log character varying,lib_log character varying,nb_occurence character varying,date_log date);
--- Pour le passage de version depuis le commit 0ea80726ed753ee9add5811d380f8ed4d285c368
--- ALTER TABLE public.zz_log RENAME "libSchema" TO lib_schema;
--- ALTER TABLE public.zz_log RENAME "libTable" TO lib_table;
--- ALTER TABLE public.zz_log RENAME "libChamp" TO lib_champ;
--- ALTER TABLE public.zz_log RENAME "typLog" TO typ_log;
--- ALTER TABLE public.zz_log RENAME "libLog" TO lib_log;
--- ALTER TABLE public.zz_log RENAME "nbOccurence" TO nb_occurence;
--- ALTER TABLE public.zz_log RENAME "date" TO date_log;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_add 
--- Description : Ajout de données (fonction utilisée par une autre fonction)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Attention listJdd null sur jdd erroné
CREATE OR REPLACE FUNCTION hub_add(schemaSource varchar,schemaDest varchar, tableSource varchar, tableDest varchar,champRef varchar, jdd varchar, typAction1 varchar = 'diff') RETURNS setof zz_log  AS 
$BODY$  
DECLARE out zz_log%rowtype;
DECLARE metasource varchar;
DECLARE listJdd varchar;
DECLARE typJdd varchar;
DECLARE source varchar;
DECLARE destination varchar;
DECLARE compte integer;
DECLARE listeChamp1 varchar;
DECLARE listeChamp2 varchar; 
DECLARE jointure varchar; 
DECLARE flag varchar; 
BEGIN
--Variables
SELECT CASE WHEN substring(tableSource from 0 for 5) = 'temp' THEN 'temp_metadonnees' ELSE 'metadonnees' END INTO metasource;
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN EXECUTE 'SELECT CASE WHEN string_agg(''''''''||cd_jdd||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||cd_jdd||'''''''','','') END FROM "'||schemaSource||'"."'||metasource||'" WHERE typ_jdd = '''||jdd||''';' INTO listJdd;
ELSE listJdd := ''||jdd||'';END CASE;

CASE WHEN champRef = 'cd_jdd_perm' THEN typJdd = 'meta';flag := 1;
WHEN champRef = 'cd_obs_perm' THEN typJdd = 'data';flag := 1;
WHEN champRef = 'cd_ent_perm' THEN typJdd = 'taxa';flag := 1;
ELSE flag := 0;
END CASE;
EXECUTE 'SELECT string_agg(''a."''||cd||''" = b."''||cd||''"'','' AND '') FROM ref.fsd_'||typJdd||' WHERE (tbl_name = '''||tableSource||''' OR tbl_name = '''||tableDest||''') AND unicite = ''Oui''' INTO jointure;
source := '"'||schemaSource||'"."'||tableSource||'"';
destination := '"'||schemaDest||'"."'||tableDest||'"';

--- Output&Log
out.lib_schema := schemaSource; out.lib_table := tableSource; out.lib_champ := '-'; out.typ_log := 'hub_add'; SELECT CURRENT_TIMESTAMP INTO out.date_log;
--- Commande
CASE WHEN typAction1 = 'push_total' THEN --- CAS utilisé pour ajouter en masse.
	EXECUTE 'SELECT string_agg(''z."''||column_name||''"::''||data_type,'','')  FROM information_schema.columns where table_name = '''||tableDest||''' AND table_schema = '''||schemaDest||''' ' INTO listeChamp1;
	EXECUTE 'SELECT string_agg(''"''||column_name||''"'','','')  FROM information_schema.columns where table_name = '''||tableSource||''' AND table_schema = '''||schemaSource||''' ' INTO listeChamp2;
	EXECUTE 'INSERT INTO '||destination||' ('||listeChamp2||') SELECT '||listeChamp1||' FROM '||source||' z WHERE cd_jdd IN ('||listJdd||')';
		out.nb_occurence := 'total'; out.lib_log := 'Jdd complet(s) ajouté(s)'; PERFORM hub_log (schemaSource, out);RETURN NEXT out;

WHEN typAction1 = 'push_diff' THEN --- CAS utilisé pour ajouter les différences
	--- Recherche des concepts (obsevation, jdd ou entite) présent dans la source et absent dans la destination
	EXECUTE 'SELECT count(DISTINCT b."'||champRef||'") FROM '||source||' b LEFT JOIN '||destination||' a ON '||jointure||' WHERE a."'||champRef||'" IS NULL AND b.cd_jdd IN ('||listJdd||')' INTO compte; 
	CASE WHEN (compte > 0) THEN --- Si de nouveau concept sont succeptible d'être ajouté
		EXECUTE 'SELECT string_agg(''z."''||column_name||''"::''||data_type,'','')  FROM information_schema.columns where table_name = '''||tableDest||''' AND table_schema = '''||schemaDest||''' ' INTO listeChamp1;
		EXECUTE 'SELECT string_agg(''"''||column_name||''"'','','')  FROM information_schema.columns where table_name = '''||tableSource||''' AND table_schema = '''||schemaSource||''' ' INTO listeChamp2;
		EXECUTE 'INSERT INTO '||destination||' ('||listeChamp2||') SELECT '||listeChamp1||' FROM '||source||' z LEFT JOIN '||source||' a ON '||jointure||' WHERE a."'||champRef||'" IS NULL AND cd_jdd IN ('||listJdd||')';
		out.nb_occurence := compte||' occurence(s)'; out.lib_log := 'Concept(s) ajouté(s)'; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	ELSE out.nb_occurence := '-'; out.lib_log := 'Aucune différence'; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	END CASE;	

WHEN typAction1 = 'diff' THEN --- CAS utilisé pour analyser les différences
	--- Recherche des concepts (obsevation, jdd ou entite) présent dans la source et absent dans la destination
	EXECUTE 'SELECT count(DISTINCT b."'||champRef||'") FROM '||source||' b LEFT JOIN '||destination||' a ON '||jointure||' WHERE a."'||champRef||'" IS NULL AND b.cd_jdd IN ('||listJdd||')' INTO compte; 
	CASE WHEN (compte > 0) THEN --- Si de nouveau concept sont succeptible d'être ajouté
		out.nb_occurence := compte||' occurence(s)'; out.lib_log := tableSource||' => '||tableDest; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	ELSE out.nb_occurence := '-'; out.lib_log := 'Aucune différence'; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	END CASE;
ELSE out.lib_champ := '-'; out.lib_log := 'ERREUR : sur champ action = '||typAction1; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
END CASE;	
END;$BODY$ LANGUAGE plpgsql;


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
out.lib_schema := libSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_bilan';out.nb_occurence := '-'; SELECT CURRENT_TIMESTAMP INTO out.date_log;
--- Commandes
EXECUTE 'UPDATE public.bilan SET 
	data_nb_releve = (SELECT count(*) FROM "'||libSchema||'".releve),
	data_nb_observation = (SELECT count(*) FROM "'||libSchema||'".observation),
	data_nb_taxon = (SELECT count(DISTINCT cd_ref) FROM "'||libSchema||'".observation),
	taxa_nb_taxon = (SELECT count(*) FROM "'||libSchema||'".entite) 
	WHERE lib_cbn = '''||libSchema||'''
	';
--- Output&Log
out.lib_log = 'bilan réalisé';
PERFORM hub_log (libSchema, out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_clear 
--- Description : Nettoyage des tables (partie temporaires ou propre)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_clear(libSchema varchar, jdd varchar, typPartie varchar = 'temp') RETURNS setof zz_log  AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE flag integer;
DECLARE prefixe varchar;
DECLARE metasource varchar;
DECLARE libTable varchar;
DECLARE listJdd varchar;
BEGIN
--- Variables 
CASE WHEN typPartie = 'temp' THEN flag :=1; prefixe = 'temp_'; WHEN typPartie = 'propre' THEN flag :=1; prefixe = ''; ELSE flag :=0; END CASE;
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN EXECUTE 'SELECT CASE WHEN string_agg(''''''''||cd_jdd||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||cd_jdd||'''''''','','') END FROM "'||libSchema||'"."'||prefixe||'metadonnees" WHERE typ_jdd = '''||jdd||''';' INTO listJdd;
ELSE listJdd := ''||jdd||'';END CASE;
--- Output&Log
out.lib_schema := libSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_clear';out.nb_occurence := '-'; SELECT CURRENT_TIMESTAMP INTO out.date_log;
--- Commandes
CASE WHEN flag = 1 AND listJdd <> '''vide''' THEN
	FOR libTable in EXECUTE 'SELECT table_name FROM information_schema.tables WHERE table_schema = '''||libSchema||''' AND table_name NOT LIKE ''temp_%'' AND table_name NOT LIKE ''zz_%'';'
		LOOP EXECUTE 'DELETE FROM "'||libSchema||'"."'||prefixe||libTable||'" WHERE cd_jdd IN ('||listJdd||');'; 
		END LOOP;
	---log---
	out.lib_log = jdd||' effacé de la partie '||typPartie;
WHEN listJdd = '''vide''' THEN out.lib_log = 'jdd vide '||jdd;
ELSE out.lib_log = 'ERREUR : mauvais typPartie : '||typPartie;
END CASE;
--- Output&Log
PERFORM hub_log (libSchema, out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_clone 
--- Description : Création d'un hub complet
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_clone(libSchema varchar) RETURNS setof zz_log AS 
$BODY$ 
DECLARE out zz_log%rowtype; 
DECLARE flag integer; 
DECLARE typjdd varchar; 
DECLARE cd_table varchar; 
DECLARE list_champ varchar; 
DECLARE list_champ_sans_format varchar; 
DECLARE list_contraint varchar; 
DECLARE schema_lower varchar; 
BEGIN
--- Variable
schema_lower = lower(libSchema);
EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = '''||schema_lower||''';' INTO flag;
--- Commande
CASE WHEN flag = 1 THEN
	out.lib_log := 'Schema '||schema_lower||' existe déjà';
ELSE 
	EXECUTE 'CREATE SCHEMA "'||schema_lower||'";';

	--- META : PARTIE PROPRE 
	FOR typjdd IN SELECT typ_jdd FROM ref.fsd GROUP BY typ_jdd
	LOOP
		FOR cd_table IN EXECUTE 'SELECT cd_table FROM ref.fsd WHERE typ_jdd = '''||typjdd||''' GROUP BY cd_table'
		LOOP
			EXECUTE 'SELECT string_agg(one.cd_champ||'' ''||one.format,'','') FROM (SELECT cd_champ, format FROM ref.fsd WHERE typ_jdd = '''||typjdd||''' AND cd_table = '''||cd_table||''' ORDER BY ordre_champ) as one;' INTO list_champ;
			EXECUTE 'SELECT string_agg(one.cd_champ||'' character varying'','','') FROM (SELECT cd_champ, format FROM ref.fsd WHERE typ_jdd = '''||typjdd||''' AND cd_table = '''||cd_table||''' ORDER BY ordre_champ) as one;' INTO list_champ_sans_format;
			EXECUTE 'SELECT ''CONSTRAINT ''||cd_table||''_pkey PRIMARY KEY (''||string_agg(cd_champ,'','')||'')'' FROM ref.fsd WHERE typ_jdd = '''||typjdd||''' AND cd_table = '''||cd_table||''' AND unicite = ''Oui'' GROUP BY cd_table' INTO list_contraint ;
			EXECUTE 'CREATE TABLE '||schema_lower||'.temp_'||cd_table||' ('||list_champ_sans_format||');';
			EXECUTE 'CREATE TABLE '||schema_lower||'.'||cd_table||' ('||list_champ||','||list_contraint||');';
		END LOOP;
	END LOOP;
	--- LISTE TAXON
	EXECUTE '
	CREATE TABLE "'||schema_lower||'".zz_log_liste_taxon  (cd_ref character varying, nom_valide character varying);
	CREATE TABLE "'||schema_lower||'".zz_log_liste_taxon_et_infra  (cd_ref_demande character varying, nom_valide_demande character varying, cd_ref_cite character varying, nom_complet_cite character varying, rang_cite character varying, cd_taxsup_cite character varying);
	';
	--- LOG
	EXECUTE '
	CREATE TABLE "'||schema_lower||'".zz_log  (lib_schema character varying,lib_table character varying,lib_champ character varying,typ_log character varying,lib_log character varying,nb_occurence character varying,date_log date);
	';
	out.lib_log := 'Schema '||schema_lower||' créé';
END CASE;
--- Output&Log
out.lib_schema := schema_lower;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_clone';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;PERFORM hub_log (schema_lower, out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_del 
--- Description : Suppression de données (fonction utilisée par une autre fonction)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- DROP FUNCTION hub_del(varchar,varchar, varchar, varchar, varchar, varchar, varchar)
CREATE OR REPLACE FUNCTION hub_del(schemaSource varchar,schemaDest varchar, tableSource varchar, tableDest varchar, champRef varchar, jdd varchar, typAction5 varchar = 'diff') RETURNS setof zz_log  AS 
$BODY$  
DECLARE out zz_log%rowtype;
DECLARE metasource varchar;
DECLARE typJdd varchar;
DECLARE listJdd varchar;
DECLARE source varchar;
DECLARE destination varchar;
DECLARE flag integer;
DECLARE compte integer;
DECLARE listeChamp1 varchar;
DECLARE jointure varchar;
BEGIN
--Variable
SELECT CASE WHEN substring(tableSource from 0 for 5) = 'temp' THEN 'temp_metadonnees' ELSE 'metadonnees' END INTO metasource;
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN EXECUTE 'SELECT CASE WHEN string_agg(''''''''||cd_jdd||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||cd_jdd||'''''''','','') END FROM "'||schemaSource||'"."'||metasource||'" WHERE typ_jdd = '''||jdd||''';' INTO listJdd;
ELSE listJdd := ''||jdd||'';END CASE;

CASE WHEN champRef = 'cd_jdd_perm' THEN typJdd = 'meta';flag := 1;
WHEN champRef = 'cd_obs_perm' THEN typJdd = 'data';flag := 1;
WHEN champRef = 'cd_ent_perm' THEN typJdd = 'taxa';flag := 1;
ELSE flag := 0;
END CASE;
source := '"'||schemaSource||'"."'||tableSource||'"';
destination := '"'||schemaDest||'"."'||tableDest||'"';
EXECUTE 'SELECT string_agg(''a."''||cd||''" = b."''||cd||''"'','' AND '') FROM ref.fsd_'||typJdd||' WHERE (tbl_name = '''||tableSource||''' OR tbl_name = '''||tableDest||''') AND unicite = ''Oui''' INTO jointure;
--- Output&Log
out.lib_schema := schemaSource; out.lib_table := tableSource; out.lib_champ := '-';out.typ_log := 'hub_del'; SELECT CURRENT_TIMESTAMP INTO out.date_log;

--- Commande
--- Recherche des concepts (obsevation, jdd ou entite) présent dans la partie propre et présent dans la partie temporaire
EXECUTE 'SELECT count(DISTINCT b."'||champRef||'") FROM '||source||' b JOIN '||destination||' a ON '||jointure||' WHERE b.cd_jdd IN ('||listJdd||')' INTO compte; 
	
CASE WHEN (compte > 0) THEN --- Si de nouveau concept sont succeptible d'être ajouté
	out.nb_occurence := compte||' occurence(s)'; ---log
	CASE WHEN typAction5 = 'push_diff' THEN
		EXECUTE 'SELECT string_agg(''''||'||champRef||'||'''','','') FROM '||source INTO listeChamp1;
		EXECUTE 'DELETE FROM '||destination||' WHERE "'||champRef||'" IN ('||listeChamp1||')';
		out.nb_occurence := compte||' occurence(s)';out.lib_log := 'Concepts supprimés'; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	WHEN typAction5 = 'diff' THEN
		out.nb_occurence := compte||' occurence(s)';out.lib_log := 'Concepts à supprimer'; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	ELSE out.nb_occurence := compte||' occurence(s)'; out.lib_log := 'ERREUR : sur champ action = '||typAction5; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	END CASE;
ELSE out.lib_log := 'Aucune différence';out.nb_occurence := '-'; PERFORM hub_log (schemaSource, out); RETURN NEXT out;
END CASE;	
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_diff 
--- Description : Analyse des différences entre une source et une cible
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_diff(libSchema varchar, jdd varchar,typAction2 varchar = 'add',mode integer = 1) RETURNS setof zz_log  AS 
$BODY$ 
DECLARE out zz_log%rowtype;
DECLARE flag integer;
DECLARE typJdd varchar;
DECLARE libTable varchar;
DECLARE tableSource varchar;
DECLARE tableDest varchar;
DECLARE schemaSource varchar;
DECLARE schemaDest varchar;
DECLARE champRef varchar;
DECLARE tableRef varchar;
DECLARE nothing varchar;
BEGIN
--- Variables
CASE WHEN jdd = 'data' THEN 
	champRef = 'cd_obs_perm'; tableRef = 'observation'; flag := 1;
WHEN jdd = 'taxa' THEN 
	champRef = 'cd_ent_perm';	tableRef = 'entite'; flag := 1;
ELSE 
	EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'".temp_metadonnees WHERE cd_jdd = '''||jdd||''';' INTO typJdd;
	CASE WHEN typJdd = 'data' THEN champRef = 'cd_obs_perm'; tableRef = 'observation'; flag := 1;
	WHEN typJdd = 'taxa' THEN champRef = 'cd_ent_perm';	tableRef = 'entite'; flag := 1;
	ELSE flag := 0;
	END CASE;
END CASE;
--- mode 1 = intra Shema / mode 2 = entre shema et agregation
CASE WHEN mode = 1 THEN schemaSource :=libSchema; schemaDest :=libSchema; WHEN mode = 2 THEN schemaSource :=libSchema; schemaDest :='agregation'; ELSE flag :=0; END CASE;
--- Commandes
CASE WHEN typAction2 = 'add' AND flag = 1 THEN
	--- Metadonnees
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE ''metadonnees%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM  hub_add(schemaSource,schemaDest, tableSource, tableDest ,'cd_jdd_perm' , jdd ,'diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
		SELECT * INTO out FROM  hub_update(schemaSource,schemaDest, tableSource, tableDest ,'cd_jdd_perm', jdd,'diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
		SELECT * INTO out FROM  hub_add(schemaDest,schemaSource, tableDest, tableSource ,'cd_jdd_perm', jdd,'diff'); --- sens inverse (champ présent dans le propre et absent dans le temporaire)
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
	--- Données
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE '''||tableRef||'%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM  hub_add(schemaSource,schemaDest, tableSource, tableDest ,champRef, jdd,'diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
		SELECT * INTO out FROM  hub_update(schemaSource,schemaDest, tableSource, tableDest ,champRef, jdd,'diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
		SELECT * INTO out FROM  hub_add(schemaDest,schemaSource, tableDest, tableSource ,champRef, jdd,'diff'); --- sens inverse (champ présent dans le propre et absent dans le temporaire)
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
WHEN typAction2 = 'del' AND flag = 1 THEN
	--- Metadonnees
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE ''metadonnees%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM  hub_del(schemaSource,schemaDest, tableSource, tableDest ,'cd_jdd_perm' , jdd ,'diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE '''||tableRef||'%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM  hub_del(schemaSource,schemaDest, tableSource, tableDest ,champRef, jdd,'diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
ELSE out.lib_table := libTable; out.lib_log := jdd||' n''est pas un jeu de données valide'; out.nb_occurence := '-'; PERFORM hub_log (libSchema, out);RETURN NEXT out;
END CASE;

END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_drop 
--- Description : Supprimer un hub dans sa totalité
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_drop(libSchema varchar) RETURNS setof zz_log AS 
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
out.lib_schema := libSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_drop';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;PERFORM hub_log ('public', out);RETURN next out;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_export 
--- Description : Exporter les données depuis un hub
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_export(libSchema varchar,jdd varchar,path varchar,format varchar = 'fcbn') RETURNS setof zz_log  AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE libTable varchar;
DECLARE typJdd varchar; 
DECLARE listJdd varchar; 
BEGIN
--- Variables Jdd
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN 	
	typJdd := jdd;
	EXECUTE 'SELECT CASE WHEN string_agg(''''''''||cd_jdd||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||cd_jdd||'''''''','','') END FROM "'||libSchema||'"."temp_metadonnees" WHERE typ_jdd = '''||jdd||''';' INTO listJdd;
WHEN jdd = 'listtaxon' THEN 
	libTable = 'zz_log_liste_taxon';
WHEN jdd = 'listtaxoninfra' THEN 
	libTable = 'zz_log_liste_taxon_et_infra';
ELSE
	EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'".temp_metadonnees WHERE cd_jdd = '''||jdd||''';' INTO typJdd; 
	listJdd := ''||jdd||'';
END CASE;
--- Output&Log
out.lib_schema := libSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_export';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;
--- Commandes
CASE WHEN format = 'fcbn' THEN
	FOR libTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_'||typJdd
		LOOP EXECUTE 'COPY (SELECT * FROM  "'||libSchema||'"."'||libTable||'" WHERE cd_jdd IN ('||listJdd||')) TO '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';'; END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_meta'
		LOOP EXECUTE 'COPY (SELECT * FROM  "'||libSchema||'"."'||libTable||'" WHERE cd_jdd IN ('||listJdd||')) TO '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';'; END LOOP;
	out.lib_log :=  jdd||'exporté au format '||format;
WHEN format = 'sinp' THEN
	out.lib_log :=  'format SINP à implémenter';
WHEN format = 'taxon' THEN
	EXECUTE 'COPY (SELECT * FROM  "'||libSchema||'"."'||libTable||'") TO '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';';
	out.lib_log :=  libTable||' exporté ';
ELSE out.lib_log :=  'format non implémenté : '||format;
END CASE;
PERFORM hub_log (libSchema, out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_help 
--- Description : Création de l'aide et Accéder à la description d'un fonction
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_help(libFonction varchar = 'all') RETURNS setof varchar AS 
$BODY$
DECLARE out varchar;
DECLARE var varchar;
DECLARE lesvariables varchar;
DECLARE flag integer;
DECLARE testFonction varchar;
BEGIN
--- Variable
flag := 0;
FOR testFonction IN EXECUTE 'SELECT nom FROM ref.help'
LOOP 
	CASE WHEN testFonction = libFonction THEN flag := 1; ELSE EXECUTE 'SELECT 1;'; END CASE; 
END LOOP;
--- Commande
CASE WHEN libFonction = 'all' THEN
	out := '- Pour accéder à la description d''une fonction : ';RETURN next out;
	out := '   SELECT * FROM hub_help(''fonction'');';RETURN next out;
	out := '- Pour utiliser une fonction : ';RETURN next out;
	out := '  SELECT * FROM fonction(''variables'');';RETURN next out;
	FOR testFonction IN EXECUTE 'SELECT nom FROM ref.help WHERE objet = ''fonction'''
	LOOP
		EXECUTE 'SELECT string_agg(champ,'','') FROM (SELECT pos, champ FROM ref.help_var WHERE nom = '''||testFonction||''' ORDER BY pos) as one;' INTO lesvariables;
		out := 'SELECT * FROM '||testFonction||'('||lesvariables||')';RETURN next out; 
	END LOOP;
WHEN flag = 1 THEN
	out := '-------------------------'; RETURN next out; 
	out := 'Nom de la Fonction = '||libFonction;RETURN next out; 
	EXECUTE 'SELECT ''- Type : ''||type FROM ref.help WHERE nom = '''||libFonction||''';'INTO out;RETURN next out; 
	EXECUTE 'SELECT ''- Libellé : ''||libelle FROM ref.help WHERE nom = '''||libFonction||''';'INTO out;RETURN next out; 
	EXECUTE 'SELECT ''- Description : ''||description FROM ref.help WHERE nom = '''||libFonction||''';'INTO out;RETURN next out; 
	EXECUTE 'SELECT ''- Etat de la fonction : ''||etat FROM ref.help WHERE nom = '''||libFonction||''';'INTO out;RETURN next out;
	EXECUTE 'SELECT ''- Amélioration à prevoir : ''||amelioration FROM ref.help WHERE nom = '''||libFonction||''';'INTO out;RETURN next out;
	out := '-------------------------'; RETURN next out; 
	out := 'Liste des variables :';RETURN next out;
	FOR var IN EXECUTE 'SELECT lib||valeur||defaut FROM
		(SELECT a.champ, pos, '' o ''||a.champ||'' : ''||z.libelle as lib FROM ref.help_var a JOIN ref.help z ON a.champ = z.nom WHERE a.nom = '''||libFonction||''') as one
		LEFT JOIN (SELECT a.champ, CASE WHEN a.valeur_possible <> ''-'' THEN '' / Valeurs admises : ''||a.valeur_possible ELSE '''' END as valeur FROM ref.help_var a WHERE a.nom = '''||libFonction||''') as two ON one.champ = two.champ
		LEFT JOIN (SELECT a.champ, CASE WHEN a.valeur_defaut <> ''-'' THEN '' / Valeurs par défaut : ''||a.valeur_defaut ELSE '''' END as defaut FROM ref.help_var a WHERE a.nom = '''||libFonction||''') as three ON one.champ = three.champ
		ORDER BY pos;'
		LOOP --- variables d'entrées
		RETURN next var;
		END LOOP;
ELSE out := 'Fonction inexistante';RETURN next out;
END CASE;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_idPerm 
--- Description : Production des identifiants uniques
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_idPerm(libSchema varchar, nomDomaine varchar, jdd varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE flag varchar;
DECLARE typJdd varchar;
DECLARE listJdd varchar;
DECLARE champMere varchar;
DECLARE champRef varchar;
DECLARE tableRef varchar;
DECLARE listTable varchar;
DECLARE listPerm varchar;
BEGIN
--- Variables
CASE WHEN jdd = 'data' THEN 
	EXECUTE 'SELECT CASE WHEN string_agg(''''''''||cd_jdd||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||cd_jdd||'''''''','','') END FROM "'||schemaSource||'"."'||metasource||'" WHERE typ_jdd = '''||jdd||''';' INTO listJdd;
	typJdd := jdd; champMere = 'cd_ent_mere';	champRef = 'cd_ent_perm';	tableRef = 'entite'; flag :=1;
WHEN  jdd = 'taxa' THEN
	EXECUTE 'SELECT CASE WHEN string_agg(''''''''||cd_jdd||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||cd_jdd||'''''''','','') END FROM "'||schemaSource||'"."'||metasource||'" WHERE typ_jdd = '''||jdd||''';' INTO listJdd;
	typJdd := jdd;	champMere = 'cd_obs_mere';	champRef = 'cd_obs_perm';	tableRef = 'observation'; flag :=1;
ELSE 
	listJdd := ''||jdd||'';
	EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'".temp_metadonnees WHERE cd_jdd = '''||jdd||''';' INTO typJdd;
	CASE WHEN typJdd = 'taxa' THEN 
		champMere = 'cd_ent_mere';	champRef = 'cd_ent_perm';	tableRef = 'entite'; flag :=1;
	WHEN typJdd = 'data' THEN 
		champMere = 'cd_obs_mere';	champRef = 'cd_obs_perm';	tableRef = 'observation';flag :=1;
	ELSE flag :=0;
	END CASE;
END CASE;

--- Output
out.lib_schema := libSchema;out.lib_table := tableRef;out.lib_champ := champRef;out.typ_log := 'hub_idPerm';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;
--- Commandes
CASE WHEN flag =1 THEN
--- Metadonnees
	FOR listPerm IN EXECUTE 'SELECT DISTINCT cd_jdd FROM "'||libSchema||'"."temp_metadonnees" WHERE cd_jdd IN ('||listJdd||');' --- Production de l'idPermanent
		LOOP EXECUTE 'UPDATE "'||libSchema||'"."temp_metadonnees" SET (cd_jdd_perm) = ('''||nomdomaine||'/cd_jdd_perm/''||(SELECT uuid_generate_v4())) WHERE cd_jdd = '''||listPerm||''' AND cd_jdd IN ('||listJdd||') AND cd_jdd_perm IS NULL;';
		out.lib_log := 'OK';out.lib_table := '-'; PERFORM hub_log (libSchema, out); RETURN next out;
		END LOOP;	
	FOR listTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_'||typJdd||' WHERE tbl_name <> ''metadonnees'''  --- Peuplement du nouvel idPermanent dans les autres tables
		LOOP EXECUTE 'UPDATE "'||libSchema||'"."temp_'||listTable||'" ot SET cd_jdd_perm = o.cd_jdd_perm FROM "'||libSchema||'"."temp_metadonnees" o WHERE o.cd_jdd = ot.cd_jdd AND o.cd_jdd = ot.cd_jdd AND ot.cd_jdd_perm IS NULL;';
		out.lib_log := 'OK';out.lib_table := listTable; PERFORM hub_log (libSchema, out);RETURN next out;
		END LOOP;
--- Donnees
	FOR listPerm IN EXECUTE 'SELECT DISTINCT "'||champMere||'" FROM "'||libSchema||'".temp_'||tableRef||' WHERE cd_jdd IN ('||listJdd||');' --- Production de l'idPermanent
		LOOP EXECUTE 'UPDATE "'||libSchema||'"."temp_'||tableRef||'" SET ("'||champRef||'") = ('''||nomdomaine||'/'||champRef||'/''||(SELECT uuid_generate_v4())) WHERE "'||champMere||'" = '''||listPerm||''' AND cd_jdd IN ('||listJdd||') AND "'||champRef||'" IS NULL;';
		out.lib_log := 'Identifiant permanent produit';out.lib_table := '-'; PERFORM hub_log (libSchema, out); RETURN next out;
		END LOOP;	
	FOR listTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_'||typJdd||' WHERE tbl_name <> '''||tableRef||''''  --- Peuplement du nouvel idPermanent dans les autres tables
		LOOP EXECUTE 'UPDATE "'||libSchema||'"."temp_'||listTable||'" ot SET "'||champRef||'" = o."'||champRef||'" FROM "'||libSchema||'"."temp_'||tableRef||'" o WHERE o.cd_jdd = ot.cd_jdd AND o."'||champMere||'" = ot."'||champMere||' AND ot."'||champRef||'" IS NULL";';
		out.lib_log := 'Identifiant permanent produit';out.lib_table := listTable; PERFORM hub_log (libSchema, out);RETURN next out;
		END LOOP;
ELSE out.lib_log := 'ERREUR : Mauvais JDD';out.lib_table := '-'; PERFORM hub_log (libSchema, out); RETURN next out;
END CASE;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_import 
--- Description : Importer des données (fichiers CSV) dans un hub
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_import(libSchema varchar, jdd varchar, path varchar, files varchar = '') RETURNS setof zz_log AS 
$BODY$
DECLARE libTable varchar;
DECLARE out zz_log%rowtype;
BEGIN
--- Commande
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN 
	FOR libTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_'||jdd||';'
		LOOP EXECUTE 'COPY "'||libSchema||'".temp_'||libTable||' FROM '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';'; END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_meta;'
		LOOP EXECUTE 'COPY "'||libSchema||'".temp_'||libTable||' FROM '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';'; END LOOP;
	out.lib_log := jdd||' importé depuis '||path;
WHEN jdd = 'listTaxon' AND files <> '' THEN 
	EXECUTE 'TRUNCATE TABLE "'||libSchema||'".zz_log_liste_taxon;TRUNCATE TABLE "'||libSchema||'".zz_log_liste_taxon_et_infra;';
	EXECUTE 'COPY "'||libSchema||'".zz_log_liste_taxon FROM '''||path||files||''' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';';
	out.lib_log := jdd||' importé depuis '||path;
WHEN jdd = 'listTaxon' AND files = '' THEN out.lib_log := 'Paramètre "files" non spécifié';
ELSE out.lib_log := 'Problème identifié dans le jdd (ni data, ni taxa,ni meta)'; END CASE;

--- Output&Log
out.lib_schema := libSchema;out.lib_champ := '-';out.lib_table := '-';out.typ_log := 'hub_import';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;PERFORM hub_log (libSchema, out);RETURN next out;
END; $BODY$  LANGUAGE plpgsql;



---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_txinfra 
--- Description : Générer une table avec les taxon infra depuis la table zz_log_liste_taxon
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_txinfra(libSchema varchar) RETURNS setof zz_log AS 
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
		FROM ref.taxref_v5 t1
		WHERE t1.cd_nom = '''||i||'''
		UNION
		SELECT t2.cd_nom, t2.nom_complet, t2.cd_taxsup, t2.rang
		FROM ref.taxref_v5 t2
		JOIN hierarchie h ON t2.cd_taxsup = h.cd_nom
		) SELECT * FROM hierarchie) as foo';
	end loop;
EXECUTE 'update  "'||libSchema||'".zz_log_liste_taxon_et_infra set nom_valide_demande = nom_valide from "'||libSchema||'".zz_log_liste_taxon where zz_log_liste_taxon_et_infra.cd_ref_demande= zz_log_liste_taxon.cd_ref ' ;
out.lib_log := 'Liste de sous taxons générée';

--- Output&Log
out.lib_schema := libSchema;out.lib_champ := '-';out.lib_table := 'zz_log_liste_taxon_et_infra';out.typ_log := 'hub_txinfra';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;PERFORM hub_log (libSchema, out);RETURN next out;
END; $BODY$  LANGUAGE plpgsql;



---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_install 
--- Description : Installe le hub en local (concataine la construction d'un hub et l'installation des référentiels)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_install (libSchema varchar, path varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
BEGIN
--- CREATE TABLE IF NOT EXISTS public.zz_log  (lib_schema character varying,lib_table character varying,lib_champ character varying,typ_log character varying,lib_log character varying,nb_occurence character varying,date_log date);
SELECT * INTO out FROM hub_ref('create',path);PERFORM hub_log ('public', out);RETURN NEXT out;
SELECT * INTO out FROM hub_clone(libSchema);PERFORM hub_log (libSchema, out);RETURN NEXT out;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_pull 
--- Description : Récupération d'un jeu de données depuis la partie propre vers la partie temporaire
--- Variables :
--- o libSchema = Nom du schema
--- o jdd = jeu de donnée (code du jeu ou 'data' ou 'taxa')
--- o mode = mode d'utilisation - valeur possibles : '2' = inter-schema et '1'(par défaut) = intra-schema
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_pull(libSchema varchar,jdd varchar, mode integer = 1) RETURNS setof zz_log AS 
$BODY$ 
DECLARE out zz_log%rowtype; 
DECLARE flag integer; 
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
CASE WHEN jdd = 'data' THEN champRef = 'cd_obs_perm'; tableRef = 'observation'; flag := 1;
	WHEN jdd = 'taxa' THEN champRef = 'cd_ent_perm';	tableRef = 'entite'; flag := 1;
	ELSE EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'".temp_metadonnees WHERE cd_jdd = '''||jdd||''';' INTO typJdd;
		CASE WHEN typJdd = 'data' THEN champRef = 'cd_obs_perm'; tableRef = 'observation'; flag := 1;
			WHEN typJdd = 'taxa' THEN champRef = 'cd_ent_perm';	tableRef = 'entite'; flag := 1;
			ELSE flag := 0;
		END CASE;
	END CASE;
--- mode 1 = intra Shema / mode 2 = entre schema et agregation
CASE WHEN mode = 1 THEN schemaSource :=libSchema; schemaDest :=libSchema; WHEN mode = 2 THEN schemaSource :=libSchema; schemaDest :='agregation'; ELSE flag :=0; END CASE;

--- Commandes
--- Remplacement total (NB : equivalent au push 'replace' mais dans l'autre sens)
CASE WHEN flag = 1 THEN
	SELECT * INTO out FROM hub_clear(libSchema, jdd, 'temp'); return next out;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE ''metadonnees%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP
		CASE WHEN mode = 1 THEN tableSource := libTable; tableDest := 'temp_'||libTable; WHEN mode = 2 THEN tableSource := 'temp_'||libTable; tableDest := libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest ,'cd_jdd_perm', jdd, 'push_total'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE '''||tableRef||'%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := libTable; tableDest := 'temp_'||libTable; WHEN mode = 2 THEN tableSource := 'temp_'||libTable; tableDest := libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest , champRef, jdd, 'push_total'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
ELSE ---Log
	out.lib_schema := libSchema; out.lib_champ := '-'; out.typ_log := 'hub_pull';SELECT CURRENT_TIMESTAMP INTO out.date_log; out.lib_log := 'ERREUR : sur champ jdd = '||jdd; PERFORM hub_log (libSchema, out);RETURN NEXT out;
END CASE;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_push 
--- Description : Mise à jour des données (on pousse les données)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_push(libSchema varchar,jdd varchar, typAction3 varchar = 'replace', mode integer = 1) RETURNS setof zz_log AS 
$BODY$ 
DECLARE out zz_log%rowtype; 
DECLARE flag integer; 
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
CASE WHEN jdd = 'data' THEN champRef = 'cd_obs_perm'; tableRef = 'observation'; flag := 1;
	WHEN jdd = 'taxa' THEN champRef = 'cd_ent_perm';	tableRef = 'entite'; flag := 1;
	ELSE EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'".temp_metadonnees WHERE cd_jdd = '''||jdd||''';' INTO typJdd;
		CASE WHEN typJdd = 'data' THEN champRef = 'cd_obs_perm'; tableRef = 'observation'; flag := 1;
			WHEN typJdd = 'taxa' THEN champRef = 'cd_ent_perm';	tableRef = 'entite'; flag := 1;
			ELSE flag := 0;
		END CASE;
	END CASE;
--- mode 1 = intra Shema / mode 2 = entre shema et agregation
CASE WHEN mode = 1 THEN schemaSource :=libSchema; schemaDest :=libSchema; WHEN mode = 2 THEN schemaSource :=libSchema; schemaDest :='agregation'; ELSE flag :=0; END CASE;

--- Commandes
--- Remplacement total
CASE WHEN typAction3 = 'replace' AND flag = 1 THEN
	SELECT * INTO out FROM hub_clear(libSchema, jdd, 'propre'); return next out;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE ''metadonnees%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest ,'cd_jdd_perm', jdd, 'push_total'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE '''||tableRef||'%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest , champRef, jdd, 'push_total'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;

--- Ajout de la différence
WHEN typAction3 = 'add' AND flag = 1 THEN
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE ''metadonnees%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest ,'cd_jdd_perm', jdd, 'push_diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
		SELECT * INTO out FROM hub_update(schemaSource,schemaDest, tableSource, tableDest ,'cd_jdd_perm', jdd, 'push_diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE '''||tableRef||'%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest ,champRef, jdd, 'push_diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
		SELECT * INTO out FROM hub_update(schemaSource,schemaDest, tableSource, tableDest ,champRef, jdd, 'push_diff');
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;

--- Suppression de l'existant de la partie temporaire
WHEN typAction3 = 'del' AND flag = 1 THEN
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE ''metadonnees%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_del(schemaSource,schemaDest, tableSource, tableDest ,'cd_jdd_perm', jdd, 'push_diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE '''||tableRef||'%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_del(schemaSource,schemaDest, tableSource, tableDest ,champRef, jdd, 'push_diff'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
ELSE ---Log
	out.lib_schema := libSchema; out.lib_champ := '-'; out.typ_log := 'hub_push';SELECT CURRENT_TIMESTAMP INTO out.date_log; out.lib_log := 'ERREUR : sur champ action = '||jdd; PERFORM hub_log (libSchema, out);RETURN NEXT out;
END CASE;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_ref 
--- Description : Création des référentiels
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_ref(typAction4 varchar, path varchar = '/home/hub/00_ref/') RETURNS setof zz_log AS 
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
out.lib_schema := '-';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_ref';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;
---Variables
DROP TABLE IF  EXISTS public.ref_meta;CREATE TABLE public.ref_meta(id serial NOT NULL, nom_ref varchar, typ varchar, ordre integer, libelle varchar, format varchar, CONSTRAINT ref_meta_pk PRIMARY KEY(id));
EXECUTE 'COPY public.ref_meta (nom_ref, typ, ordre, libelle, format) FROM '''||path||'00_meta.csv'' HEADER CSV ENCODING ''UTF8'' DELIMITER '';'';';

--- Commandes 
CASE WHEN typAction4 = 'drop' THEN	--- Suppression
	EXECUTE 'SELECT DISTINCT 1 FROM information_schema.schemata WHERE schema_name = ''ref''' INTO flag1;
	CASE WHEN flag1 = 1 THEN DROP SCHEMA IF EXISTS ref CASCADE; out.lib_log := 'Shema ref supprimé';RETURN next out;
	ELSE out.lib_log := 'Schéma ref inexistant';RETURN next out;END CASE;
WHEN typAction4 = 'delete' THEN	--- Suppression
	EXECUTE 'SELECT DISTINCT 1 FROM information_schema.schemata WHERE schema_name = ''ref''' INTO flag1;
	CASE WHEN flag1 = 1 THEN
		FOR libTable IN EXECUTE 'SELECT nom_ref FROM public.ref_meta GROUP BY nom_ref ORDER BY nom_ref'
		LOOP EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = ''ref'' AND tablename = '''||libTable||''';' INTO flag2;
		CASE WHEN flag2 = 1 THEN 
			EXECUTE 'DROP TABLE ref."'||libTable||'" CASCADE';  out.lib_log := 'Table '||libTable||' supprimée';RETURN next out;
		ELSE out.lib_log := 'Table '||libTable||' inexistante';
		END CASE;
		END LOOP;
	ELSE out.lib_log := 'Schéma ref inexistant';RETURN next out;END CASE;
WHEN typAction4 = 'create' THEN	--- Creation
	EXECUTE 'SELECT DISTINCT 1 FROM information_schema.schemata WHERE schema_name =  ''ref''' INTO flag1;
	CASE WHEN flag1 = 1 THEN out.lib_log := 'Schema ref déjà créés';RETURN next out;ELSE CREATE SCHEMA "ref"; out.lib_log := 'Schéma ref créés';RETURN next out;END CASE;
	--- Tables
	FOR libTable IN EXECUTE 'SELECT nom_ref FROM public.ref_meta GROUP BY nom_ref  ORDER BY nom_ref'
		LOOP EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = ''ref'' AND tablename = '''||libTable||''';' INTO flag2;
		CASE WHEN flag2 = 1 THEN 
			out.lib_log := libTable||' a déjà été créée' ;RETURN next out;
		ELSE EXECUTE 'SELECT ''(''||champs||'',''||contrainte||'')''
			FROM (SELECT nom_ref, string_agg(libelle||'' ''||format,'','') as champs FROM public.ref_meta WHERE nom_ref = '''||libTable||''' AND typ = ''champ'' GROUP BY nom_ref) as one
			JOIN (SELECT nom_ref, ''CONSTRAINT ''||nom_ref||''_pk PRIMARY KEY (''||libelle||'')'' as contrainte FROM public.ref_meta WHERE nom_ref = '''||libTable||''' AND typ = ''cle_primaire'') as two ON one.nom_ref = two.nom_ref
			' INTO structure;
			EXECUTE 'SELECT CASE WHEN libelle = ''virgule'' THEN '','' WHEN libelle = ''tab'' THEN ''\t'' WHEN libelle = ''point_virgule'' THEN '';'' ELSE '';'' END as delimiter FROM public.ref_meta WHERE nom_ref = '''||libTable||''' AND typ = ''delimiter''' INTO delimitr;
			EXECUTE 'CREATE TABLE ref.'||libTable||' '||structure||';'; out.lib_log := libTable||' créée';RETURN next out;
			EXECUTE 'COPY ref.'||libTable||' FROM '''||path||'ref_'||libTable||'.csv'' HEADER CSV DELIMITER E'''||delimitr||''' ENCODING ''UTF8'';';
		out.lib_log := libTable||' : données importées';RETURN next out;
		END CASE;
		END LOOP;
WHEN typAction4 = 'update' THEN	--- Mise à jour
	EXECUTE 'SELECT DISTINCT 1 FROM information_schema.schemata WHERE schema_name =  ''ref''' INTO flag1;
	CASE WHEN flag1 = 1 THEN out.lib_log := 'Schema ref déjà créés';RETURN next out;ELSE CREATE SCHEMA "ref"; out.lib_log := 'Schéma ref créés';RETURN next out;END CASE;
	FOR libTable IN EXECUTE 'SELECT nom_ref FROM public.ref_meta GROUP BY nom_ref'
		LOOP 
		EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = ''ref'' AND tablename = '''||libTable||''';' INTO flag2;
		EXECUTE 'SELECT CASE WHEN libelle = ''virgule'' THEN '','' WHEN libelle = ''tab'' THEN ''\t'' WHEN libelle = ''point_virgule'' THEN '';'' ELSE '';'' END as delimiter FROM public.ref_meta WHERE nom_ref = '''||libTable||''' AND typ = ''delimiter''' INTO delimitr;
		CASE WHEN flag2 = 1 THEN
			EXECUTE 'TRUNCATE ref.'||libTable;
			EXECUTE 'COPY ref.'||libTable||' FROM '''||path||'ref_'||libTable||'.csv'' HEADER CSV DELIMITER E'''||delimitr||''' ENCODING ''UTF8'';';
			out.lib_log := 'Mise à jour de la table '||libTable;RETURN next out;
		ELSE out.lib_log := 'Les tables doivent être créée auparavant : SELECT * FROM hub_ref(''create'',path)';RETURN next out;
		END CASE;
	END LOOP;
ELSE out.lib_log := 'Action non reconnue';RETURN next out;
END CASE;
--- DROP TABLE public.ref_meta;
--- Log
PERFORM hub_log ('public', out); 
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_update 
--- Description : Mise à jour de données (fonction utilisée par une autre fonction)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_update(schemaSource varchar,schemaDest varchar, tableSource varchar, tableDest varchar, champRef varchar, jdd varchar, typAction1 varchar = 'diff') RETURNS setof zz_log  AS 
$BODY$  
DECLARE out zz_log%rowtype; 
DECLARE metasource varchar; 
DECLARE listJdd varchar; 
DECLARE typJdd varchar; 
DECLARE source varchar; 
DECLARE destination varchar; 
DECLARE flag integer; 
DECLARE compte integer;
DECLARE listeChamp varchar;
DECLARE val varchar;
DECLARE wheres varchar;
DECLARE jointure varchar;
BEGIN
--Variable
SELECT CASE WHEN substring(tableSource from 0 for 5) = 'temp' THEN 'temp_metadonnees' ELSE 'metadonnees' END INTO metasource;
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN EXECUTE 'SELECT CASE WHEN string_agg(''''''''||cd_jdd||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||cd_jdd||'''''''','','') END FROM "'||schemaSource||'"."'||metasource||'" WHERE typ_jdd = '''||jdd||''';' INTO listJdd;
ELSE listJdd := ''||jdd||'';END CASE;

CASE WHEN champRef = 'cd_jdd_perm' THEN typJdd = 'meta';flag := 1;
WHEN champRef = 'cd_obs_perm' THEN typJdd = 'data';flag := 1;
WHEN champRef = 'cd_ent_perm' THEN typJdd = 'taxa';flag := 1;
ELSE flag := 0;
END CASE;
EXECUTE 'SELECT string_agg(''a."''||cd||''" = b."''||cd||''"'','' AND '') FROM ref.fsd_'||typJdd||' WHERE (tbl_name = '''||tableSource||''' OR tbl_name = '''||tableDest||''') AND unicite = ''Oui''' INTO jointure;
source := '"'||schemaSource||'"."'||tableSource||'"';
destination := '"'||schemaDest||'"."'||tableDest||'"';
--- Output
out.lib_schema := schemaSource; out.lib_table := tableSource; out.lib_champ := '-'; out.typ_log := 'hub_update';SELECT CURRENT_TIMESTAMP INTO out.date_log;
--- Commande
EXECUTE 'SELECT string_agg(''"''||column_name||''" = b."''||column_name||''"::''||data_type,'','')  FROM information_schema.columns where table_name = '''||tableDest||''' AND table_schema = '''||schemaDest||'''' INTO listeChamp;
EXECUTE 'SELECT string_agg(''a."''||column_name||''"::varchar <> b."''||column_name||''"::varchar'','' OR '')  FROM information_schema.columns where table_name = '''||tableSource||''' AND table_schema = '''||schemaSource||'''' INTO wheres;
EXECUTE 'SELECT count(DISTINCT a."'||champRef||'") FROM '||source||' a JOIN '||destination||' b ON '||jointure||' WHERE a.cd_jdd IN ('||listJdd||') AND ('||wheres||');' INTO compte;

CASE WHEN (compte > 0) AND flag = 1 THEN
	CASE WHEN typAction1 = 'push_diff' THEN
		EXECUTE 'SELECT string_agg(''''''''||b."'||champRef||'"||'''''''','','') FROM '||source||' a JOIN '||destination||' b ON '||jointure||' WHERE a.cd_jdd IN ('||listJdd||') AND ('||wheres||');' INTO val;
		EXECUTE 'UPDATE '||destination||' a SET '||listeChamp||' FROM (SELECT * FROM '||source||') b WHERE a."'||champRef||'" IN ('||val||')';
		out.lib_table := tableSource; out.lib_log := 'Concept(s) modifié(s)'; out.nb_occurence := compte||' occurence(s)';PERFORM hub_log (schemaSource, out); return next out;
	WHEN typAction1 = 'diff' THEN
		out.lib_log := 'Concept(s) à modifier'; out.nb_occurence := compte||' occurence(s)';PERFORM hub_log (schemaSource, out); return next out;
	ELSE out.lib_log := 'ERREUR : sur champ action = '||typAction1; out.nb_occurence := compte||' occurence(s)';PERFORM hub_log (schemaSource, out); return next out;
	END CASE;
ELSE out.lib_log := 'Aucune différence'; out.nb_occurence := '-';PERFORM hub_log (schemaSource, out); return next out;
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
DECLARE typJdd varchar;
DECLARE libTable varchar;
DECLARE libChamp varchar;
DECLARE typChamp varchar;
DECLARE val varchar;
DECLARE flag integer;
DECLARE compte integer;
BEGIN
--- Output
out.lib_schema := libSchema;out.typ_log := 'hub_verif';SELECT CURRENT_TIMESTAMP INTO out.date_log;
--- Variables
CASE WHEN jdd = 'data' OR jdd = 'taxa' OR jdd = 'meta' THEN
	typJdd := Jdd;
	---
ELSE EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'"."temp_metadonnees" WHERE cd_jdd = '''||jdd||'''' INTO typJdd;
	---
END CASE;
out.lib_log = '';

--- Test concernant l'obligation
CASE WHEN (typVerif = 'obligation' OR typVerif = 'all') THEN
FOR libTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_'||typJdd||' WHERE obligation = ''Oui'''
LOOP		
	FOR libChamp in EXECUTE 'SELECT cd FROM ref.fsd_'||typJdd||' WHERE tbl_name = '''||libTable||''' AND obligation = ''Oui'''
	LOOP		
		compte := 0;
		EXECUTE 'SELECT count(*) FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" IS NULL' INTO compte;
		CASE WHEN (compte > 0) THEN
			--- log
			out.lib_table := libTable; out.lib_champ := libChamp;out.lib_log := 'Champ obligatoire non renseigné => SELECT * FROM hub_verif_plus('''||libSchema||''','''||libTable||''','''||libChamp||''',''obligation'');'; out.nb_occurence := compte||' occurence(s)'; return next out;
			out.lib_log := 'Valeur(s) non listée(s)';PERFORM hub_log (libSchema, out);
		ELSE --- rien
		END CASE;
	END LOOP;
END LOOP;
ELSE --- rien
END CASE;

--- Test concernant le typage des champs
CASE WHEN (typVerif = 'type' OR typVerif = 'all') THEN
FOR libTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_'||typJdd||';'
	LOOP
	FOR libChamp in EXECUTE 'SELECT cd FROM ref.fsd_'||typJdd||' WHERE tbl_name = '''||libTable||''';'
	LOOP	
		compte := 0;
		EXECUTE 'SELECT type FROM ref.ddd WHERE cd = '''||libChamp||'''' INTO typChamp;
		IF (typChamp = 'int') THEN --- un entier
			EXECUTE 'SELECT count(*) FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^\d+$''' INTO compte;
		ELSIF (typChamp = 'float') THEN --- un float
			EXECUTE 'SELECT count(*) FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^\-?\d+\.\d+$''' INTO compte;
		ELSIF (typChamp = 'date') THEN --- une date
			EXECUTE 'SELECT count(*) FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^[1,2][0-9]{2}[0-9]\-[0,1][0-9]\-[0-3][0-9]$'' AND "'||libChamp||'" !~ ''^[1,2][0-9]{2}[0-9]\-[0,1][0-9]$'' AND "'||libChamp||'" !~ ''^[1,2][0-9]{2}[0-9]$''' INTO compte;
		ELSIF (typChamp = 'boolean') THEN --- Boolean
			EXECUTE 'SELECT count(*) FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^t$'' AND "'||libChamp||'" !~ ''^f$''' INTO compte;
		ELSE --- le reste
			compte := 0;
		END IF;
		CASE WHEN (compte > 0) THEN
			--- log
			out.lib_table := libTable; out.lib_champ := libChamp;	out.lib_log := typChamp||' incorrecte => SELECT * FROM hub_verif_plus('''||libSchema||''','''||libTable||''','''||libChamp||''',''type'');'; out.nb_occurence := compte||' occurence(s)'; return next out;
			out.lib_log := typChamp||' incorrecte ';PERFORM hub_log (libSchema, out);
		ELSE --- rien
		END CASE;	
		END LOOP;
	END LOOP;
ELSE --- rien
END CASE;

--- Test concernant les doublon
CASE WHEN (typVerif = 'doublon' OR typVerif = 'all') THEN
FOR libTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_'||typJdd
	LOOP
	FOR libChamp in EXECUTE 'SELECT string_agg(''"''||cd||''"'',''||'') FROM ref.fsd_'||typJdd||' WHERE tbl_name = '''||libTable||''' AND unicite = ''Oui'''
		LOOP	
		compte := 0;
		EXECUTE 'SELECT count('||libChamp||') FROM "'||libSchema||'"."temp_'||libTable||'" GROUP BY '||libChamp||' HAVING COUNT('||libChamp||') > 1' INTO compte;
		CASE WHEN (compte > 0) THEN
			--- log
			out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := 'doublon(s) => SELECT * FROM hub_verif_plus('''||libSchema||''','''||libTable||''','''||libChamp||''',''doublon'');'; out.nb_occurence := compte||' occurence(s)'; return next out;
			out.lib_log := 'doublon(s)';PERFORM hub_log (libSchema, out);			
		ELSE --- rien
		END CASE;
		END LOOP;
	END LOOP;
ELSE --- rien
END CASE;

--- Test concernant le vocbulaire controlé
CASE WHEN (typVerif = 'vocactrl' OR typVerif = 'all') THEN
FOR libTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_'||typJdd
	LOOP FOR libChamp in EXECUTE 'SELECT cd FROM ref.fsd_'||typJdd||' WHERE tbl_name = '''||libTable||''';'
		LOOP EXECUTE 'SELECT DISTINCT 1 FROM ref.voca_ctrl WHERE "typChamp" = '''||libChamp||''' ;' INTO flag;
		CASE WHEN flag = 1 THEN
			compte := 0;
			EXECUTE 'SELECT count("'||libChamp||'") FROM "'||libSchema||'"."temp_'||libTable||'" LEFT JOIN ref.voca_ctrl ON "'||libChamp||'" = "cdChamp" WHERE "cdChamp" IS NULL'  INTO compte;
			CASE WHEN (compte > 0) THEN
				--- log
				out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := 'Valeur(s) non listée(s) => SELECT * FROM hub_verif_plus('''||libSchema||''','''||libTable||''','''||libChamp||''',''vocactrl'');'; out.nb_occurence := compte||' occurence(s)'; return next out;
				out.lib_log := 'Valeur(s) non listée(s)';PERFORM hub_log (libSchema, out);
			ELSE --- rien
			END CASE;
		ELSE --- rien
		END CASE;
		END LOOP;
	END LOOP;
ELSE --- rien
END CASE;

--- Le 100%
CASE WHEN out.lib_log = '' THEN
	out.lib_table := '-'; out.lib_champ := '-'; out.lib_log := jdd||' conformes pour '||typVerif; out.nb_occurence := '-'; PERFORM hub_log (libSchema, out); return next out;
ELSE ---Rien
END CASE;

END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_verif_plus
--- Description : Vérification des données
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_verif_plus(libSchema varchar, libTable varchar, libChamp varchar, typVerif varchar = 'all') RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE champRefSelected varchar;
DECLARE champRef varchar;
DECLARE typJdd varchar;
DECLARE typChamp varchar;
DECLARE flag integer;
BEGIN
--- Output
out.lib_schema := libSchema;out.typ_log := 'hub_verif_plus';SELECT CURRENT_TIMESTAMP INTO out.date_log;
--- Variables
CASE 	WHEN libTable LIKE 'metadonnees%' 				THEN 	champRef = 'cd_jdd_perm';typJdd = 'meta';
	WHEN libTable LIKE 'observation%' OR libTable LIKE 'releve%' 	THEN 	champRef = 'cd_obs_perm';typJdd = 'data';
	WHEN libTable LIKE 'entite%' 					THEN 	champRef = 'cd_ent_perm';typJdd = 'taxa';
	ELSE 									champRef = ''; 	END CASE;

--- Test concernant l'obligation
CASE WHEN (typVerif = 'obligation' OR typVerif = 'all') THEN
FOR champRefSelected IN EXECUTE 'SELECT "'||champRef||'" FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" IS NULL'
	LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  "'||champRef||'" = '''||champRefSelected||''''; return next out; END LOOP;
ELSE --- rien
END CASE;

--- Test concernant le typage des champs
CASE WHEN (typVerif = 'type' OR typVerif = 'all') THEN
	EXECUTE 'SELECT type FROM ref.ddd WHERE cd = '''||libChamp||'''' INTO typChamp;
		IF (typChamp = 'int') THEN --- un entier
			FOR champRefSelected IN EXECUTE 'SELECT "'||champRef||'" FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^\d+$''' 
			LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  "'||champRef||'" = '''||champRefSelected||''''; return next out;END LOOP;
		ELSIF (typChamp = 'float') THEN --- un float
			FOR champRefSelected IN EXECUTE 'SELECT "'||champRef||'" FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^\-?\d+\.\d+$'''
			LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  "'||champRef||'" = '''||champRefSelected||''''; return next out;END LOOP;
		ELSIF (typChamp = 'date') THEN --- une date
			FOR champRefSelected IN EXECUTE 'SELECT "'||champRef||'" FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^[1,2][0-9]{2}[0-9]\-[0,1][0-9]\-[0-3][0-9]$'' AND "'||libChamp||'" !~ ''^[1,2][0-9]{2}[0-9]\-[0,1][0-9]$'' AND "'||libChamp||'" !~ ''^[1,2][0-9]{2}[0-9]$'''
			LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  "'||champRef||'" = '''||champRefSelected||''''; return next out;END LOOP;
		ELSIF (typChamp = 'boolean') THEN --- Boolean
			FOR champRefSelected IN EXECUTE 'SELECT "'||champRef||'" FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^t$'' AND "'||libChamp||'" !~ ''^f$'''
			LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  "'||champRef||'" = '''||champRefSelected||''''; return next out;END LOOP;
		ELSE --- le reste
			EXECUTE 'SELECT 1';
		END IF;
ELSE --- rien
END CASE;

--- Test concernant les doublon
CASE WHEN (typVerif = 'doublon' OR typVerif = 'all') THEN
	FOR champRefSelected IN EXECUTE 'SELECT '||libChamp||' FROM "'||libSchema||'"."temp_'||libTable||'" GROUP BY '||libChamp||' HAVING COUNT('||libChamp||') > 1'
		LOOP EXECUTE 'SELECT "'||champRef||'" FROM "'||libSchema||'"."temp_'||libTable||'" WHERE '||libChamp||' = '''||champRefSelected||''';' INTO champRefSelected;
		out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  "'||champRef||'" = '''||champRefSelected||''''; return next out;END LOOP;
		--- EXECUTE 'INSERT INTO "'||libSchema||'".zz_log (lib_schema,lib_table,lib_champ,typ_log,lib_log,nb_occurence,date_log) VALUES ('''||out.lib_schema||''','''||out.lib_table||''','''||out.lib_champ||''','''||out.typ_log||''','''||out.lib_log||''','''||out.nb_occurence||''','''||out.date_log||''');';
ELSE --- rien
END CASE;

--- Test concernant le vocbulaire controlé
CASE WHEN (typVerif = 'vocactrl' OR typVerif = 'all') THEN
	EXECUTE 'SELECT DISTINCT 1 FROM ref.voca_ctrl WHERE "typChamp" = '''||libChamp||''' ;' INTO flag;
		CASE WHEN flag = 1 THEN
		FOR champRefSelected IN EXECUTE 'SELECT "'||champRef||'" FROM "'||libSchema||'"."temp_'||libTable||'" LEFT JOIN ref.voca_ctrl ON "'||libChamp||'" = "cdChamp" WHERE "cdChamp" IS NULL'
		LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  "'||champRef||'" = '''||champRefSelected||''''; return next out; END LOOP;
	ELSE ---Rien
	END CASE;
ELSE --- rien
END CASE;

--- Log général
--- EXECUTE 'INSERT INTO "public".zz_log (lib_schema,lib_table,lib_champ,typ_log,lib_log,nb_occurence,date_log) VALUES ('''||out.lib_schema||''',''-'',''-'',''hub_verif'',''-'',''-'','''||out.date_log||''');';
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
TRUNCATE public.verification;
SELECT * into out FROM hub_verif(libSchema,'meta','all');return next out;
SELECT * into out FROM hub_verif(libSchema,'data','all');return next out;
SELECT * into out FROM hub_verif(libSchema,'taxa','all');return next out;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_log
--- Description : ecrit les output dans le Log du schema et le log global
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_log (libSchema varchar, outp zz_log) RETURNS void AS 
$BODY$ 
BEGIN
EXECUTE 'INSERT INTO "'||libSchema||'".zz_log (lib_schema,lib_table,lib_champ,typ_log,lib_log,nb_occurence,date_log) VALUES ('''||outp.lib_schema||''','''||outp.lib_table||''','''||outp.lib_champ||''','''||outp.typ_log||''','''||outp.lib_log||''','''||outp.nb_occurence||''','''||outp.date_log||''');';
CASE WHEN libSchema <> 'public' THEN EXECUTE 'INSERT INTO "public".zz_log (lib_schema,lib_table,lib_champ,typ_log,lib_log,nb_occurence,date_log) VALUES ('''||outp.lib_schema||''','''||outp.lib_table||''','''||outp.lib_champ||''','''||outp.typ_log||''','''||outp.lib_log||''','''||outp.nb_occurence||''','''||outp.date_log||''');'; ELSE PERFORM 1; END CASE;
END;$BODY$ LANGUAGE plpgsql;