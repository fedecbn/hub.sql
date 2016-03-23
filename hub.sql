--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
---- FONCTIONS LOCALES ET GLOBALES POUR LE PARTAGE DE DONNÉES AU SEIN DU RESEAU DES CBN ----
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_init
--- Description : initialise les fonction du hub (supprime toutes les fonctions) et  initialise certaines tables (zz_log et bilan)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_init() RETURNS varchar  AS 
$BODY$ 
DECLARE fonction varchar;
DECLARE schema varchar;
DECLARE table varchar;
DECLARE exist varchar;
BEGIN 
FOR fonction IN SELECT routine_name||'('||string_agg(data_type ,',')||')' FROM(
	SELECT routine_name, z.specific_name, 
		CASE WHEN z.data_type  = 'ARRAY' THEN z.udt_name||'[]' 
		WHEN z.data_type  = 'USER-DEFINED' THEN z.udt_name 
		ELSE z.data_type END as data_type
	FROM information_schema.routines a
	JOIN information_schema.parameters z ON a.specific_name = z.specific_name
	WHERE  routine_name LIKE 'hub_%'
	ORDER BY ordinal_position
	) as one
	GROUP BY routine_name, specific_name
   LOOP
   EXECUTE 'DROP FUNCTION '||fonction||';';
   END LOOP;
   
--- Création de la table générale de log
schema = 'public'; table = 'zz_log';
EXECUTE 'SELECT CASE WHEN table_name IS NULL THEN ''Non'' ELSE ''Oui'' END FROM information_schema.tables WHERE table_schema = '''||schema||''' AND table_name = '''||table||''';' INTO exist
CASE WHEN exist = 'Non' THEN
	CREATE TABLE IF NOT EXISTS public.zz_log (lib_schema character varying,lib_table character varying,lib_champ character varying,typ_log character varying,lib_log character varying,nb_occurence character varying,date_log timestamp);
ELSE END CASE;

--- Création de la table générale de bilan
schema = 'public'; table = 'bilan';
EXECUTE 'SELECT CASE WHEN table_name IS NULL THEN ''Non'' ELSE ''Oui'' END FROM information_schema.tables WHERE table_schema = '''||schema||''' AND table_name = '''||table||''';' INTO exist
CASE WHEN exist = 'Non' THEN
	CREATE TABLE IF NOT EXISTS public.bilan (uid integer NOT NULL,lib_cbn character varying,data_nb_releve integer,data_nb_observation integer,data_nb_taxon integer,taxa_nb_taxon integer,taxa_pourcentage_statut character varying,CONSTRAINT bilan_pkey PRIMARY KEY (uid))WITH (OIDS=FALSE);
ELSE END CASE;

RETURN 'Initilsation OK';
END;$BODY$ LANGUAGE plpgsql;
SELECT * FROM hub_init();

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
out.lib_schema := schemaSource; out.lib_table := tableSource; out.lib_champ := '-'; out.typ_log := 'hub_add'; SELECT CURRENT_TIMESTAMP INTO out.date_log;
--- Commande
SELECT CASE WHEN substring(tableSource from 0 for 5) = 'temp' THEN 'temp_metadonnees' ELSE 'metadonnees' END INTO metasource;
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN 
	EXECUTE 'SELECT CASE WHEN string_agg(''''''''||cd_jdd||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||cd_jdd||'''''''','','') END FROM "'||schemaSource||'"."'||metasource||'" WHERE typ_jdd = '''||jdd||''';' INTO listJdd;
ELSE listJdd := ''''||jdd||'''';
END CASE;

CASE WHEN typAction = 'push_total' OR typAction = 'push_diff' THEN
	EXECUTE 'SELECT string_agg(''a."''||column_name||''"::''||data_type,'','')  FROM information_schema.columns where table_name = '''||tableDest||''' AND table_schema = '''||schemaDest||''' ' INTO listeChamp1;
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
ELSE listJdd := ''''||jdd||'''';END CASE;
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
CREATE OR REPLACE FUNCTION hub_clone(libSchema varchar, typ varchar = 'all') RETURNS setof zz_log AS 
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
			CASE WHEN typ = 'all' OR typ = 'propre' THEN
				EXECUTE 'SELECT string_agg(one.cd_champ||'' ''||one.format,'','') FROM (SELECT cd_champ, format FROM ref.fsd WHERE typ_jdd = '''||typjdd||''' AND cd_table = '''||cd_table||''' ORDER BY ordre_champ) as one;' INTO list_champ;
				EXECUTE 'SELECT ''CONSTRAINT ''||cd_table||''_pkey PRIMARY KEY (''||string_agg(cd_champ,'','')||'')'' FROM ref.fsd WHERE typ_jdd = '''||typjdd||''' AND cd_table = '''||cd_table||''' AND unicite = ''Oui'' GROUP BY cd_table' INTO list_contraint ;
				EXECUTE 'CREATE TABLE '||schema_lower||'.'||cd_table||' ('||list_champ||','||list_contraint||');';
			ELSE END CASE;
			CASE WHEN typ = 'all' OR typ = 'temp' THEN
				EXECUTE 'SELECT string_agg(one.cd_champ||'' character varying'','','') FROM (SELECT cd_champ, format FROM ref.fsd WHERE typ_jdd = '''||typjdd||''' AND cd_table = '''||cd_table||''' ORDER BY ordre_champ) as one;' INTO list_champ_sans_format;
				EXECUTE 'CREATE TABLE '||schema_lower||'.temp_'||cd_table||' ('||list_champ_sans_format||');';
			ELSE END CASE;
		END LOOP;
	END LOOP;
	--- LISTE TAXON
	EXECUTE '
	CREATE TABLE "'||schema_lower||'".zz_log_liste_taxon  (cd_ref character varying, nom_valide character varying);
	CREATE TABLE "'||schema_lower||'".zz_log_liste_taxon_et_infra  (cd_ref_demande character varying, nom_valide_demande character varying, cd_ref_cite character varying, nom_complet_cite character varying, rang_cite character varying, cd_taxsup_cite character varying);
	';
	--- LOG
	EXECUTE '
	CREATE TABLE "'||schema_lower||'".zz_log  (lib_schema character varying,lib_table character varying,lib_champ character varying,typ_log character varying,lib_log character varying,nb_occurence character varying,date_log timestamp);
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
out.lib_schema := schemaSource; out.lib_table := tableSource; out.lib_champ := '-';out.typ_log := 'hub_del'; SELECT CURRENT_TIMESTAMP INTO out.date_log;

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
out.typ_log := 'hub_diff'; SELECT CURRENT_TIMESTAMP INTO out.date_log; out.lib_table := '-'; out.lib_champ := '-'; out.nb_occurence := ct||' tables analysées';
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
CREATE OR REPLACE FUNCTION hub_export(libSchema varchar,jdd varchar,path varchar,format varchar = 'fcbn',source varchar = null) RETURNS setof zz_log  AS 
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
--- Output&Log
out.lib_schema := libSchema;out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'hub_export';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;
--- Commandes
CASE WHEN format = 'fcbn' THEN
	FOR libTable in EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd '||typJdd||''
		LOOP EXECUTE 'COPY (SELECT * FROM  '||libSchema||'.'||source||libTable||' '||listJdd||') TO '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';'; END LOOP;
	out.lib_log :=  'Tous les jdd ont été exporté au format '||format;
WHEN format = 'sinp' THEN
	out.lib_log :=  'format SINP à implémenter';
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
out.lib_schema := libSchema;out.lib_table := tableRef;out.lib_champ := champ_perm;out.typ_log := 'hub_idperm'; SELECT CURRENT_TIMESTAMP INTO out.date_log;
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
CREATE OR REPLACE FUNCTION hub_import(libSchema varchar, jdd varchar, path varchar, rempla boolean = false, files varchar = '') RETURNS setof zz_log AS 
$BODY$
DECLARE typJdd varchar;
DECLARE listJdd varchar;
DECLARE libTable varchar;
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
--- Commande
--- Cas du chargement de tous les jeux de données
CASE WHEN jdd = 'all' THEN 
	FOR libTable in EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd;'
	LOOP 
		CASE WHEN rempla = TRUE THEN EXECUTE 'TRUNCATE "'||libSchema||'".temp_'||libTable||';';out.lib_log := ' Tous les fichiers ont été remplacé '; ELSE out.lib_log := ' Tous les fichiers ont été importé '; END CASE;
		EXECUTE 'COPY "'||libSchema||'".temp_'||libTable||' FROM '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';'; 
	END LOOP;
--- Cas du chargement global (tous les fichiers)
WHEN files = '' THEN
	FOR libTable in EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd WHERE typ_jdd = '''||typJdd||''' OR typ_jdd = ''meta'';'
	LOOP 
		CASE WHEN rempla = TRUE THEN EXECUTE 'DELETE FROM "'||libSchema||'".temp_'||libTable||' WHERE cd_jdd IN ('||listJdd||');'; ELSE PERFORM 1; END CASE;
		EXECUTE 'COPY "'||libSchema||'".temp_'||libTable||' FROM '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';'; 
	END LOOP;
	CASE WHEN rempla = TRUE THEN out.lib_log := 'jdd '||jdd||' remplacé'; ELSE out.lib_log := 'jdd '||jdd||' ajouté';END CASE;
--- Cas du chargement spécifique (un seul fichier)
ELSE
	CASE WHEN rempla = TRUE THEN EXECUTE 'DELETE FROM "'||libSchema||'".temp_'||files||' WHERE cd_jdd IN ('||listJdd||');'; ELSE PERFORM 1; END CASE;
	EXECUTE 'COPY "'||libSchema||'".temp_'||files||' FROM '''||path||'std_'||files||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';';
	CASE WHEN rempla = TRUE THEN out.lib_log := 'fichier std_'||files||'.csv remplacé'; ELSE out.lib_log := 'fichier std_'||files||'.csv ajouté'; END CASE;
END CASE;
--- WHEN jdd <> 'data' AND jdd <> 'taxa' AND files <> '' THEN 
--- 	CASE WHEN rempla = TRUE THEN EXECUTE 'DELETE FROM "'||libSchema||'".temp_'||files||' WHERE cd_jdd IN ('||listJdd||');'; out.lib_log := 'Fichier remplacé'; ELSE out.lib_log := 'Fichier ajouté'; END CASE;
--- 	EXECUTE 'COPY "'||libSchema||'".temp_'||files||' FROM '''||path||'std_'||files||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';';
--- ELSE out.lib_log := 'Problème identifié (Soit le jdd soit le fichier)'; END CASE;

--- Output&Log
out.lib_schema := libSchema;out.lib_champ := '-';out.lib_table := '-';out.typ_log := 'hub_import';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;PERFORM hub_log (libSchema, out);RETURN next out;
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
out.lib_schema := libSchema;out.lib_champ := '-';out.lib_table := 'zz_log_liste_taxon';out.typ_log := 'hub_import_taxon';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;PERFORM hub_log (libSchema, out);RETURN next out;
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
SELECT * INTO out FROM hub_ref('create',path);PERFORM hub_log ('public', out);RETURN NEXT out;
SELECT * INTO out FROM hub_clone(libSchema);PERFORM hub_log (libSchema, out);RETURN NEXT out;
END;$BODY$ LANGUAGE plpgsql;

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
		ct = ct+1;
		CASE WHEN mode = 1 THEN tableSource := libTable; tableDest := 'temp_'||libTable; WHEN mode = 2 THEN tableSource := 'temp_'||libTable; tableDest := libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest , jdd, 'push_total'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE ct2 = ct2+1; END CASE;
	END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE '''||tableRef||'%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		ct = ct+1;
		CASE WHEN mode = 1 THEN tableSource := libTable; tableDest := 'temp_'||libTable; WHEN mode = 2 THEN tableSource := 'temp_'||libTable; tableDest := libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest , jdd, 'push_total'); 
			CASE WHEN out.nb_occurence <> '-' THEN RETURN NEXT out; ELSE ct2 = ct2+1; END CASE;
	END LOOP;
ELSE ---Log
	out.lib_schema := libSchema; out.lib_champ := '-'; out.typ_log := 'hub_pull';SELECT CURRENT_TIMESTAMP INTO out.date_log; out.lib_log := 'ERREUR : sur champ jdd = '||jdd; PERFORM hub_log (libSchema, out);RETURN NEXT out;
END CASE;

---Log final
out.typ_log := 'hub_pull'; SELECT CURRENT_TIMESTAMP INTO out.date_log; out.lib_table := '-'; out.lib_champ := '-';
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
ELSE EXECUTE 'SELECT typ_jdd FROM "'||libSchema||'".temp_metadonnees WHERE cd_jdd = '''||jdd||''';' INTO typJdd;
END CASE;
--- mode 1 = intra Shema / mode 2 = entre shema et agregation
CASE WHEN mode = 1 THEN schemaSource :=libSchema; schemaDest :=libSchema; WHEN mode = 2 THEN schemaSource :=libSchema; schemaDest :='agregation'; ELSE flag :=0; END CASE;

--- Commandes
--- Remplacement total = hub_clear + hub_add
CASE WHEN typAction = 'replace' THEN
	CASE WHEN mode = 1 THEN SELECT * INTO out FROM hub_clear(schemaDest, jdd, 'propre'); WHEN mode = 2 THEN SELECT * INTO out FROM hub_clear(schemaDest, jdd, 'temp'); ELSE END CASE; return next out;
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
	out.lib_schema := libSchema; out.lib_champ := '-'; out.typ_log := 'hub_push';SELECT CURRENT_TIMESTAMP INTO out.date_log; out.lib_log := 'ERREUR : sur champ action = '||jdd; PERFORM hub_log (libSchema, out);RETURN NEXT out;
END CASE;

---Log final
out.typ_log := 'hub_push'; SELECT CURRENT_TIMESTAMP INTO out.date_log; out.lib_table := '-'; out.lib_champ := '-';
CASE 
WHEN (ct = ct2) AND typAction = 'replace' THEN out.lib_log := 'Partie temporaire vide - jdd = '||jdd; out.nb_occurence := '-'; 
WHEN (ct <> ct2) AND typAction = 'replace' THEN out.lib_log := 'Données poussées - jdd = '||jdd; out.nb_occurence := '-';
WHEN (ct = ct2) AND typAction = 'add' THEN out.lib_log := 'Aucune modification à apporter à la partie propre - jdd = '||jdd; out.nb_occurence := '-'; 
WHEN (ct <> ct2) AND typAction = 'add' THEN out.lib_log := 'Modification apportées à la partie propre - jdd = '||jdd; out.nb_occurence := '-';
WHEN (ct = ct2) AND typAction = 'del' THEN out.lib_log := 'Aucun point commun entre les partie propre et temporaire - jdd = '||jdd; out.nb_occurence := '-'; 
WHEN (ct <> ct2) AND typAction = 'del' THEN out.lib_log := 'Partie temporaire nettoyée - jdd = '||jdd; out.nb_occurence := '-';
ELSE SELECT 1 into nothing; END CASE;
PERFORM hub_log (libSchema, out);RETURN NEXT out; 

END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_ref 
--- Description : Création des référentiels
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_ref(typAction varchar, path varchar = '/home/hub/00_ref/') RETURNS setof zz_log AS 
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
CASE WHEN typAction = 'drop' THEN	--- Suppression
	EXECUTE 'SELECT DISTINCT 1 FROM information_schema.schemata WHERE schema_name = ''ref''' INTO flag1;
	CASE WHEN flag1 = 1 THEN DROP SCHEMA IF EXISTS ref CASCADE; out.lib_log := 'Shema ref supprimé';RETURN next out;
	ELSE out.lib_log := 'Schéma ref inexistant';RETURN next out;END CASE;
WHEN typAction = 'delete' THEN	--- Suppression
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
WHEN typAction = 'create' THEN	--- Creation
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
WHEN typAction = 'update' THEN	--- Mise à jour
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
out.lib_schema := schemaSource; out.lib_table := tableSource; out.lib_champ := '-'; out.typ_log := 'hub_update';SELECT CURRENT_TIMESTAMP INTO out.date_log;
--- Commande
EXECUTE 'SELECT string_agg(''a."''||cd_champ||''" = b."''||cd_champ||''"'','' AND '') FROM ref.fsd WHERE (cd_table = '''||tableSource||''' OR cd_table = '''||tableDest||''') AND unicite = ''Oui''' INTO jointure;
EXECUTE 'SELECT string_agg(''''''''||cd_champ||'''''''','', '') FROM ref.fsd WHERE (cd_table = '''||tableSource||''' OR cd_table = '''||tableDest||''') AND unicite = ''Oui''' INTO champRef_guillement;
EXECUTE 'SELECT string_agg(''a.''||cd_champ,''||'') FROM ref.fsd WHERE (cd_table = '''||tableSource||''' OR cd_table = '''||tableDest||''') AND unicite = ''Oui''' INTO champRef;
EXECUTE 'SELECT string_agg(''"''||column_name||''" = b."''||column_name||''"::''||data_type,'','')  FROM information_schema.columns WHERE table_name = '''||tableDest||''' AND table_schema = '''||schemaDest||''' AND column_name NOT IN ('||champRef_guillement||')' INTO listeChamp;
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
			EXECUTE 'SELECT count(*) FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^[1,2][0-9]{2}[0-9]\-[0,1][0-9]\-[0-3][0-9]$''' INTO compte;
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
FOR libTable in EXECUTE 'SELECT DISTINCT cd_table FROM ref.fsd WHERE typ_jdd = '''||typJdd||''''
	LOOP FOR libChamp in EXECUTE 'SELECT cd_champ FROM ref.fsd WHERE cd_table = '''||libTable||''' AND typ_jdd = '''||typJdd||''''
		LOOP EXECUTE 'SELECT DISTINCT 1 FROM ref.voca_ctrl WHERE cd_champ = '''||libChamp||''' ;' INTO flag;
		CASE WHEN flag = 1 THEN
			compte := 0;
			EXECUTE 'SELECT count("'||libChamp||'") FROM "'||libSchema||'"."temp_'||libTable||'" LEFT JOIN ref.voca_ctrl ON "'||libChamp||'" = code_valeur WHERE code_valeur IS NULL'  INTO compte;
			CASE WHEN (compte > 0) THEN
				--- log
				out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := 'Valeur(s) non listée(s) => SELECT * FROM hub_verif_plus('''||libSchema||''','''||libTable||''','''||libChamp||''',''vocactrl'');'; out.nb_occurence := compte||' occurence(s)'; return next out;
				out.lib_log := typJdd ||' : Valeur(s) non listée(s)';PERFORM hub_log (libSchema, out);
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
CREATE OR REPLACE FUNCTION hub_verif_plus(libSchema varchar, libTable varchar, libChamp varchar, typVerif varchar = 'all') RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE champRefSelected varchar;
DECLARE champRef varchar;
DECLARE typChamp varchar;
DECLARE flag integer;
BEGIN
--- Output
out.lib_schema := libSchema;out.typ_log := 'hub_verif_plus';SELECT CURRENT_TIMESTAMP INTO out.date_log;
--- Variables
EXECUTE 'SELECT string_agg(cd_champ,''||'') FROM ref.fsd WHERE cd_table = '''||libTable||''' AND unicite = ''Oui'';' INTO champRef;

--- Test concernant l'obligation
CASE WHEN (typVerif = 'obligation' OR typVerif = 'all') THEN
FOR champRefSelected IN EXECUTE 'SELECT '||champRef||' FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" IS NULL'
	LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  '||champRef||' = '''||champRefSelected||''''; return next out; END LOOP;
ELSE --- rien
END CASE;

--- Test concernant le typage des champs
CASE WHEN (typVerif = 'type' OR typVerif = 'all') THEN
	EXECUTE 'SELECT DISTINCT format FROM ref.fsd WHERE cd_champ = '''||libChamp||'''' INTO typChamp;
		IF (typChamp = 'int') THEN --- un entier
			FOR champRefSelected IN EXECUTE 'SELECT '||champRef||' FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^\d+$''' 
			LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  '||champRef||' = '''||champRefSelected||''''; return next out;END LOOP;
		ELSIF (typChamp = 'float') THEN --- un float
			FOR champRefSelected IN EXECUTE 'SELECT '||champRef||' FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^\-?\d+\.\d+$'''
			LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  '||champRef||' = '''||champRefSelected||''''; return next out;END LOOP;
		ELSIF (typChamp = 'date') THEN --- une date
			FOR champRefSelected IN EXECUTE 'SELECT '||champRef||' FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^[1,2][0-9]{2}[0-9]\-[0,1][0-9]\-[0-3][0-9]$'' AND "'||libChamp||'" !~ ''^[1,2][0-9]{2}[0-9]\-[0,1][0-9]$'' AND "'||libChamp||'" !~ ''^[1,2][0-9]{2}[0-9]$'''
			LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  '||champRef||' = '''||champRefSelected||''''; return next out;END LOOP;
		ELSIF (typChamp = 'boolean') THEN --- Boolean
			FOR champRefSelected IN EXECUTE 'SELECT '||champRef||' FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^t$'' AND "'||libChamp||'" !~ ''^f$'''
			LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  '||champRef||' = '''||champRefSelected||''''; return next out;END LOOP;
		ELSE --- le reste
			EXECUTE 'SELECT 1';
		END IF;
ELSE --- rien
END CASE;

--- Test concernant les doublon
CASE WHEN (typVerif = 'doublon' OR typVerif = 'all') THEN
	FOR champRefSelected IN EXECUTE 'SELECT '||libChamp||' FROM "'||libSchema||'"."temp_'||libTable||'" GROUP BY '||libChamp||' HAVING COUNT('||libChamp||') > 1'
		LOOP EXECUTE 'SELECT '||champRef||' FROM "'||libSchema||'"."temp_'||libTable||'" WHERE '||libChamp||' = '''||champRefSelected||''';' INTO champRefSelected;
		out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  '||champRef||' = '''||champRefSelected||''''; return next out;END LOOP;
ELSE --- rien
END CASE;

--- Test concernant le vocbulaire controlé
CASE WHEN (typVerif = 'vocactrl' OR typVerif = 'all') THEN
	EXECUTE 'SELECT DISTINCT 1 FROM ref.voca_ctrl WHERE cd_champ = '''||libChamp||''' ;' INTO flag;
		CASE WHEN flag = 1 THEN
		FOR champRefSelected IN EXECUTE 'SELECT '||champRef||' FROM "'||libSchema||'"."temp_'||libTable||'" LEFT JOIN ref.voca_ctrl ON "'||libChamp||'" = code_valeur WHERE code_valeur IS NULL'
		LOOP out.lib_table := libTable; out.lib_champ := libChamp; out.lib_log := champRefSelected; out.nb_occurence := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  '||champRef||' = '''||champRefSelected||''''; return next out; END LOOP;
	ELSE ---Rien
	END CASE;
ELSE --- rien
END CASE;

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
--- Nom : hub_log
--- Description : ecrit les output dans le Log du schema et le log global
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_log (libSchema varchar, outp zz_log, action varchar = 'write') RETURNS void AS 
$BODY$ 
BEGIN
CASE WHEN action = 'write' THEN
	EXECUTE 'INSERT INTO "'||libSchema||'".zz_log (lib_schema,lib_table,lib_champ,typ_log,lib_log,nb_occurence,date_log) VALUES ('''||outp.lib_schema||''','''||outp.lib_table||''','''||outp.lib_champ||''','''||outp.typ_log||''','''||outp.lib_log||''','''||outp.nb_occurence||''','''||outp.date_log||''');';
	CASE WHEN libSchema <> 'public' THEN EXECUTE 'INSERT INTO "public".zz_log (lib_schema,lib_table,lib_champ,typ_log,lib_log,nb_occurence,date_log) VALUES ('''||outp.lib_schema||''','''||outp.lib_table||''','''||outp.lib_champ||''','''||outp.typ_log||''','''||outp.lib_log||''','''||outp.nb_occurence||''','''||outp.date_log||''');'; ELSE PERFORM 1; END CASE;
WHEN action = 'clear' THEN
	EXECUTE 'DELETE FROM "'||libSchema||'".zz_log';
ELSE SELECT 1;
END CASE;
END;$BODY$ LANGUAGE plpgsql;