--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
---- FONCTIONS LOCALES ET GLOBALES POUR LE PARTAGE DE DONNÉES AU SEIN DU RESEAU DES CBN ----
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------

--- Création de la table générale de log
CREATE TABLE IF NOT EXISTS "public"."zz_log" ("libSchema" character varying,"libTable" character varying,"libChamp" character varying,"typLog" character varying,"libLog" character varying,"nbOccurence" character varying,"date" date);

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_add 
--- Description : Ajout de données (fonction utilisée par une autre fonction)
--- Variables :
--- o schemaSource = Nom du schema source
--- o schemaDest = Nom du schema de destination
--- o tableSource  = Nom de la table source
--- o tableDest  = Nom de la table de destination
--- o champRef = nom du champ de référence utilisé pour tester la jointure entre la source et la destination
--- o jdd = jeu de donnée (code du jeu ou 'data' ou 'taxa')
--- o action = type d'action à réaliser - valeur possibles : 'push_total', 'push_diff' et 'diff'(par défaut)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Attention listJdd null sur jdd erroné
CREATE OR REPLACE FUNCTION hub_add(schemaSource varchar,schemaDest varchar, tableSource varchar, tableDest varchar,champRef varchar, jdd varchar, action varchar = 'diff') RETURNS setof zz_log  AS 
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
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN EXECUTE 'SELECT CASE WHEN string_agg(''''''''||"cdJdd"||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||"cdJdd"||'''''''','','') END FROM "'||schemaSource||'"."'||metasource||'" WHERE "typJdd" = '''||jdd||''';' INTO listJdd;
ELSE listJdd := ''||jdd||'';END CASE;

CASE WHEN champRef = 'cdJddPerm' THEN typJdd = 'meta';flag := 1;
WHEN champRef = 'cdObsPerm' THEN typJdd = 'data';flag := 1;
WHEN champRef = 'cdEntPerm' THEN typJdd = 'taxa';flag := 1;
ELSE flag := 0;
END CASE;
EXECUTE 'SELECT string_agg(''a."''||cd||''" = b."''||cd||''"'','' AND '') FROM ref.fsd_'||typJdd||' WHERE (tbl_name = '''||tableSource||''' OR tbl_name = '''||tableDest||''') AND unicite = ''Oui''' INTO jointure;
source := '"'||schemaSource||'"."'||tableSource||'"';
destination := '"'||schemaDest||'"."'||tableDest||'"';

--- Output&Log
out."libSchema" := schemaSource; out."libTable" := tableSource; out."libChamp" := '-'; out."typLog" := 'hub_add'; SELECT CURRENT_TIMESTAMP INTO out."date";
--- Commande
CASE WHEN action = 'push_total' THEN --- CAS utilisé pour ajouter en masse.
	EXECUTE 'SELECT string_agg(''z."''||column_name||''"::''||data_type,'','')  FROM information_schema.columns where table_name = '''||tableDest||''' AND table_schema = '''||schemaDest||''' ' INTO listeChamp1;
	EXECUTE 'SELECT string_agg(''"''||column_name||''"'','','')  FROM information_schema.columns where table_name = '''||tableSource||''' AND table_schema = '''||schemaSource||''' ' INTO listeChamp2;
	EXECUTE 'INSERT INTO '||destination||' ('||listeChamp2||') SELECT '||listeChamp1||' FROM '||source||' z WHERE "cdJdd" IN ('||listJdd||')';
		out."nbOccurence" := 'total'; out."libLog" := 'Jdd complet(s) ajouté(s)'; PERFORM hub_log (schemaSource, out);RETURN NEXT out;

WHEN action = 'push_diff' THEN --- CAS utilisé pour ajouter les différences
	--- Recherche des concepts (obsevation, jdd ou entite) présent dans la source et absent dans la destination
	EXECUTE 'SELECT count(DISTINCT b."'||champRef||'") FROM '||source||' b LEFT JOIN '||destination||' a ON '||jointure||' WHERE a."'||champRef||'" IS NULL AND b."cdJdd" IN ('||listJdd||')' INTO compte; 
	CASE WHEN (compte > 0) THEN --- Si de nouveau concept sont succeptible d'être ajouté
		EXECUTE 'SELECT string_agg(''z."''||column_name||''"::''||data_type,'','')  FROM information_schema.columns where table_name = '''||tableDest||''' AND table_schema = '''||schemaDest||''' ' INTO listeChamp1;
		EXECUTE 'SELECT string_agg(''"''||column_name||''"'','','')  FROM information_schema.columns where table_name = '''||tableSource||''' AND table_schema = '''||schemaSource||''' ' INTO listeChamp2;
		EXECUTE 'INSERT INTO '||destination||' ('||listeChamp2||') SELECT '||listeChamp1||' FROM '||source||' z LEFT JOIN '||source||' a ON '||jointure||' WHERE a."'||champRef||'" IS NULL AND "cdJdd" IN ('||listJdd||')';
		out."nbOccurence" := compte||' occurence(s)'; out."libLog" := 'Concept(s) ajouté(s)'; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	ELSE out."nbOccurence" := '-'; out."libLog" := 'Aucune différence'; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	END CASE;	

WHEN action = 'diff' THEN --- CAS utilisé pour analyser les différences
	--- Recherche des concepts (obsevation, jdd ou entite) présent dans la source et absent dans la destination
	EXECUTE 'SELECT count(DISTINCT b."'||champRef||'") FROM '||source||' b LEFT JOIN '||destination||' a ON '||jointure||' WHERE a."'||champRef||'" IS NULL AND b."cdJdd" IN ('||listJdd||')' INTO compte; 
	CASE WHEN (compte > 0) THEN --- Si de nouveau concept sont succeptible d'être ajouté
		out."nbOccurence" := compte||' occurence(s)'; out."libLog" := 'Valeur supplémentaire : '||tableSource||' => '||tableDest; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	ELSE out."nbOccurence" := '-'; out."libLog" := 'Aucune différence'; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	END CASE;
ELSE out."libChamp" := '-'; out."libLog" := 'ERREUR : sur champ action = '||action; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
END CASE;	
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_clear 
--- Description : Nettoyage des tables (partie temporaires ou propre)
--- Variables :
--- o libSchema = Nom du schema
--- o jdd = Jeu de donnée (code du jeu ou 'data' ou 'taxa')
--- o typPartie = type de la partie à nettoyer - valeur possibles : 'propre', 'temp' (par défaut)
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
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN EXECUTE 'SELECT CASE WHEN string_agg(''''''''||"cdJdd"||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||"cdJdd"||'''''''','','') END FROM "'||libSchema||'"."'||prefixe||'metadonnees" WHERE "typJdd" = '''||jdd||''';' INTO listJdd;
ELSE listJdd := ''||jdd||'';END CASE;
--- Output&Log
out."libSchema" := libSchema;out."libTable" := '-';out."libChamp" := '-';out."typLog" := 'hub_clear';out."nbOccurence" := '-'; SELECT CURRENT_TIMESTAMP INTO out."date";
--- Commandes
CASE WHEN flag = 1 AND listJdd <> '''vide''' THEN
	FOR libTable in EXECUTE 'SELECT table_name FROM information_schema.tables WHERE table_schema = '''||libSchema||''' AND table_name NOT LIKE ''temp_%'' AND table_name NOT LIKE ''zz_%'';'
		LOOP EXECUTE 'DELETE FROM "'||libSchema||'"."'||prefixe||libTable||'" WHERE "cdJdd" IN ('||listJdd||');'; 
		END LOOP;
	---log---
	out."libLog" = jdd||' effacé de la partie '||typPartie;
WHEN listJdd = '''vide''' THEN out."libLog" = 'jdd vide '||jdd;
ELSE out."libLog" = 'ERREUR : mauvais typPartie : '||typPartie;
END CASE;
--- Output&Log
PERFORM hub_log (libSchema, out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_clone 
--- Description : Création d'un hub complet
--- Variables :
--- o libSchema = Nom du schema
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_clone(libSchema varchar) RETURNS setof zz_log AS 
$BODY$ 
DECLARE out zz_log%rowtype; 
DECLARE flag integer; 
BEGIN
--- Variable
EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = '''||libSchema||''';' INTO flag;
--- Commande
CASE WHEN flag = 1 THEN
	out."libLog" := 'Schema '||libSchema||' existe déjà';
ELSE EXECUTE '
	CREATE SCHEMA "'||libSchema||'";

	--- META : PARTIE PROPRE 
	CREATE TABLE "'||libSchema||'".metadonnees 		("cdJddPerm" character varying, "cdJdd" character varying NOT NULL,"typJdd" character varying NOT NULL,"libJdd" character varying NOT NULL,"descJdd" character varying,"rmq" character varying, 
									CONSTRAINT metadonnees_pkey PRIMARY KEY ("cdJddPerm"));
	CREATE TABLE "'||libSchema||'".metadonnees_territoire 	("cdJddPerm" character varying,"cdJdd" character varying NOT NULL,"cdGeo" character varying,"typGeo" character varying,"cdRefGeo" character varying NOT NULL,"versionRefGeo" character varying NOT NULL,"rmq" character varying, 
									CONSTRAINT metadonnees_territoire_pkey PRIMARY KEY ("cdJddPerm","typGeo","cdGeo"),CONSTRAINT metadonnees_territoire_fk FOREIGN KEY ("cdJddPerm") REFERENCES "'||libSchema||'".metadonnees ("cdJddPerm"));
	CREATE TABLE "'||libSchema||'".metadonnees_acteur 	("cdJddPerm" character varying,"cdJdd" character varying NOT NULL,"typActeur" character varying,"nomActeur" character varying,"libOrgm" character varying,"mailActeur" character varying,"cdActeur" character varying,"cdOrgm" character varying,	"rmq" character varying,
									CONSTRAINT metadonnees_acteur_pkey PRIMARY KEY ("cdJddPerm","typActeur","nomActeur","libOrgm"),CONSTRAINT metadonnees_acteur_fk FOREIGN KEY ("cdJddPerm") REFERENCES "'||libSchema||'".metadonnees ("cdJddPerm"));
	CREATE TABLE "'||libSchema||'".metadonnees_date 		("cdJddPerm" character varying,"cdJdd" character varying NOT NULL,"typAction" character varying,"dateDebut" character varying,"dateFin" character varying,"natureDate" character varying NOT NULL,"rmq" character varying,
									CONSTRAINT metadonnees_date_pkey PRIMARY KEY ("cdJddPerm","typAction","dateDebut","dateFin"),CONSTRAINT metadonnees_date_fk FOREIGN KEY ("cdJddPerm") REFERENCES "'||libSchema||'".metadonnees ("cdJddPerm"));
	--- META : PARTIE TEMPORAIRE
	CREATE TABLE "'||libSchema||'".temp_metadonnees 			("cdJddPerm" character varying,"cdJdd" character varying,"typJdd" character varying,"libJdd" character varying,"descJdd" character varying,"rmq" character varying);
	CREATE TABLE "'||libSchema||'".temp_metadonnees_territoire 	("cdJddPerm" character varying,"cdJdd" character varying,"cdGeo" character varying,"typGeo" character varying,"cdRefGeo" character varying,"versionRefGeo" character varying,"rmq" character varying);
	CREATE TABLE "'||libSchema||'".temp_metadonnees_acteur 		("cdJddPerm" character varying,"cdJdd" character varying,"typActeur" character varying,"nomActeur" character varying,"libOrgm" character varying,"mailActeur" character varying,"cdActeur" character varying,"cdOrgm" character varying,"rmq" character varying);
	CREATE TABLE "'||libSchema||'".temp_metadonnees_date 		("cdJddPerm" character varying,"cdJdd" character varying,"typAction" character varying,"dateDebut" character varying,"dateFin" character varying,"natureDate" character varying,"rmq" character varying);

	--- DATA : PARTIE PROPRE
	CREATE TABLE "'||libSchema||'".releve 			("cdJdd" character varying  NOT NULL,"cdReleve" character varying NOT NULL,"cdRelevePerm" character varying,"typReleve" character varying NOT NULL,"cdDisp" character varying,"typDisp" character varying NOT NULL,"libDisp" character varying,"descDisp" character varying,"cdHab" character varying,"cdRefHab" character varying,"versionRefHab" character varying,"cdIDCNP" character varying,"rmq" character varying,
									CONSTRAINT releve_pkey PRIMARY KEY ("cdRelevePerm"));
	CREATE TABLE "'||libSchema||'".observation 		("cdJddPerm" character varying NOT NULL,"cdJdd" character varying NOT NULL,"cdRelevePerm" character varying,"cdObsPerm" character varying,"typEnt" character varying NOT NULL,"cdObsMere" character varying NOT NULL,"cdEntMere" character varying,"nomEntMere" character varying,"cdRef" character varying NOT NULL,"cdNom" character varying,"cdRefTaxo" character varying NOT NULL,"versionRefTaxo" character varying NOT NULL,"nomEntRef" character varying NOT NULL,"cdObsOrig" character varying,"cdEntOrig" character varying,"nomEntOrig" character varying,"cdJddOrig" character varying,"libJddOrig" character varying,"typSource" character varying NOT NULL,"cdBiblio" character varying,"libBiblio" character varying,"urlBiblio" character varying,"cdHerbier" character varying,"libHerbier" character varying,"cdValidite" integer NOT NULL,"cdSensi" integer NOT NULL,"libRefSensi" character varying,"versionRefSensi" character varying,"proprieteObs" character varying NOT NULL,"statutPop" integer NOT NULL,"typDenombt" character varying,"denombtMin" integer,"denombtMax" integer,"objetDenombt" character varying,"rmq" character varying,
									CONSTRAINT observation_pkey PRIMARY KEY ("cdObsPerm"),CONSTRAINT observation_fk FOREIGN KEY ("cdJddPerm") REFERENCES "'||libSchema ||'".metadonnees ("cdJddPerm"),CONSTRAINT observation_releve_fk FOREIGN KEY ("cdRelevePerm") REFERENCES "'||libSchema||'".releve ("cdRelevePerm"));
	CREATE TABLE "'||libSchema||'".observation_territoire 	("cdJdd" character varying NOT NULL,"cdObsPerm" character varying,"cdObsMere" character varying NOT NULL,"typGeo" character varying,"cdRefGeo" character varying NOT NULL,"versionRefGeo" character varying NOT NULL,"cdGeo" character varying,"libGeo" character varying,"confianceGeo" character varying NOT NULL,"moyenGeo" character varying NOT NULL,"natureGeo" character varying NOT NULL,"origineGeo" character varying NOT NULL,"precisionGeo" float,"rmq" character varying,
									CONSTRAINT observation_territoire_pkey PRIMARY KEY ("cdObsPerm","typGeo","cdGeo"),CONSTRAINT observation_territoire_fk FOREIGN KEY ("cdObsPerm") REFERENCES "'||libSchema ||'".observation ("cdObsPerm"));
	CREATE TABLE "'||libSchema||'".observation_acteur 	("cdJdd" character varying NOT NULL,"cdObsPerm" character varying,"cdObsMere" character varying NOT NULL,"typActeur" character varying,"nomActeur" character varying,"libOrgm" character varying NOT NULL,"mailActeur" character varying,"cdActeur" character varying,"cdOrgm" character varying, "rmq" character varying,
									CONSTRAINT observation_acteur_pkey PRIMARY KEY ("cdObsPerm","typActeur","nomActeur","libOrgm"),CONSTRAINT observation_territoire_fk FOREIGN KEY ("cdObsPerm") REFERENCES "'||libSchema ||'".observation ("cdObsPerm"));
	CREATE TABLE "'||libSchema||'".observation_date 		("cdJdd" character varying NOT NULL,"cdObsPerm" character varying,"cdObsMere" character varying NOT NULL,"typAction" character varying,"dateDebut" character varying,"dateFin" character varying,"natureDate" character varying NOT NULL,"rmq" character varying,
									CONSTRAINT observation_date_pkey PRIMARY KEY ("cdObsPerm","typAction","dateDebut","dateFin"),CONSTRAINT observation_territoire_fk FOREIGN KEY ("cdObsPerm") REFERENCES "'||libSchema ||'".observation ("cdObsPerm"));
	--- DATA : PARTIE TEMPORAIRE
	CREATE TABLE "'||libSchema||'".temp_releve			("cdJdd" character varying,"cdReleve" character varying,"cdRelevePerm" character varying,"typReleve" character varying,"cdDisp" character varying,"typDisp" character varying,"libDisp" character varying,"descDisp" character varying,"cdHab" character varying,"cdRefHab" character varying,"versionRefHab" character varying,"cdIDCNP" character varying,"rmq" character varying);
	CREATE TABLE "'||libSchema||'".temp_observation 			("cdJddPerm" character varying,"cdJdd" character varying,"cdRelevePerm" character varying,"cdObsPerm" character varying,"typEnt" character varying,"cdObsMere" character varying,"cdEntMere" character varying,"nomEntMere" character varying,"cdRef" character varying,"cdNom" character varying,"cdRefTaxo" character varying,"versionRefTaxo" character varying,"nomEntRef" character varying,"cdObsOrig" character varying,"cdEntOrig" character varying,"nomEntOrig" character varying,"cdJddOrig" character varying,"libJddOrig" character varying,"typSource" character varying,"cdBiblio" character varying,"libBiblio" character varying,"urlBiblio" character varying,"cdHerbier" character varying,"libHerbier" character varying,"cdValidite" character varying,"cdSensi" character varying,"libRefSensi" character varying,"versionRefSensi" character varying,"proprieteObs" character varying,"statutPop" character varying,"typDenombt" character varying,"denombtMin" character varying,"denombtMax" character varying,"objetDenombt" character varying,"rmq" character varying);
	CREATE TABLE "'||libSchema||'".temp_observation_territoire 	("cdJdd" character varying,"cdObsPerm" character varying,"cdObsMere" character varying,"typGeo" character varying,"cdRefGeo" character varying,"versionRefGeo" character varying,"cdGeo" character varying,"libGeo" character varying,"confianceGeo" character varying,"moyenGeo" character varying,"natureGeo" character varying,"origineGeo" character varying,"precisionGeo" character varying,"rmq" character varying);
	CREATE TABLE "'||libSchema||'".temp_observation_acteur 		("cdJdd" character varying,"cdObsPerm" character varying,"cdObsMere" character varying,"typActeur" character varying,"nomActeur" character varying,"libOrgm" character varying,"mailActeur" character varying,"cdActeur" character varying,"cdOrgm" character varying,	"rmq" character varying);
	CREATE TABLE "'||libSchema||'".temp_observation_date 		("cdJdd" character varying,"cdObsPerm" character varying,"cdObsMere" character varying,"typAction" character varying,"dateDebut" character varying,"dateFin" character varying,"natureDate" character varying,"rmq" character varying);

	--- TAXA : PARTIE PROPRE
	CREATE TABLE "'||libSchema||'".entite 			("cdJddPerm" character varying NOT NULL,"cdJdd" character varying NOT NULL,"typEnt" character varying NOT NULL,"cdEntMere" character varying NOT NULL,"cdEntPerm" character varying,"nomEntMere" character varying NOT NULL,"cdSup" character varying,"cdRang" character varying,"famille" character varying,"ordre" character varying,"classe" character varying,"phylum" character varying,"regne" character varying,"rmq" character varying,
									CONSTRAINT entite_pkey PRIMARY KEY ("cdEntPerm"),CONSTRAINT entite_fk FOREIGN KEY ("cdJddPerm") REFERENCES "'||libSchema||'".metadonnees ("cdJddPerm"));
	CREATE TABLE "'||libSchema||'".entite_statut 		("cdJdd" character varying NOT NULL,"cdEntPerm" character varying,"cdEntMere" character varying NOT NULL,"typStatut" character varying,"cdStatut" character varying,"critereStatut" character varying,"methodeStatut" character varying,"anneeStatut" date,"valeurStatut" float,"metriqueStatut" character varying,"typGeo" character varying,"cdGeo" character varying,"cdRefGeo" character varying NOT NULL,"versionRefGeo" character varying NOT NULL,"libGeo" character varying,"rmq" character varying,
									CONSTRAINT entite_statut_pkey PRIMARY KEY ("cdEntPerm","typStatut","typGeo","cdGeo"),CONSTRAINT entite_statut_fk FOREIGN KEY ("cdEntPerm") REFERENCES "'||libSchema||'".entite ("cdEntPerm"));
	CREATE TABLE "'||libSchema||'".entite_referentiel 	("cdJdd" character varying NOT NULL,"cdEntPerm" character varying,"cdEntMere" character varying NOT NULL,"cdNom" character varying,"cdRef" character varying NOT NULL,"nomEntRef" character varying NOT NULL,"cdRefTaxo" character varying,"versionRefTaxo" character varying,"rmq" character varying,
									CONSTRAINT entite_referentiel_pkey PRIMARY KEY ("cdEntPerm","cdRefTaxo","versionRefTaxo"),CONSTRAINT entite_referentiel_fk FOREIGN KEY ("cdEntPerm") REFERENCES "'||libSchema||'".entite ("cdEntPerm"));
	CREATE TABLE "'||libSchema||'".entite_verna 		("cdJdd" character varying NOT NULL,"cdEntPerm" character varying,"cdEntMere" character varying NOT NULL,"nomEntVerna" character varying,"rmq" character varying,
									CONSTRAINT entite_verna_pkey PRIMARY KEY ("cdEntPerm","nomEntVerna"),CONSTRAINT entite_verna_fk FOREIGN KEY ("cdEntPerm") REFERENCES "'||libSchema||'".entite ("cdEntPerm"));
	CREATE TABLE "'||libSchema||'".entite_biblio 		("cdJdd" character varying NOT NULL,"cdEntPerm" character varying,"cdEntMere" character varying NOT NULL,"typBiblio" character varying,"cdBiblio" character varying,"libBiblio" character varying,"urlBiblio" character varying,"rmq" character varying,
									CONSTRAINT entite_biblio_pkey PRIMARY KEY ("cdEntPerm","typBiblio","cdBiblio"),CONSTRAINT entite_biblio_fk FOREIGN KEY ("cdEntPerm") REFERENCES "'||libSchema||'".entite ("cdEntPerm"));
	--- TAXA : PARTIE TEMPORAIRE
	CREATE TABLE "'||libSchema||'".temp_entite 			("cdJddPerm" character varying, "cdJdd" character varying,"typEnt" character varying,"cdEntMere" character varying,"cdEntPerm" character varying, "nomEntMere" character varying,"cdSup" character varying,"cdRang" character varying,"famille" character varying,"ordre" character varying,"classe" character varying,"phylum" character varying,"regne" character varying,"rmq" character varying);
	CREATE TABLE "'||libSchema||'".temp_entite_statut 		("cdJdd" character varying,"cdEntPerm" character varying,"cdEntMere" character varying,"typStatut" character varying,"cdStatut" character varying,"critereStatut" character varying,"methodeStatut" character varying,"anneeStatut" character varying,"valeurStatut" character varying,"metriqueStatut" character varying,"typGeo" character varying,"cdGeo" character varying,"cdRefGeo" character varying,"versionRefGeo" character varying,"libGeo" character varying,"rmq" character varying);
	CREATE TABLE "'||libSchema||'".temp_entite_referentiel 		("cdJdd" character varying,"cdEntPerm" character varying,"cdEntMere" character varying,"cdNom" character varying,"cdRef" character varying,"nomEntRef" character varying,"cdRefTaxo" character varying,"versionRefTaxo" character varying,"rmq" character varying);
	CREATE TABLE "'||libSchema||'".temp_entite_verna 		("cdJdd" character varying,"cdEntPerm" character varying,"cdEntMere" character varying,"nomEntVerna" character varying,"rmq" character varying);
	CREATE TABLE "'||libSchema||'".temp_entite_biblio 		("cdJdd" character varying,"cdEntPerm" character varying,"cdEntMere" character varying,"typBiblio" character varying,"cdBiblio" character varying,"libBiblio" character varying,"urlBiblio" character varying,"rmq" character varying);

	--- LISTE TAXON
	CREATE TABLE "'||libSchema||'".zz_log_liste_taxon  ("cdRef" character varying,"nomValide" character varying);
	CREATE TABLE "'||libSchema||'".zz_log_liste_taxon_et_infra  ("cdRefDemande" character varying,"nomValideDemande" character varying, "cdRefCite" character varying, "nomCompletCite" character varying, "rangCite" character varying, "cdTaxsupCite" character varying);

	--- LOG
	CREATE TABLE "'||libSchema||'".zz_log  ("libSchema" character varying,"libTable" character varying,"libChamp" character varying,"typLog" character varying,"libLog" character varying,"nbOccurence" character varying,"date" date);
	';
	out."libLog" := 'Schema '||libSchema||' créé';
END CASE;
--- Output&Log
out."libSchema" := libSchema;out."libTable" := '-';out."libChamp" := '-';out."typLog" := 'hub_clone';out."nbOccurence" := 1; SELECT CURRENT_TIMESTAMP INTO out."date";PERFORM hub_log (libSchema, out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_del 
--- Description : Suppression de données (fonction utilisée par une autre fonction)
--- Variables :
--- o schemaSource = Nom du schema source
--- o schemaDest = Nom du schema de destination
--- o tableSource  = Nom de la table source
--- o tableDest  = Nom de la table de destination
--- o champRef = nom du champ de référence utilisé pour tester la jointure entre la source et la destination
--- o jdd = jeu de donnée (code du jeu ou 'data' ou 'taxa')
--- o action = type d'action à réaliser - valeur possibles : 'push_total', 'push_diff' et 'diff'(par défaut)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION hub_del(schemaSource varchar,schemaDest varchar, tableSource varchar, tableDest varchar, champRef varchar, jdd varchar, action varchar = 'diff') RETURNS setof zz_log  AS 
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
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN EXECUTE 'SELECT CASE WHEN string_agg(''''''''||"cdJdd"||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||"cdJdd"||'''''''','','') END FROM "'||schemaSource||'"."'||metasource||'" WHERE "typJdd" = '''||jdd||''';' INTO listJdd;
ELSE listJdd := ''||jdd||'';END CASE;

CASE WHEN champRef = 'cdJddPerm' THEN typJdd = 'meta';flag := 1;
WHEN champRef = 'cdObsPerm' THEN typJdd = 'data';flag := 1;
WHEN champRef = 'cdEntPerm' THEN typJdd = 'taxa';flag := 1;
ELSE flag := 0;
END CASE;
source := '"'||schemaSource||'"."'||tableSource||'"';
destination := '"'||schemaDest||'"."'||tableDest||'"';
EXECUTE 'SELECT string_agg(''a."''||cd||''" = b."''||cd||''"'','' AND '') FROM ref.fsd_'||typJdd||' WHERE (tbl_name = '''||tableSource||''' OR tbl_name = '''||tableDest||''') AND unicite = ''Oui''' INTO jointure;
--- Output&Log
out."libSchema" := schemaSource; out."libTable" := tableSource; out."libChamp" := '-';out."typLog" := 'hub_del'; SELECT CURRENT_TIMESTAMP INTO out."date";

--- Commande
--- Recherche des concepts (obsevation, jdd ou entite) présent dans la partie propre et présent dans la partie temporaire
EXECUTE 'SELECT count(DISTINCT b."'||champRef||'") FROM '||source||' b JOIN '||destination||' a ON '||jointure||' WHERE b."cdJdd" IN ('||listJdd||')' INTO compte; 
	
CASE WHEN (compte > 0) THEN --- Si de nouveau concept sont succeptible d'être ajouté
	out."nbOccurence" := compte||' occurence(s)'; ---log
	CASE WHEN action = 'push_diff' THEN
		EXECUTE 'SELECT string_agg(''''||'||champRef||'||'''','','') FROM '||source INTO listeChamp1;
		EXECUTE 'DELETE FROM '||destination||' WHERE "'||champRef||'" IN ('||listeChamp1||')';
		out."nbOccurence" := compte||' occurence(s)';out."libLog" := 'Concepts supprimés'; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	WHEN action = 'diff' THEN
		out."nbOccurence" := compte||' occurence(s)';out."libLog" := 'Concepts à supprimer'; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	ELSE out."nbOccurence" := compte||' occurence(s)'; out."libLog" := 'ERREUR : sur champ action = '||action; PERFORM hub_log (schemaSource, out);RETURN NEXT out;
	END CASE;
ELSE out."libLog" := 'Aucune différence';out."nbOccurence" := '-'; PERFORM hub_log (schemaSource, out); RETURN NEXT out;
END CASE;	
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_diff 
--- Description : Analyse des différences entre une source et une cible
--- Variables :
--- o libSchema = Nom du schema à analyser
--- o jdd = jeu de donnée (code du jeu ou 'data' ou 'taxa')
--- o typeAction = type d'action à réaliser - valeur possibles : 'del' et 'add'(par défaut)
--- o mode = mode d'utilisation - valeur possibles : '2' = inter-schema et '1'(par défaut) = intra-schema
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_diff(libSchema varchar, jdd varchar,typAction varchar = 'add',mode integer = 1) RETURNS setof zz_log  AS 
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
	champRef = 'cdObsPerm'; tableRef = 'observation'; flag := 1;
WHEN jdd = 'taxa' THEN 
	champRef = 'cdEntPerm';	tableRef = 'entite'; flag := 1;
ELSE 
	EXECUTE 'SELECT "typJdd" FROM "'||libSchema||'".temp_metadonnees WHERE "cdJdd" = '''||jdd||''';' INTO typJdd;
	CASE WHEN typJdd = 'data' THEN champRef = 'cdObsPerm'; tableRef = 'observation'; flag := 1;
	WHEN typJdd = 'taxa' THEN champRef = 'cdEntPerm';	tableRef = 'entite'; flag := 1;
	ELSE flag := 0;
	END CASE;
END CASE;
--- mode 1 = intra Shema / mode 2 = entre shema et agregation
CASE WHEN mode = 1 THEN schemaSource :=libSchema; schemaDest :=libSchema; WHEN mode = 2 THEN schemaSource :=libSchema; schemaDest :='agregation'; ELSE flag :=0; END CASE;
--- Commandes
CASE WHEN typAction = 'add' AND flag = 1 THEN
	--- Metadonnees
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE ''metadonnees%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM  hub_add(schemaSource,schemaDest, tableSource, tableDest ,'cdJddPerm' , jdd ,'diff'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
		SELECT * INTO out FROM  hub_update(schemaSource,schemaDest, tableSource, tableDest ,'cdJddPerm', jdd,'diff'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
		SELECT * INTO out FROM  hub_add(schemaDest,schemaSource, tableDest, tableSource ,'cdJddPerm', jdd,'diff'); --- sens inverse (champ présent dans le propre et absent dans le temporaire)
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
	--- Données
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE '''||tableRef||'%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM  hub_add(schemaSource,schemaDest, tableSource, tableDest ,champRef, jdd,'diff'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
		SELECT * INTO out FROM  hub_update(schemaSource,schemaDest, tableSource, tableDest ,champRef, jdd,'diff'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
		SELECT * INTO out FROM  hub_add(schemaDest,schemaSource, tableDest, tableSource ,champRef, jdd,'diff'); --- sens inverse (champ présent dans le propre et absent dans le temporaire)
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
WHEN typAction = 'del' AND flag = 1 THEN
	--- Metadonnees
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE ''metadonnees%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM  hub_del(schemaSource,schemaDest, tableSource, tableDest ,'cdJddPerm' , jdd ,'diff'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE '''||tableRef||'%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM  hub_del(schemaSource,schemaDest, tableSource, tableDest ,champRef, jdd,'diff'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
ELSE out."libTable" := libTable; out."libLog" := jdd||' n''est pas un jeu de données valide'; out."nbOccurence" := '-'; PERFORM hub_log (libSchema, out);RETURN NEXT out;
END CASE;

END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_drop 
--- Description : Supprimer un hub dans sa totalité
--- Variables :
--- o libSchema = Nom du schema à analyser
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
	out."libLog" := 'Schema '||libSchema||' supprimé';
ELSE out."libLog" := 'Schema '||libSchema||' inexistant pas dans le Hub';
END CASE;
RETURN next out;
--- Output&Log
out."libSchema" := libSchema;out."libTable" := '-';out."libChamp" := '-';out."typLog" := 'hub_drop';out."nbOccurence" := 1;SELECT CURRENT_TIMESTAMP INTO out."date";PERFORM hub_log (libSchema, out);RETURN NEXT out;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_export 
--- Description : Exporter les données depuis un hub
--- Variables :
--- o libSchema = Nom du schema à analyser
--- o jdd = jeu de donnée (code du jeu ou 'data' ou 'taxa')
--- o path = chemin vers le dossier dans lequel on souhaite exporter les données
--- o format = format d'export - valeur possibles : 'sinp' et 'fcbn'(par défaut)
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
CASE 	WHEN Jdd = 'data' OR Jdd = 'taxa' THEN 	
	typJdd := Jdd;
	EXECUTE 'SELECT CASE WHEN string_agg(''''''''||"cdJdd"||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||"cdJdd"||'''''''','','') END FROM "'||libSchema||'"."temp_metadonnees" WHERE "typJdd" = '''||jdd||''';' INTO listJdd;
ELSE
	EXECUTE 'SELECT "typJdd" FROM "'||libSchema||'".temp_metadonnees WHERE "cdJdd" = '''||Jdd||''';' INTO typJdd; 
	listJdd := ''||jdd||'';
END CASE;
--- Output&Log
out."libSchema" := libSchema;out."libTable" := '-';out."libChamp" := '-';out."typLog" := 'hub_export';out."nbOccurence" := 1; SELECT CURRENT_TIMESTAMP INTO out."date";
--- Commandes
CASE WHEN format = 'fcbn' THEN
	FOR libTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_'||typJdd
		LOOP EXECUTE 'COPY (SELECT * FROM  "'||libSchema||'"."'||libTable||'" WHERE "cdJdd" IN ('||listJdd||')) TO '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';'; END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_meta'
		LOOP EXECUTE 'COPY (SELECT * FROM  "'||libSchema||'"."'||libTable||'" WHERE "cdJdd" IN ('||listJdd||')) TO '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';'; END LOOP;
	out."libLog" :=  Jdd||'exporté au format '||format;
WHEN format = 'sinp' THEN
	out."libLog" :=  'format SINP à implémenter';
ELSE out."libLog" :=  'format non implémenté : '||format;
END CASE;
PERFORM hub_log (libSchema, out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_help 
--- Description : Création de l'aide et Accéder à la description d'un fonction
--- Variables :
--- o libFonction = Nom de la fonction à décrire (par défaut, 'all' ==> liste routes les fonctions)
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
FOR testFonction IN EXECUTE 'SELECT id FROM ref.help'
	LOOP 
		 CASE WHEN testFonction = libFonction THEN flag := 1; ELSE EXECUTE 'SELECT 1;'; END CASE; 
	END LOOP;
--- Commande
CASE WHEN libFonction = 'all' THEN
	out := '- Pour accéder à la description d''une fonction : ';RETURN next out;
	out := '   SELECT * FROM hub_help(''fonction'');';RETURN next out;
	out := '- Pour utiliser une fonction : ';RETURN next out;
	out := '  SELECT * FROM fonction(''variables'');';RETURN next out;
	FOR testFonction IN EXECUTE 'SELECT id FROM ref.help'
		LOOP lesvariables := '(';
		FOR var IN EXECUTE 'SELECT id FROM ref.help_var WHERE "'||testFonction||'" = ''oui'';'
			LOOP lesvariables := lesvariables||var||','; END LOOP;
		EXECUTE 'SELECT trim(trailing '','' FROM '''||lesvariables||''')||'')''' INTO lesvariables;
		out := 'SELECT * FROM '||testFonction||lesvariables;RETURN next out; END LOOP;
WHEN flag = 1 THEN
	out := '-------------------------'; RETURN next out; 
	out := 'Nom de la Fonction = '||libFonction;RETURN next out; 
	EXECUTE 'SELECT ''- Description : ''||"description" FROM ref.help WHERE "id" = '''||libFonction||''';'INTO out;RETURN next out; 
	EXECUTE 'SELECT ''- Type : ''||"type" FROM ref.help WHERE "id" = '''||libFonction||''';' INTO out;RETURN next out; 
	EXECUTE 'SELECT ''- Etat de la fonction : ''||"etat" FROM ref.help WHERE "id" = '''||libFonction||''';'INTO out;RETURN next out;
	EXECUTE 'SELECT ''- Amélioration à prevoir : ''||"amelioration" FROM ref.help WHERE "id" = '''||libFonction||''';'INTO out;RETURN next out;
	out := '-------------------------'; RETURN next out; 
	out := 'Liste des variables :';RETURN next out;
	FOR var IN EXECUTE 'SELECT '' o ''||"id"||'' : ''||"description"||''. Valeurs possibles = ("''||valeurs||''")'' FROM ref.help_var WHERE "'||libFonction||'" = ''oui'';'
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
--- Variables :
--- o libSchema = Nom du schema
--- o nomDomaine = nom du domaine utilisé pour produire l'identifiant permanent (avec http://)
--- o jdd = jeu de donnée (code du jeu ou 'data' ou 'taxa')
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_idPerm(libSchema varchar, nomDomaine varchar, jdd varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE typJdd varchar;
DECLARE champMere varchar;
DECLARE champRef varchar;
DECLARE tableRef varchar;
DECLARE listTable varchar;
DECLARE listPerm varchar;
BEGIN
--- Variables
EXECUTE 'SELECT typJdd FROM temp_metadonnees WHERE cdJdd = '''||jdd||'''' INTO typJdd;
CASE WHEN typJdd = 'meta' 	THEN champMere = 'cdJdd'; 	champRef = 'cdJddPerm';	tableRef = 'metadonnees'; 
	WHEN typJdd = 'data' 	THEN champMere = 'cdObsMere';	champRef = 'cdObsPerm';	tableRef = 'observation'; 
	WHEN typJdd = 'taxa' 	THEN champMere = 'cdEntMere';	champRef = 'cdEntPerm';	tableRef = 'entite'; 
				ELSE champMere = ''; 		champRef = ''; 		tableRef = '';END CASE;
--- Output
out."libSchema" := libSchema;out."libTable" := tableRef;out."libChamp" := champRef;out."typLog" := 'hub_idPerm';out."nbOccurence" := 1; SELECT CURRENT_TIMESTAMP INTO out."date";out."libLog" := 'Identifiant permanent produit';
--- Commandes
FOR listPerm IN EXECUTE 'SELECT DISTINCT "'||champMere||'" FROM "'||libSchema||'".temp_'||tableRef||' WHERE "cdJdd" = '''||cdjdd||''')' --- Production de l'idPermanent
	LOOP EXECUTE 'UPDATE "'||libSchema||'"."temp_'||tableRef||'" SET ("'||champRef||'") = ('''||nomdomaine||'/'||champRef||'/''||(SELECT uuid_generate_v4())) WHERE "'||champMere||'" = '''||listPerm||''' AND "cdJdd" = '''||cdJdd||''';';
	out."libTable" := null; PERFORM hub_log (libSchema, out);RETURN next out;
	END LOOP;	
FOR listTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_'||typJdd||' WHERE tbl_name <> '''||tableRef||''''  --- Peuplement du nouvel idPermanent dans les autres tables
	LOOP EXECUTE 'UPDATE "'||libSchema||'"."temp_'||listTable||'" ot SET "'||champRef||'" = o."'||champRef||'" FROM "'||libSchema||'"."temp_'||tableRef||'" o WHERE o."cdJdd" = ot."cdJdd" AND o."'||champMere||'" = ot."'||champMere||'";';
	out."libTable" := listTable; PERFORM hub_log (libSchema, out);RETURN next out;
	END LOOP;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_import 
--- Description : Importer des données (fichiers CSV) dans un hub
--- Variables :
--- o libSchema = Nom du schema
--- o jdd = jeu de donnée (code du jeu ou 'data' ou 'taxa')
--- o path = chemin vers le dossier dans lequel on souhaite exporter les données
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_import(libSchema varchar, jdd varchar, path varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE libTable varchar;
DECLARE out zz_log%rowtype;
DECLARE i varchar;
BEGIN
--- Commande
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN 
	FOR libTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_'||jdd||';'
		LOOP EXECUTE 'COPY "'||libSchema||'".temp_'||libTable||' FROM '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';'; END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT tbl_name FROM ref.fsd_meta;'
		LOOP EXECUTE 'COPY "'||libSchema||'".temp_'||libTable||' FROM '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER '';'' ENCODING ''UTF8'';'; END LOOP;
	out."libLog" := jdd||' importé depuis '||path;
WHEN jdd = 'listTaxon' THEN 
	EXECUTE 'TRUNCATE TABLE "'||libSchema||'".zz_log_liste_taxon;TRUNCATE TABLE "'||libSchema||'".zz_log_liste_taxon_et_infra;';
	EXECUTE 'COPY "'||libSchema||'".zz_log_liste_taxon FROM '''||path||'std_listTaxon.csv'' HEADER CSV DELIMITER ''	'' ENCODING ''UTF8'';';
	FOR i in EXECUTE 'select "cdRef" from "'||libSchema||'".zz_log_liste_taxon' 
		LOOP  
		EXECUTE
			'INSERT INTO "'||libSchema||'".zz_log_liste_taxon_et_infra ("cdRefDemande", "nomValideDemande", "cdRefCite", "nomCompletCite","cdTaxsupCite","rangCite")
			select '''||i||''' as cdRefDemande, '''' as nomValideDemande, foo.* from 
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
	EXECUTE 'update  "'||libSchema||'".zz_log_liste_taxon_et_infra set "nomValideDemande" = "nomValide" from "'||libSchema||'".zz_log_liste_taxon where zz_log_liste_taxon_et_infra."cdRefDemande"= zz_log_liste_taxon."cdRef" ' ;
	out."libLog" := jdd||' importé depuis '||path;
ELSE out."libLog" := 'Problème identifié dans le jdd (ni data, ni taxa,ni meta)'; END CASE;

--- Output&Log
out."libSchema" := libSchema;out."libChamp" := '-';out."libTable" := '-';out."typLog" := 'hub_import';out."nbOccurence" := 1; SELECT CURRENT_TIMESTAMP INTO out."date";PERFORM hub_log (libSchema, out);RETURN next out;
END; $BODY$  LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_install 
--- Description : Installe le hub en local (concataine la construction d'un hub et l'installation des référentiels)
--- Variables :
--- o libSchema = Nom du schema
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_install (libSchema varchar, path varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
BEGIN
CREATE TABLE IF NOT EXISTS "public".zz_log  ("libSchema" character varying,"libTable" character varying,"libChamp" character varying,"typLog" character varying,"libLog" character varying,"nbOccurence" character varying,"date" date);
SELECT * INTO out FROM hub_clone(libSchema);PERFORM hub_log (libSchema, out);RETURN NEXT out;
SELECT * INTO out FROM hub_ref('create',path);PERFORM hub_log (libSchema, out);RETURN NEXT out;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_pull 
--- Description : Récupération d'un jeu de données depuis la partie propre vers la partie temporaire
--- Variables :
--- o libSchema = Nom du schema
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
CASE WHEN jdd = 'data' THEN champRef = 'cdObsPerm'; tableRef = 'observation'; flag := 1;
	WHEN jdd = 'taxa' THEN champRef = 'cdEntPerm';	tableRef = 'entite'; flag := 1;
	ELSE EXECUTE 'SELECT "typJdd" FROM "'||libSchema||'".temp_metadonnees WHERE "cdJdd" = '''||jdd||''';' INTO typJdd;
		CASE WHEN typJdd = 'data' THEN champRef = 'cdObsPerm'; tableRef = 'observation'; flag := 1;
			WHEN typJdd = 'taxa' THEN champRef = 'cdEntPerm';	tableRef = 'entite'; flag := 1;
			ELSE flag := 0;
		END CASE;
	END CASE;
--- mode 1 = intra Shema / mode 2 = entre shema et agregation
CASE WHEN mode = 1 THEN schemaSource :=libSchema; schemaDest :=libSchema; WHEN mode = 2 THEN schemaSource :=libSchema; schemaDest :='agregation'; ELSE flag :=0; END CASE;

--- Commandes
--- Remplacement total (NB : equivalent au push 'replace' mais dans l'autre sens)
CASE WHEN flag = 1 THEN
	SELECT * INTO out FROM hub_clear(libSchema, jdd, 'temp'); return next out;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE ''metadonnees%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP
		CASE WHEN mode = 1 THEN tableSource := libTable; tableDest := 'temp_'||libTable; WHEN mode = 2 THEN tableSource := 'temp_'||libTable; tableDest := libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest ,'cdJddPerm', jdd, 'push_total'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE '''||tableRef||'%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := libTable; tableDest := 'temp_'||libTable; WHEN mode = 2 THEN tableSource := 'temp_'||libTable; tableDest := libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest , champRef, jdd, 'push_total'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
ELSE ---Log
	out."libSchema" := libSchema; out."libChamp" := '-'; out."typLog" := 'hub_pull';SELECT CURRENT_TIMESTAMP INTO out."date"; out."libLog" := 'ERREUR : sur champ jdd = '||jdd; PERFORM hub_log (libSchema, out);RETURN NEXT out;
END CASE;
END; $BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_push 
--- Description : Mise à jour des données (on pousse les données)
--- Variables :
--- o libSchema = Nom du schema à analyser
--- o jdd = jeu de donnée (code du jeu ou 'data' ou 'taxa')
--- o typAction = type d'action à réaliser - valeur possibles : 'del', 'add' et 'replace'(par défaut)
--- o mode = mode d'utilisation - valeur possibles : '2' = inter-schema et '1'(par défaut) = intra-schema
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_push(libSchema varchar,jdd varchar, typAction varchar = 'replace', mode integer = 1) RETURNS setof zz_log AS 
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
CASE WHEN jdd = 'data' THEN champRef = 'cdObsPerm'; tableRef = 'observation'; flag := 1;
	WHEN jdd = 'taxa' THEN champRef = 'cdEntPerm';	tableRef = 'entite'; flag := 1;
	ELSE EXECUTE 'SELECT "typJdd" FROM "'||libSchema||'".temp_metadonnees WHERE "cdJdd" = '''||jdd||''';' INTO typJdd;
		CASE WHEN typJdd = 'data' THEN champRef = 'cdObsPerm'; tableRef = 'observation'; flag := 1;
			WHEN typJdd = 'taxa' THEN champRef = 'cdEntPerm';	tableRef = 'entite'; flag := 1;
			ELSE flag := 0;
		END CASE;
	END CASE;
--- mode 1 = intra Shema / mode 2 = entre shema et agregation
CASE WHEN mode = 1 THEN schemaSource :=libSchema; schemaDest :=libSchema; WHEN mode = 2 THEN schemaSource :=libSchema; schemaDest :='agregation'; ELSE flag :=0; END CASE;

--- Commandes
--- Remplacement total
CASE WHEN typAction = 'replace' AND flag = 1 THEN
	SELECT * INTO out FROM hub_clear(libSchema, jdd, 'propre'); return next out;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE ''metadonnees%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest ,'cdJddPerm', jdd, 'push_total'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE '''||tableRef||'%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest , champRef, jdd, 'push_total'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;

--- Ajout de la différence
WHEN typAction = 'add' AND flag = 1 THEN
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE ''metadonnees%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest ,'cdJddPerm', jdd, 'push_diff'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
		SELECT * INTO out FROM hub_update(schemaSource,schemaDest, tableSource, tableDest ,'cdJddPerm', jdd, 'push_diff'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE '''||tableRef||'%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_add(schemaSource,schemaDest, tableSource, tableDest ,champRef, jdd, 'push_diff'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
		SELECT * INTO out FROM hub_update(schemaSource,schemaDest, tableSource, tableDest ,champRef, jdd, 'push_diff');
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;

--- Suppression de l'existant de la partie temporaire
WHEN typAction = 'del' AND flag = 1 THEN
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE ''metadonnees%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_del(schemaSource,schemaDest, tableSource, tableDest ,'cdJddPerm', jdd, 'push_diff'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
	FOR libTable in EXECUTE 'SELECT DISTINCT table_name FROM information_schema.tables WHERE table_name LIKE '''||tableRef||'%'' AND table_schema = '''||libSchema||''' ORDER BY table_name;' LOOP 
		CASE WHEN mode = 1 THEN tableSource := 'temp_'||libTable; tableDest := libTable; WHEN mode = 2 THEN tableSource := libTable; tableDest := 'temp_'||libTable; END CASE;
		SELECT * INTO out FROM hub_del(schemaSource,schemaDest, tableSource, tableDest ,champRef, jdd, 'push_diff'); 
			CASE WHEN out."nbOccurence" <> '-' THEN RETURN NEXT out; ELSE SELECT 1 INTO nothing; END CASE;
	END LOOP;
ELSE ---Log
	out."libSchema" := libSchema; out."libChamp" := '-'; out."typLog" := 'hub_push';SELECT CURRENT_TIMESTAMP INTO out."date"; out."libLog" := 'ERREUR : sur champ action = '||jdd; PERFORM hub_log (libSchema, out);RETURN NEXT out;
END CASE;
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_ref 
--- Description : Création des référentiels
--- Variables :
--- o typAction = type d'action à réaliser - valeur possibles : 'del', 'add' et 'replace'(par défaut)
--- o path = chemin vers le dossier dans lequel on souhaite exporter les données
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
out."libSchema" := '-';out."libTable" := '-';out."libChamp" := '-';out."typLog" := 'hub_ref';out."nbOccurence" := 1; SELECT CURRENT_TIMESTAMP INTO out."date";
---Variables
DROP TABLE IF  EXISTS public.ref_meta;CREATE TABLE public.ref_meta(id varchar, delimitr varchar, structure varchar, CONSTRAINT ref_meta_pk PRIMARY KEY(id));
INSERT INTO public.ref_meta VALUES 
('fsd_meta',',','(id serial NOT NULL, tbl_order integer, tbl_name character varying, pos character varying, cd character varying, lib character varying, format character varying,obligation character varying, unicite character varying, regle character varying, CONSTRAINT fsd_meta_pkey PRIMARY KEY (id))'),
('fsd_data',',','(id serial NOT NULL, tbl_order integer, tbl_name character varying, pos character varying, cd character varying, lib character varying, format character varying,obligation character varying, unicite character varying, regle character varying, CONSTRAINT fsd_data_pkey PRIMARY KEY (id))'),
('fsd_taxa',',','(id serial NOT NULL, tbl_order integer, tbl_name character varying, pos character varying, cd character varying, lib character varying, format character varying,obligation character varying, unicite character varying, regle character varying, CONSTRAINT fsd_taxa_pkey PRIMARY KEY (id))'),
('help',',','("id" varchar,"type" varchar,"description" varchar, "etat" varchar, "amelioration" varchar, CONSTRAINT pk_help PRIMARY KEY ("id"))'),
('help_var',';','("id" varchar,"description" varchar, "valeurs" varchar, "hub_checkout" varchar,"hub_clear" varchar,"hub_clone" varchar,"hub_diff" varchar,"hub_drop" varchar,"hub_export" varchar,"hub_extract" varchar,"hub_help" varchar,"hub_idPerm" varchar,"hub_import" varchar,"hub_install" varchar,"hub_pull" varchar,"hub_push" varchar,"hub_ref" varchar,"hub_verif" varchar,"hub_verif_all" varchar,CONSTRAINT pk_help_var PRIMARY KEY ("id"))'),
('taxref_v2','\t','("ogc_fid" integer, "regne" character varying, "phylum" character varying, "classe" character varying, "ordre" character varying, "famille" character varying, cd_nom character varying, lb_nom character varying, lb_auteur character varying, nom_complet character varying, cd_ref character varying, nom_valide character varying, rang character varying, nom_vern character varying, nom_vern_eng character varying, fr character varying, mar character varying, gua character varying, smsb character varying, gf character varying, spm character varying, reu character varying, may character varying, taaf character varying, nom_complet_sans_date character varying, CONSTRAINT refv20_utf8_pk PRIMARY KEY (ogc_fid))'),
('taxref_v3','\t','(ogc_fid integer, regne character varying, phylum character varying, classe character varying, ordre character varying, famille character varying, cd_nom character varying, cd_taxsup character varying, cd_ref character varying, rang character varying, lb_nom character varying, lb_auteur character varying, nom_valide character varying, nom_vern character varying, nom_vern_eng character varying, habitat character varying, fr character varying, gf character varying, mar character varying, gua character varying, smsb character varying, spm character varying, may character varying, epa character varying, reu character varying, taaf character varying, nc character varying, wf character varying, pf character varying, cli character varying, nom_complet character varying, nom_complet_sans_date character varying, CONSTRAINT taxrefv30_utf8_pk PRIMARY KEY (ogc_fid))'),
('taxref_v4','\t','(ogc_fid integer, regne character varying, phylum character varying, classe character varying, ordre character varying, famille character varying, cd_nom character varying, cd_taxsup character varying, cd_ref character varying, rang character varying, lb_nom character varying, lb_auteur character varying, nom_complet character varying, nom_valide character varying, nom_vern character varying, nom_vern_eng character varying, habitat character varying, fr character varying, gf character varying, mar character varying, gua character varying, sm character varying, sb character varying, spm character varying, may character varying, epa character varying, reu character varying, taaf character varying, pf character varying, nc character varying, wf character varying, cli character varying, aphia_id character varying, nom_complet_sans_date character varying, CONSTRAINT taxrefv40_utf8_pk PRIMARY KEY (ogc_fid))'),
('taxref_v5','\t','(ogc_fid integer, regne character varying, phylum character varying, classe character varying, ordre character varying, famille character varying, cd_nom character varying, cd_taxsup character varying, cd_ref character varying, rang character varying, lb_nom character varying, lb_auteur character varying, nom_complet character varying, nom_valide character varying, nom_vern character varying, nom_vern_eng character varying, habitat character varying, fr character varying, gf character varying, mar character varying, gua character varying, sm character varying, sb character varying, spm character varying, may character varying, epa character varying, reu character varying, taaf character varying, pf character varying, nc character varying, wf character varying, cli character varying, url character varying, nom_complet_sans_date character varying, CONSTRAINT taxrefv50_utf8_pk PRIMARY KEY (ogc_fid))'),
('taxref_v6','\t','(ogc_fid integer, regne character varying, phylum character varying, classe character varying, ordre character varying, famille character varying, cd_nom character varying, cd_taxsup character varying, cd_ref character varying, rang character varying, lb_nom character varying, lb_auteur character varying, nom_complet character varying, nom_valide character varying, nom_vern character varying, nom_vern_eng character varying, habitat character varying, fr character varying, gf character varying, mar character varying, gua character varying, sm character varying, sb character varying, spm character varying, may character varying, epa character varying, reu character varying, taaf character varying, pf character varying, nc character varying, wf character varying, cli character varying, url character varying, nom_complet_sans_date character varying, CONSTRAINT taxrefv60_utf8_pk PRIMARY KEY (ogc_fid))'),
('taxref_v7','\t','(ogc_fid integer, regne character varying, phylum character varying, classe character varying, ordre character varying, famille character varying, group1_inpn character varying, group2_inpn character varying, cd_nom character varying, cd_taxsup character varying, cd_ref character varying, rang character varying, lb_nom character varying, lb_auteur character varying, nom_complet character varying, nom_valide character varying, nom_vern character varying, nom_vern_eng character varying, habitat character varying, fr character varying, gf character varying, mar character varying, gua character varying, sm character varying, sb character varying, spm character varying, may character varying, epa character varying, reu character varying, taaf character varying, pf character varying, nc character varying, wf character varying, cli character varying, url character varying, nom_complet_sans_date character varying, CONSTRAINT taxrefv70_utf8_pk PRIMARY KEY (ogc_fid))'),
('taxref_v8','\t','(ogc_fid integer, regne character varying, phylum character varying, classe character varying, ordre character varying, famille character varying, group1_inpn character varying, group2_inpn character varying, cd_nom character varying, cd_taxsup character varying, cd_ref character varying, rang character varying, lb_nom character varying, lb_auteur character varying, nom_complet character varying, nom_valide character varying, nom_vern character varying, nom_vern_eng character varying, habitat character varying, fr character varying, gf character varying, mar character varying, gua character varying, sm character varying, sb character varying, spm character varying, may character varying, epa character varying, reu character varying, taaf character varying, pf character varying, nc character varying, wf character varying, cli character varying, url character varying, nom_complet_html character varying, nom_complet_sans_date character varying, CONSTRAINT taxrefv80_utf8_pk PRIMARY KEY (ogc_fid))'),
('geo_maille10','\t','(gid integer,  cd_sig character varying(17) NOT NULL,  code10km character varying(10),  geom geometry(MultiPolygon,2154),  geom_3857 geometry(MultiPolygon,3857),  CONSTRAINT l93_10k_pkey PRIMARY KEY (cd_sig))'),
('geo_maille5','\t', '(gid integer,  cd_sig character varying(21) NOT NULL,  code5km character varying(10),  geom geometry(MultiPolygon,2154),  geom_3857 geometry(MultiPolygon,3857),  CONSTRAINT l93_5k_pkey PRIMARY KEY (cd_sig))'),
('geo_commune','\t', '(gid integer,  id_bdcarto numeric,  nom_comm character varying(254),  insee_comm character varying(254) NOT NULL,  statut character varying(254),  x_commune integer,  y_commune integer,  superficie numeric,  population integer,  insee_cant character varying(254),  insee_arr character varying(254),  nom_dept character varying(254),  insee_dept character varying(254),  nom_region character varying(254),  insee_reg character varying(254),  geom geometry(MultiPolygon,2154),  geom_3857 geometry(MultiPolygon,3857),  geom_3857_s500 geometry(MultiPolygon,3857),  geom_3857_s1000 geometry(MultiPolygon,3857),  geom_3857_s100 geometry(MultiPolygon,3857),  CONSTRAINT communes_bdcart2011_fcbn_pkey PRIMARY KEY (insee_comm))'),
('geo_maille10_zee_974','\t','(gid integer,  cd_sig character varying(21) NOT NULL,  code_10km character varying(9),  geom geometry(MultiPolygon,2975),  CONSTRAINT geo_maille10_zee_974_pkey PRIMARY KEY (cd_sig))'),
('geo_maille1_utm1','\t','(gid integer ,  nom_maille character varying(8) NOT NULL,  centroid_x character varying(8), centroid_y character varying(8),  geom geometry(MultiPolygon,2975),  geom_3857_s100 geometry(MultiPolygon,3857),  geom_3857 geometry(MultiPolygon,3857),  CONSTRAINT geo_maille1_utm1_pkey PRIMARY KEY (nom_maille))')
;

--- Commandes 
CASE WHEN typAction = 'drop' THEN	--- Suppression
	EXECUTE 'SELECT DISTINCT 1 FROM information_schema.schemata WHERE schema_name = ''ref''' INTO flag1;
	CASE WHEN flag1 = 1 THEN DROP SCHEMA IF EXISTS ref CASCADE; out."libLog" := 'Shema ref supprimé';RETURN next out;
	ELSE out."libLog" := 'Schéma ref inexistant';RETURN next out;END CASE;
WHEN typAction = 'delete' THEN	--- Suppression
	EXECUTE 'SELECT DISTINCT 1 FROM information_schema.schemata WHERE schema_name = ''ref''' INTO flag1;
	CASE WHEN flag1 = 1 THEN
		FOR libTable IN EXECUTE 'SELECT id FROM public.ref_meta'
		LOOP EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = ''ref'' AND tablename = '''||libTable||''';' INTO flag2;
		CASE WHEN flag2 = 1 THEN 
			EXECUTE 'DROP TABLE ref."'||libTable||'" CASCADE';  out."libLog" := 'Table '||libTable||' supprimée';RETURN next out;
		ELSE out."libLog" := 'Table '||libTable||' inexistante';
		END CASE;
		END LOOP;
	ELSE out."libLog" := 'Schéma ref inexistant';RETURN next out;END CASE;
WHEN typAction = 'create' THEN	--- Creation
	EXECUTE 'SELECT DISTINCT 1 FROM information_schema.schemata WHERE schema_name =  ''ref''' INTO flag1;
	CASE WHEN flag1 = 1 THEN out."libLog" := 'Schema ref déjà créés';RETURN next out;ELSE CREATE SCHEMA "ref"; out."libLog" := 'Schéma ref créés';RETURN next out;END CASE;
	--- Tables
	FOR libTable IN EXECUTE 'SELECT id FROM public.ref_meta'
		LOOP EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = ''ref'' AND tablename = '''||libTable||''';' INTO flag2;
		CASE WHEN flag2 = 1 THEN 
			out."libLog" := libTable||' a déjà été créée' ;RETURN next out;
		ELSE EXECUTE 'SELECT structure FROM public.ref_meta WHERE id = '''||libTable||'''' INTO structure;
		EXECUTE 'SELECT delimitr FROM public.ref_meta WHERE id = '''||libTable||'''' INTO delimitr;
		EXECUTE 'CREATE TABLE ref.'||libTable||' '||structure||';'; out."libLog" := libTable||' créée';RETURN next out;
		EXECUTE 'COPY ref.'||libTable||' FROM '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER E'''||delimitr||''' ENCODING ''UTF8'';';
		out."libLog" := libTable||' : données importées';RETURN next out;
		END CASE;
		END LOOP;
WHEN typAction = 'update' THEN	--- Mise à jour
	EXECUTE 'SELECT DISTINCT 1 FROM information_schema.schemata WHERE schema_name =  ''ref''' INTO flag1;
	CASE WHEN flag1 = 1 THEN out."libLog" := 'Schema ref déjà créés';RETURN next out;ELSE CREATE SCHEMA "ref"; out."libLog" := 'Schéma ref créés';RETURN next out;END CASE;
	FOR libTable IN EXECUTE 'SELECT id FROM public.ref_meta'
		LOOP 
		EXECUTE 'SELECT DISTINCT 1 FROM pg_tables WHERE schemaname = ''ref'' AND tablename = '''||libTable||''';' INTO flag2;
		EXECUTE 'SELECT delimitr FROM public.ref_meta WHERE id = '''||libTable||'''' INTO delimitr;
		CASE WHEN flag2 = 1 THEN
			EXECUTE 'TRUNCATE ref.'||libTable;
			EXECUTE 'COPY ref.'||libTable||' FROM '''||path||'std_'||libTable||'.csv'' HEADER CSV DELIMITER E'''||delimitr||''' ENCODING ''UTF8'';';
			out."libLog" := 'Mise à jour de la table '||libTable;RETURN next out;
		ELSE out."libLog" := 'Les tables doivent être créée auparavant : SELECT * FROM hub_ref(''create'',path)';RETURN next out;
		END CASE;
	END LOOP;
ELSE out."libLog" := 'Action non reconnue';RETURN next out;
END CASE;
--- DROP TABLE public.ref_meta;
--- Log
PERFORM hub_log (schemaSource, out); 
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_update 
--- Description : Mise à jour de données (fonction utilisée par une autre fonction)
--- Variables :
--- o schemaSource = Nom du schema source
--- o schemaDest = Nom du schema de destination
--- o tableSource  = Nom de la table source
--- o tableDest  = Nom de la table de destination
--- o champRef = nom du champ de référence utilisé pour tester la jointure entre la source et la destination
--- o jdd = jeu de donnée (code du jeu ou 'data' ou 'taxa')
--- o action = type d'action à réaliser - valeur possibles : 'push_total', 'push_diff' et 'diff'(par défaut)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_update(schemaSource varchar,schemaDest varchar, tableSource varchar, tableDest varchar, champRef varchar, jdd varchar, action varchar = 'diff') RETURNS setof zz_log  AS 
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
CASE WHEN jdd = 'data' OR jdd = 'taxa' THEN EXECUTE 'SELECT CASE WHEN string_agg(''''''''||"cdJdd"||'''''''','','') IS NULL THEN ''''''vide'''''' ELSE string_agg(''''''''||"cdJdd"||'''''''','','') END FROM "'||schemaSource||'"."'||metasource||'" WHERE "typJdd" = '''||jdd||''';' INTO listJdd;
ELSE listJdd := ''||jdd||'';END CASE;

CASE WHEN champRef = 'cdJddPerm' THEN typJdd = 'meta';flag := 1;
WHEN champRef = 'cdObsPerm' THEN typJdd = 'data';flag := 1;
WHEN champRef = 'cdEntPerm' THEN typJdd = 'taxa';flag := 1;
ELSE flag := 0;
END CASE;
EXECUTE 'SELECT string_agg(''a."''||cd||''" = b."''||cd||''"'','' AND '') FROM ref.fsd_'||typJdd||' WHERE (tbl_name = '''||tableSource||''' OR tbl_name = '''||tableDest||''') AND unicite = ''Oui''' INTO jointure;
source := '"'||schemaSource||'"."'||tableSource||'"';
destination := '"'||schemaDest||'"."'||tableDest||'"';
--- Output
out."libSchema" := schemaSource; out."libTable" := tableSource; out."libChamp" := '-'; out."typLog" := 'hub_update';SELECT CURRENT_TIMESTAMP INTO out."date";
--- Commande
EXECUTE 'SELECT string_agg(''"''||column_name||''" = b."''||column_name||''"::''||data_type,'','')  FROM information_schema.columns where table_name = '''||tableDest||''' AND table_schema = '''||schemaDest||'''' INTO listeChamp;
EXECUTE 'SELECT string_agg(''a."''||column_name||''"::varchar <> b."''||column_name||''"::varchar'','' OR '')  FROM information_schema.columns where table_name = '''||tableSource||''' AND table_schema = '''||schemaSource||'''' INTO wheres;
EXECUTE 'SELECT count(DISTINCT a."'||champRef||'") FROM '||source||' a JOIN '||destination||' b ON '||jointure||' WHERE a."cdJdd" IN ('||listJdd||') AND ('||wheres||');' INTO compte;

CASE WHEN (compte > 0) AND flag = 1 THEN
	CASE WHEN action = 'push_diff' THEN
		EXECUTE 'SELECT string_agg(''''''''||b."'||champRef||'"||'''''''','','') FROM '||source||' a JOIN '||destination||' b ON '||jointure||' WHERE a."cdJdd" IN ('||listJdd||') AND ('||wheres||');' INTO val;
		EXECUTE 'UPDATE '||destination||' a SET '||listeChamp||' FROM (SELECT * FROM '||source||') b WHERE a."'||champRef||'" IN ('||val||')';
		out."libTable" := tableSource; out."libLog" := 'Concept(s) modifié(s)'; out."nbOccurence" := compte||' occurence(s)';PERFORM hub_log (schemaSource, out); return next out;
	WHEN action = 'diff' THEN
		out."libLog" := 'Concept(s) à modifier'; out."nbOccurence" := compte||' occurence(s)';PERFORM hub_log (schemaSource, out); return next out;
	ELSE out."libLog" := 'ERREUR : sur champ action = '||action; out."nbOccurence" := compte||' occurence(s)';PERFORM hub_log (schemaSource, out); return next out;
	END CASE;
ELSE out."libLog" := 'Aucune différence'; out."nbOccurence" := '-';PERFORM hub_log (schemaSource, out); return next out;
END CASE;	
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_verif 
--- Description : Vérification des données
--- Variables :
--- o libSchema = Nom du schema
--- o jdd = jeu de donnée (code du jeu ou 'data' ou 'taxa')
--- o typVerif = type de vérification - valeur possibles : 'obligation', 'type', 'doublon', 'vocactrl' et 'all'(par défaut)
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
out."libSchema" := libSchema;out."typLog" := 'hub_verif';SELECT CURRENT_TIMESTAMP INTO out."date";
--- Variables
CASE WHEN jdd = 'data' OR jdd = 'taxa' OR jdd = 'meta' THEN
	typJdd := Jdd;
	---
ELSE EXECUTE 'SELECT "typJdd" FROM "'||libSchema||'"."temp_metadonnees" WHERE "cdJdd" = '''||jdd||'''' INTO typJdd;
	---
END CASE;
out."libLog" = '';

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
			out."libTable" := libTable; out."libChamp" := libChamp;out."libLog" := 'Champ obligatoire non renseigné => SELECT * FROM hub_verif_plus('''||libSchema||''','''||libTable||''','''||libChamp||''',''obligation'');'; out."nbOccurence" := compte||' occurence(s)'; return next out;PERFORM hub_log (schemaSource, out);
			EXECUTE 'INSERT INTO "'||libSchema||'".zz_log ("libSchema","libTable","libChamp","typLog","libLog","nbOccurence","date") VALUES ('''||out."libSchema"||''','''||out."libTable"||''','''||out."libChamp"||''','''||out."typLog"||''',''Champ obligatoire non renseigné'','''||out."nbOccurence"||''','''||out."date"||''');';
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
			out."libTable" := libTable; out."libChamp" := libChamp;	out."libLog" := typChamp||' incorrecte => SELECT * FROM hub_verif_plus('''||libSchema||''','''||libTable||''','''||libChamp||''',''type'');'; out."nbOccurence" := compte||' occurence(s)'; return next out;PERFORM hub_log (schemaSource, out);
			EXECUTE 'INSERT INTO "'||libSchema||'".zz_log ("libSchema","libTable","libChamp","typLog","libLog","nbOccurence","date") VALUES ('''||out."libSchema"||''','''||out."libTable"||''','''||out."libChamp"||''','''||out."typLog"||''','''||typChamp||'incorrecte'','''||out."nbOccurence"||''','''||out."date"||''');';
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
			out."libTable" := libTable; out."libChamp" := libChamp; out."libLog" := 'doublon(s) => SELECT * FROM hub_verif_plus('''||libSchema||''','''||libTable||''','''||libChamp||''',''doublon'');'; out."nbOccurence" := compte||' occurence(s)'; return next out;PERFORM hub_log (schemaSource, out);
			EXECUTE 'INSERT INTO "'||libSchema||'".zz_log ("libSchema","libTable","libChamp","typLog","libLog","nbOccurence","date") VALUES ('''||out."libSchema"||''','''||out."libTable"||''','''||out."libChamp"||''','''||out."typLog"||''',''doublon(s)'','''||out."nbOccurence"||''','''||out."date"||''');';
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
				out."libTable" := libTable; out."libChamp" := libChamp; out."libLog" := 'Valeur(s) non listée(s) => SELECT * FROM hub_verif_plus('''||libSchema||''','''||libTable||''','''||libChamp||''',''vocactrl'');'; out."nbOccurence" := compte||' occurence(s)'; return next out;PERFORM hub_log (schemaSource, out);
				EXECUTE 'INSERT INTO "'||libSchema||'".zz_log ("libSchema","libTable","libChamp","typLog","libLog","nbOccurence","date") VALUES ('''||out."libSchema"||''','''||out."libTable"||''','''||out."libChamp"||''','''||out."typLog"||''',''Valeur(s) non listée(s)'','''||out."nbOccurence"||''','''||out."date"||''');';
			ELSE --- rien
			END CASE;
		ELSE --- rien
		END CASE;
		END LOOP;
	END LOOP;
ELSE --- rien
END CASE;

--- Le 100%
CASE WHEN out."libLog" = '' THEN
	out."libTable" := '-'; out."libChamp" := '-'; out."libLog" := jdd||' conformes pour '||typVerif; out."nbOccurence" := '-'; PERFORM hub_log (schemaSource, out); return next out;
	EXECUTE 'INSERT INTO "'||libSchema||'".zz_log ("libSchema","libTable","libChamp","typLog","libLog","nbOccurence","date") VALUES ('''||out."libSchema"||''','''||out."libTable"||''','''||out."libChamp"||''','''||out."typLog"||''','''||out."libLog"||''','''||out."nbOccurence"||''','''||out."date"||''');';
ELSE ---Rien
END CASE;

--- Log général 
END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_verif_plus
--- Description : Vérification des données
--- Variables :
--- o libSchema = Nom du schema
--- o libTable = Nom de la table
--- o libChamp = Nom du champ
--- o typVerif = type de vérification - valeur possibles : 'obligation', 'type', 'doublon', 'vocactrl' et 'all'(par défaut)
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
out."libSchema" := libSchema;out."typLog" := 'hub_verif_plus';SELECT CURRENT_TIMESTAMP INTO out."date";
--- Variables
CASE 	WHEN libTable LIKE 'metadonnees%' 				THEN 	champRef = 'cdJddPerm';typJdd = 'meta';
	WHEN libTable LIKE 'observation%' OR libTable LIKE 'releve%' 	THEN 	champRef = 'cdObsPerm';typJdd = 'data';
	WHEN libTable LIKE 'entite%' 					THEN 	champRef = 'cdEntPerm';typJdd = 'taxa';
	ELSE 									champRef = ''; 	END CASE;

--- Test concernant l'obligation
CASE WHEN (typVerif = 'obligation' OR typVerif = 'all') THEN
FOR champRefSelected IN EXECUTE 'SELECT "'||champRef||'" FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" IS NULL'
	LOOP out."libTable" := libTable; out."libChamp" := libChamp; out."libLog" := champRefSelected; out."nbOccurence" := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  "'||champRef||'" = '''||champRefSelected||''''; return next out; END LOOP;
ELSE --- rien
END CASE;

--- Test concernant le typage des champs
CASE WHEN (typVerif = 'type' OR typVerif = 'all') THEN
	EXECUTE 'SELECT type FROM ref.ddd WHERE cd = '''||libChamp||'''' INTO typChamp;
		IF (typChamp = 'int') THEN --- un entier
			FOR champRefSelected IN EXECUTE 'SELECT "'||champRef||'" FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^\d+$''' 
			LOOP out."libTable" := libTable; out."libChamp" := libChamp; out."libLog" := champRefSelected; out."nbOccurence" := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  "'||champRef||'" = '''||champRefSelected||''''; return next out;END LOOP;
		ELSIF (typChamp = 'float') THEN --- un float
			FOR champRefSelected IN EXECUTE 'SELECT "'||champRef||'" FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^\-?\d+\.\d+$'''
			LOOP out."libTable" := libTable; out."libChamp" := libChamp; out."libLog" := champRefSelected; out."nbOccurence" := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  "'||champRef||'" = '''||champRefSelected||''''; return next out;END LOOP;
		ELSIF (typChamp = 'date') THEN --- une date
			FOR champRefSelected IN EXECUTE 'SELECT "'||champRef||'" FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^[1,2][0-9]{2}[0-9]\-[0,1][0-9]\-[0-3][0-9]$'' AND "'||libChamp||'" !~ ''^[1,2][0-9]{2}[0-9]\-[0,1][0-9]$'' AND "'||libChamp||'" !~ ''^[1,2][0-9]{2}[0-9]$'''
			LOOP out."libTable" := libTable; out."libChamp" := libChamp; out."libLog" := champRefSelected; out."nbOccurence" := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  "'||champRef||'" = '''||champRefSelected||''''; return next out;END LOOP;
		ELSIF (typChamp = 'boolean') THEN --- Boolean
			FOR champRefSelected IN EXECUTE 'SELECT "'||champRef||'" FROM "'||libSchema||'"."temp_'||libTable||'" WHERE "'||libChamp||'" !~ ''^t$'' AND "'||libChamp||'" !~ ''^f$'''
			LOOP out."libTable" := libTable; out."libChamp" := libChamp; out."libLog" := champRefSelected; out."nbOccurence" := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  "'||champRef||'" = '''||champRefSelected||''''; return next out;END LOOP;
		ELSE --- le reste
			EXECUTE 'SELECT 1';
		END IF;
ELSE --- rien
END CASE;

--- Test concernant les doublon
CASE WHEN (typVerif = 'doublon' OR typVerif = 'all') THEN
	FOR champRefSelected IN EXECUTE 'SELECT '||libChamp||' FROM "'||libSchema||'"."temp_'||libTable||'" GROUP BY '||libChamp||' HAVING COUNT('||libChamp||') > 1'
		LOOP EXECUTE 'SELECT "'||champRef||'" FROM "'||libSchema||'"."temp_'||libTable||'" WHERE '||libChamp||' = '''||champRefSelected||''';' INTO champRefSelected;
		out."libTable" := libTable; out."libChamp" := libChamp; out."libLog" := champRefSelected; out."nbOccurence" := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  "'||champRef||'" = '''||champRefSelected||''''; return next out;END LOOP;
		--- EXECUTE 'INSERT INTO "'||libSchema||'".zz_log ("libSchema","libTable","libChamp","typLog","libLog","nbOccurence","date") VALUES ('''||out."libSchema"||''','''||out."libTable"||''','''||out."libChamp"||''','''||out."typLog"||''','''||out."libLog"||''','''||out."nbOccurence"||''','''||out."date"||''');';
ELSE --- rien
END CASE;

--- Test concernant le vocbulaire controlé
CASE WHEN (typVerif = 'vocactrl' OR typVerif = 'all') THEN
	EXECUTE 'SELECT DISTINCT 1 FROM ref.voca_ctrl WHERE "typChamp" = '''||libChamp||''' ;' INTO flag;
		CASE WHEN flag = 1 THEN
		FOR champRefSelected IN EXECUTE 'SELECT "'||champRef||'" FROM "'||libSchema||'"."temp_'||libTable||'" LEFT JOIN ref.voca_ctrl ON "'||libChamp||'" = "cdChamp" WHERE "cdChamp" IS NULL'
		LOOP out."libTable" := libTable; out."libChamp" := libChamp; out."libLog" := champRefSelected; out."nbOccurence" := 'SELECT * FROM "'||libSchema||'"."temp_'||libTable||'" WHERE  "'||champRef||'" = '''||champRefSelected||''''; return next out; END LOOP;
	ELSE ---Rien
	END CASE;
ELSE --- rien
END CASE;

--- Log général
--- EXECUTE 'INSERT INTO "public".zz_log ("libSchema","libTable","libChamp","typLog","libLog","nbOccurence","date") VALUES ('''||out."libSchema"||''',''-'',''-'',''hub_verif'',''-'',''-'','''||out."date"||''');';
RETURN;END;$BODY$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : hub_verif_all
--- Description : Chainage des vérification
--- Variables :
--- o libSchema = Nom du schema
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
--- Variables :
--- o libSchema = Nom du schema
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION hub_log (libSchema varchar, outp zz_log) RETURNS void AS 
$BODY$ 
BEGIN
EXECUTE 'INSERT INTO "'||libSchema||'".zz_log ("libSchema","libTable","libChamp","typLog","libLog","nbOccurence","date") VALUES ('''||outp."libSchema"||''','''||outp."libTable"||''','''||outp."libChamp"||''','''||outp."typLog"||''','''||outp."libLog"||''','''||outp."nbOccurence"||''','''||outp."date"||''');';
EXECUTE 'INSERT INTO "public".zz_log ("libSchema","libTable","libChamp","typLog","libLog","nbOccurence","date") VALUES ('''||outp."libSchema"||''','''||outp."libTable"||''','''||outp."libChamp"||''','''||outp."typLog"||''','''||outp."libLog"||''','''||outp."nbOccurence"||''','''||outp."date"||''');';
END;$BODY$ LANGUAGE plpgsql;