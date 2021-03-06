﻿-------------------------------------------------------------
-------------------------------------------------------------
--- Pas à pas pour la création du SI FLORE
-------------------------------------------------------------
--- 1. Création de la base de données SI FLORE
-- CREATE DATABASE siflore_data OWNER postgres ENCODING 'UTF-8';
-- Récupérer les fonction en lançant les script hub.sql ET hub_to_siflore.sql sur cette base de données.

--- 2. Création d'un hub pour récupérer les données
-- SELECT * FROM hub_connect_ref_simple('all', 'dbname=[nom_bdd_a_copier] port=[port_bdd_a_copier] user=postgres password=[mpd_utilisateur]');
-- SELECT * FROM hub_admin_clone('hub');

--- 3. Mettre en place le reste de la base de données
-- SELECT * FROM siflore_clone(7);

--- 4. Création/mise à jour des référentiels utilisés (table taxref_new )
-- SELECT * FROM siflore_ref_refresh();

--- 4. Ajout/mise à jour des utilisateurs 
--- SELECT * FROM siflore_right();

-------------------------------------------------------------
-------------------------------------------------------------
--- Pas à pas pour la mise à jour des données dans le SI FLORE ---
-------------------------------------------------------------
--- 1. Mise à jour des données (HUB => HUB SIFLORE => SIFLORE /exploitation)
-- SELECT * FROM siflore_data_refresh('alp');
--- 2. Mise à jour des tables "dynamiques" utilisées dans l'application SI FLORE (menus déroulants, onglets de synthèse en bas de page...)
-- SELECT * FROM siflore_synthese_refresh();

-------------------------------------------------------------
-------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.zz_log (lib_schema character varying,lib_table character varying,lib_champ character varying,typ_log character varying,lib_log character varying,nb_occurence character varying,date_log timestamp,user_log character varying);
CREATE TABLE IF NOT EXISTS public.threecol (col1 varchar, col2 varchar, col3 varchar);
CREATE TABLE IF NOT EXISTS public.twocol (col1 varchar, col2 varchar);


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : siflore_admin_init
--- Description : initialise les fonctions hub_to_siflore (supprime toutes les fonctions) et  initialise certaines tables (zz_log et bilan)
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_admin_init() RETURNS void  AS 
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
	WHERE  routine_name LIKE ''siflore_%''
	ORDER BY routine_name,ordinal_position
	) as one
	GROUP BY routine_name, specific_name';
--- Suppression de ces fonctions
FOR fonction IN EXECUTE cmd
   LOOP EXECUTE 'DROP FUNCTION '||fonction||';';
   END LOOP;

-- Fonctions utilisées par le siflore
listFunction = ARRAY['dblink','postgis'];
FOREACH fonction IN ARRAY listFunction LOOP
	EXECUTE 'SELECT extname from pg_extension WHERE extname = '''||fonction||''';' INTO exist;
	CASE WHEN exist IS NULL THEN EXECUTE 'CREATE EXTENSION "'||fonction||'";';
	ELSE END CASE;
END LOOP;
END;$BODY$ LANGUAGE plpgsql;

--- Lancé à chaque fois pour réinitialier les fonctions
SELECT * FROM siflore_admin_init();


-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction : siflore_clone
--- Description : Construit l'architecture du SI FLORE
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_clone(version_taxref integer = 7) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE exist varchar;
DECLARE base_source varchar;
BEGIN
/*variables*/
base_source = 'siflore_data_temp';

--- Schema: observation;
DROP SCHEMA IF EXISTS observation CASCADE; CREATE SCHEMA observation;
-- Table: observation.bd_mere
CREATE TABLE observation.bd_mere(  bd_mere character varying NOT NULL,  libelle_court_bd_mere character varying,  CONSTRAINT bd_mere_pkey PRIMARY KEY (bd_mere));
-- Table: observation.nature_date
CREATE TABLE observation.nature_date(  nature_date character(1) NOT NULL,  libelle_nature_date character varying,  CONSTRAINT nature_date_pkey PRIMARY KEY (nature_date));
EXECUTE 'INSERT INTO observation.nature_date SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM observation.nature_date;'') as t1(  a character(1),z character varying)';
-- Table: observation.nature_objet_geo
CREATE TABLE observation.nature_objet_geo(nature_objet_geo character varying NOT NULL,  libelle_nature_objet_geo character varying,  CONSTRAINT nature_objet_geo_pkey PRIMARY KEY (nature_objet_geo));
EXECUTE 'INSERT INTO observation.nature_objet_geo SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM observation.nature_objet_geo;'') as t1(  a character,z character varying)';
-- Table: observation.statut_pop
CREATE TABLE observation.statut_pop(statut_pop character varying NOT NULL,  libelle_statut_pop character varying,  CONSTRAINT statut_pop_pkey PRIMARY KEY (statut_pop));
EXECUTE 'INSERT INTO observation.statut_pop SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM observation.statut_pop;'') as t1(a character varying,z character varying)';
-- Table: observation.syst_ref_spatial
CREATE TABLE observation.syst_ref_spatial(syst_ref_spatial character varying NOT NULL,  libelle_ref_spatial character varying,  CONSTRAINT syst_ref_spatial_pkey PRIMARY KEY (syst_ref_spatial));
EXECUTE 'INSERT INTO observation.syst_ref_spatial SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM observation.syst_ref_spatial;'') as t1(a character varying,z character varying)';
-- Table: observation.type_localisation
CREATE TABLE observation.type_localisation(type_localisation character NOT NULL,  libelle_type_localisation character varying,  CONSTRAINT type_localisation_pkey PRIMARY KEY (type_localisation));
EXECUTE 'INSERT INTO observation.type_localisation SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM observation.type_localisation;'') as t1(a character varying,z character varying)';
-- Table: observation.type_rattachement
CREATE TABLE observation.type_rattachement(type_rattachement character NOT NULL,  libelle_type_rattachement character varying,  CONSTRAINT type_rattachement_pkey PRIMARY KEY (type_rattachement));
EXECUTE 'INSERT INTO observation.type_rattachement SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM observation.type_rattachement;'') as t1(a character varying,z character varying)';
-- Table: observation.type_source
CREATE TABLE observation.type_source(  type_source character NOT NULL,  libelle_type_source character varying,  CONSTRAINT type_source_pkey PRIMARY KEY (type_source));
EXECUTE 'INSERT INTO observation.type_source SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM observation.type_source;'') as t1(a character varying,z character varying)';
-- Table: observation.usage_donnee
CREATE TABLE observation.usage_donnee(  usage_donnee integer NOT NULL,  libelle_usage_donnee character varying,  CONSTRAINT usage_donnee_pkey PRIMARY KEY (usage_donnee));
EXECUTE 'INSERT INTO observation.usage_donnee SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM observation.usage_donnee;'') as t1(a integer,z character varying)';


--- Schema: exploitation;
DROP SCHEMA IF EXISTS exploitation CASCADE; CREATE SCHEMA exploitation;
--- Table: exploitation.commentaires;
CREATE TABLE exploitation.commentaires (id serial NOT NULL,  comment text NOT NULL,  nom character varying NOT NULL,  prenom character varying,  utilisateur character varying,  email character varying,  cd_ref integer,  id_obj character varying,  date_com date,  type_com character varying,  priorite_com character varying,  action_com character varying,  id_flore_fcbn character varying,  nom_complet character varying,  comment2 text[],  CONSTRAINT commentaire_id_pkey PRIMARY KEY (id));
--- Table: exploitation.obs_commune;
CREATE TABLE exploitation.obs_commune(id_flore_fcbn character varying NOT NULL,  cd_ref integer,  nom_complet character varying,  code_taxon_mere varchar,  referentiel_mere character varying,  nom_taxon_mere character varying,  nom_taxon_originel character varying,  remarque_taxon character varying,  libelle_statut_pop character varying,  libelle_court_bd_mere character varying,  libelle_usage_donnee character varying,  libelle_court_bd_source character varying,  id_flore_source character varying,  remarque_donnee_mere character varying,  libelle_nature_date character varying,  remarque_date character varying,  remarque_lieu character varying,  libelle_type_source character varying,  type_doc character varying,  cote_biblio_cbn varchar,  titre_doc character varying,  annee_doc integer,  auteur_doc character varying,  ref_doc character varying,  code_herbarium character varying,  code_index_herbariorum character varying,  nom_herbarium character varying,  code_herbier character varying,  nom_herbier character varying,  part_herbier character varying,  id_part character varying,  cote_biblio_bd_mere character varying,  date_debut_obs date,  date_fin_obs date,  date_transmission date,  id_flore_mere character varying,  libelle_type_localisation character varying,  libelle_type_rattachement character varying,  insee_comm character varying NOT NULL,  nom_comm character varying,  geom geometry,  statut_pop character varying,  nom_observateur character varying,  prenom_observateur character varying,  libelle_organisme character varying,  observateur character varying,  cd_jdd character varying, CONSTRAINT obs_commune_pkey PRIMARY KEY (id_flore_fcbn, insee_comm));
DROP INDEX IF EXISTS exploitation.obs_commune_cd_ref_idk;CREATE INDEX obs_commune_cd_ref_idk  ON exploitation.obs_commune  USING btree  (cd_ref);
DROP INDEX IF EXISTS exploitation.obs_commune_geom_gist;CREATE INDEX obs_commune_geom_gist  ON exploitation.obs_commune  USING gist  (geom);
DROP INDEX IF EXISTS exploitation.obs_commune_insee_comm_idk;CREATE INDEX obs_commune_insee_comm_idk  ON exploitation.obs_commune  USING btree  (insee_comm COLLATE pg_catalog."default");
DROP INDEX IF EXISTS exploitation.obs_commune_nom_comm_idk;CREATE INDEX obs_commune_nom_comm_idk  ON exploitation.obs_commune  USING btree  (nom_comm COLLATE pg_catalog."default");
DROP INDEX IF EXISTS exploitation.obs_commune_cd_jdd_idk;CREATE INDEX obs_commune_cd_jdd_idk  ON exploitation.obs_commune  USING btree  (cd_jdd);
--- Table: exploitation.obs_maille_fr10
CREATE TABLE exploitation.obs_maille_fr10(id_flore_fcbn character varying NOT NULL,  cd_ref integer,  nom_complet character varying,  code_taxon_mere varchar,  referentiel_mere character varying,  nom_taxon_mere character varying,  nom_taxon_originel character varying,  remarque_taxon character varying,  libelle_statut_pop character varying,  libelle_court_bd_mere character varying,  libelle_usage_donnee character varying,  libelle_court_bd_source character varying,  id_flore_source character varying,  remarque_donnee_mere character varying,  libelle_nature_date character varying,  remarque_date character varying,  remarque_lieu character varying,  libelle_type_source character varying,  type_doc character varying,  cote_biblio_cbn varchar,  titre_doc character varying,  annee_doc integer,  auteur_doc character varying,  ref_doc character varying,  code_herbarium character varying,  code_index_herbariorum character varying,  nom_herbarium character varying,  code_herbier character varying,  nom_herbier character varying,  part_herbier character varying,  id_part character varying,  cote_biblio_bd_mere character varying,  date_debut_obs date,  date_fin_obs date,  date_transmission date,  id_flore_mere character varying,  cd_sig character varying NOT NULL,  libelle_type_localisation character varying,  libelle_type_rattachement character varying,  geom geometry(MultiPolygon,2154),  statut_pop character varying,  nom_observateur character varying,  prenom_observateur character varying,  libelle_organisme character varying,  observateur character varying, cd_jdd character varying, CONSTRAINT obs_maille_fr10_pkey PRIMARY KEY (id_flore_fcbn, cd_sig));
DROP INDEX IF EXISTS exploitation.obs_maille_fr10_cd_ref_idk; CREATE INDEX obs_maille_fr10_cd_ref_idk  ON exploitation.obs_maille_fr10  USING btree  (cd_ref);
DROP INDEX IF EXISTS exploitation.obs_maille_fr10_cd_sig_idk; CREATE INDEX obs_maille_fr10_cd_sig_idk  ON exploitation.obs_maille_fr10  USING btree  (cd_sig COLLATE pg_catalog."default");
DROP INDEX IF EXISTS exploitation.obs_maille_fr10_geom_gist; CREATE INDEX obs_maille_fr10_geom_gist ON exploitation.obs_maille_fr10  USING gist  (geom);
DROP INDEX IF EXISTS exploitation.obs_maille_fr10_cd_jdd_idk;CREATE INDEX obs_maille_fr10_cd_jdd_idk  ON exploitation.obs_maille_fr10  USING btree  (cd_jdd);
-- Table: exploitation.obs_maille_fr5
CREATE TABLE exploitation.obs_maille_fr5 (id_flore_fcbn character varying NOT NULL,  cd_ref integer,  nom_complet character varying,  code_taxon_mere varchar,  referentiel_mere character varying,  nom_taxon_mere character varying,  nom_taxon_originel character varying,  remarque_taxon character varying,  libelle_statut_pop character varying,  libelle_court_bd_mere character varying,  libelle_usage_donnee character varying,  libelle_court_bd_source character varying,  id_flore_source character varying,  remarque_donnee_mere character varying,  libelle_nature_date character varying,  remarque_date character varying,  remarque_lieu character varying,  libelle_type_source character varying,  type_doc character varying,  cote_biblio_cbn varchar,  titre_doc character varying,  annee_doc integer,  auteur_doc character varying,  ref_doc character varying,  code_herbarium character varying,  code_index_herbariorum character varying,  nom_herbarium character varying,  code_herbier character varying,  nom_herbier character varying,  part_herbier character varying,  id_part character varying,  cote_biblio_bd_mere character varying,  date_debut_obs date,  date_fin_obs date,  date_transmission date,  id_flore_mere character varying,  cd_sig character varying NOT NULL,  libelle_type_localisation character varying(20),  libelle_type_rattachement character varying,  geom geometry(MultiPolygon,2154),  statut_pop character varying,  nom_observateur character varying,  prenom_observateur character varying,  libelle_organisme character varying,  observateur character varying, cd_jdd character varying, CONSTRAINT obs_maille_fr5_pkey PRIMARY KEY (id_flore_fcbn, cd_sig));
DROP INDEX IF EXISTS exploitation.obs_maille_fr5_cd_ref_idk;CREATE INDEX obs_maille_fr5_cd_ref_idk  ON exploitation.obs_maille_fr5  USING btree  (cd_ref);
DROP INDEX IF EXISTS exploitation.obs_maille_fr5_cd_sig_idk;CREATE INDEX obs_maille_fr5_cd_sig_idk  ON exploitation.obs_maille_fr5  USING btree  (cd_sig COLLATE pg_catalog."default");
DROP INDEX IF EXISTS exploitation.obs_maille_fr5_geom_gist;CREATE INDEX obs_maille_fr5_geom_gist  ON exploitation.obs_maille_fr5  USING gist  (geom);
DROP INDEX IF EXISTS exploitation.obs_maille_fr5_cd_jdd_idk;CREATE INDEX obs_maille_fr5_cd_jdd_idk  ON exploitation.obs_maille_fr5  USING btree  (cd_jdd);
-- Table: exploitation.taxref
CREATE TABLE exploitation.taxref_new (cd_ref integer NOT NULL,  nom_complet character varying NOT NULL,  regne character varying,  phylum character varying,  classe character varying,  ordre character varying, famille character varying,  cd_taxsup integer,  rang character varying,  lb_nom character varying,  lb_auteur character varying,  nom_vern character varying,  nom_vern_eng character varying, habitat character varying,  liste_bryo boolean DEFAULT false,  bryophyta boolean DEFAULT false,  cd_taxsup2 integer,  cd_taxsup3 integer,  cd_taxsup4 integer,  CONSTRAINT taxref_new_pkey PRIMARY KEY (cd_ref, nom_complet));
DROP INDEX IF EXISTS cd_ref_idk;CREATE INDEX cd_ref_idk  ON exploitation.taxref_new  USING btree (cd_ref);
DROP INDEX IF EXISTS cd_taxsup2_idk;CREATE INDEX cd_taxsup2_idk ON exploitation.taxref_new USING btree (cd_taxsup2);
DROP INDEX IF EXISTS cd_taxsup3_idk;CREATE INDEX cd_taxsup3_idk ON exploitation.taxref_new USING btree (cd_taxsup3);
DROP INDEX IF EXISTS cd_taxsup4_idk;CREATE INDEX cd_taxsup4_idk ON exploitation.taxref_new USING btree (cd_taxsup4);
DROP INDEX IF EXISTS cd_taxsup_idk ;CREATE INDEX cd_taxsup_idk  ON exploitation.taxref_new USING btree (cd_taxsup);
-- Table: exploitation.taxons
CREATE TABLE exploitation.taxons(cd_ref integer NOT NULL, nom_complet text NOT NULL,  rang character varying,  type character varying,  CONSTRAINT taxons_new_pkey PRIMARY KEY (cd_ref, nom_complet));
-- Table: exploitation.taxons_communs
CREATE TABLE exploitation.taxons_communs(  cd_ref integer NOT NULL,  nom_complet character varying,  taxons_communs_ss_inf character varying,  taxons_communs_av_inf character varying,  CONSTRAINT pk_taxons_communs PRIMARY KEY (cd_ref));
-- Table : les synthèses
CREATE TABLE exploitation.synthese_taxon_comm(cd_ref integer,  nom_complet character varying,  nb_obs bigint,  nb_obs_1500_1980 bigint,  nb_obs_1981_2000 bigint,  nb_obs_2001_2013 bigint,  nb_obs_averee bigint,  nb_obs_interpretee bigint,  date_premiere_obs date,  date_derniere_obs date);
CREATE TABLE exploitation.synthese_taxon_fr10(cd_ref integer,  nom_complet character varying,  nb_obs bigint,  nb_obs_1500_1980 bigint,  nb_obs_1981_2000 bigint,  nb_obs_2001_2013 bigint,  nb_obs_averee bigint,  nb_obs_interpretee bigint,  date_premiere_obs date,  date_derniere_obs date);
CREATE TABLE exploitation.synthese_taxon_fr5  (cd_ref integer,  nom_complet character varying,  nb_obs bigint,  nb_obs_1500_1980 bigint,  nb_obs_1981_2000 bigint,  nb_obs_2001_2013 bigint,  nb_obs_averee bigint,  nb_obs_interpretee bigint,  date_premiere_obs date,  date_derniere_obs date);
--CREATE TABLE exploitation.synthese_maille_fr10( cd_sig character varying,  nb_taxons integer,  nb_obs integer,  nb_obs_1500_1980 integer,  nb_obs_1981_2000 integer,  nb_obs_2001_2013 integer,  nb_obs_averee integer,  nb_obs_interpretee integer,  date_premiere_obs date,  date_derniere_obs date,  geom geometry(MultiPolygon,2154),  gid integer,  CONSTRAINT synthese_maille_fr10_pkey PRIMARY KEY (cd_sig));
CREATE TABLE exploitation.information_taxa_taxons(cd_ref text,  famille text,  nom_sci text,  cd_rang text,  national text,  alsace text,  aquitaine text,  auvergne text,  basse_normandie text,  bourgogne text,  bretagne text,  centre text,  champagne_ardenne text,  corse text,  franche_comte text,  haute_normandie text,  ile_de_france text,  languedoc_roussillon text,  limousin text,  lorraine text,  midi_pyrenees text,  nord_pas_de_calais text,  pays_de_la_loire text,  picardie text,  poitou_charentes text,  paca text,  rhone_alpes text);
CREATE TABLE exploitation.information_taxons(cd_ref integer,  nom_complet character varying,  url text,  num_nom_tela character varying,  num_nom_retenu_tela character varying,  nom_sci character varying,  cd_nom integer);
--- Table : exploitation.lien_bdtfx_taxref
CREATE TABLE exploitation.lien_bdtfx_taxref(num_nom character varying, num_nom_retenu character varying, nom_sci character varying, cd_nom integer, CONSTRAINT num_nom_pkey PRIMARY KEY (num_nom));
EXECUTE 'INSERT INTO exploitation.lien_bdtfx_taxref SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM exploitation.lien_bdtfx_taxref;'') as t1(a character varying,z character varying,e character varying,t integer)';
--- Table : exploitation.stt_lr_reg_catnat
CREATE TABLE exploitation.stt_lr_reg_catnat(uid integer NOT NULL,cd_ref integer,famille text,nom_sci text,cd_rang text,id_reg integer NOT NULL,statuts text,CONSTRAINT stt_lr_reg_catnat_pkey PRIMARY KEY (uid, id_reg));
--- Table : exploitation.stt_lr_nat_catnat
CREATE TABLE exploitation.stt_lr_nat_catnat(uid integer NOT NULL,statuts_nat text,CONSTRAINT stt_lr_nat_catnat_pkey PRIMARY KEY (uid));
--- Table : exploitation.suivi_maj_data
CREATE TABLE exploitation.suivi_maj_data(cd_jdd varchar NOT NULL, date_maj timestamp,CONSTRAINT suivi_maj_data_pkey PRIMARY KEY (cd_jdd));
-- Table: exploitation.convention_berne
CREATE TABLE exploitation.convention_berne(cd_ref integer, nom_complet character varying, regne character varying, phylum character varying, classe character varying, ordre character varying, famille character varying, cd_taxsup integer, rang character varying, lb_nom character varying, lb_auteur character varying, nom_vern character varying, nom_vern_eng character varying, habitat character varying, cd_taxsup2 integer, cd_taxsup3 integer, cd_taxsup4 integer,CONSTRAINT convention_berne_pkey PRIMARY KEY (cd_ref));
EXECUTE 'INSERT INTO exploitation.convention_berne SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM exploitation.convention_berne;'') as t1(cd_ref integer, nom_complet character varying, regne character varying, phylum character varying, classe character varying, ordre character varying, famille character varying, cd_taxsup integer, rang character varying, lb_nom character varying, lb_auteur character varying, nom_vern character varying, nom_vern_eng character varying, habitat character varying, cd_taxsup2 integer, cd_taxsup3 integer, cd_taxsup4 integer);';
--- Table:exploitation.taxref_no_change
PERFORM siflore_taxref_no_change(version_taxref);

--- Schema: observation_reunion;
DROP SCHEMA IF EXISTS observation_reunion CASCADE; CREATE SCHEMA observation_reunion;
-- Table: observation_reunion.communes_bdtopo_reunion
CREATE TABLE observation_reunion.communes_bdtopo_reunion(  gid serial NOT NULL,  id character varying,  prec_plani double precision,  nom character varying,  code_insee character varying NOT NULL,  statut character varying,  canton character varying,  arrondisst character varying,  depart character varying,  region character varying,  popul integer,  multican character varying,  geom geometry(MultiPolygon,2975),  geom_3857_s100 geometry(MultiPolygon,3857),  geom_3857 geometry(MultiPolygon,3857),  CONSTRAINT communes_bd_topo_reunion_pkey PRIMARY KEY (code_insee));
EXECUTE 'INSERT INTO observation_reunion.communes_bdtopo_reunion SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM observation_reunion.communes_bdtopo_reunion;'') as t1(gid integer,  id character varying(24),  prec_plani double precision,  nom character varying,  code_insee character varying,  statut character varying,  canton character varying(45),  arrondisst character varying,  depart character varying,  region character varying,  popul integer,  multican character varying,  geom geometry(MultiPolygon,2975),  geom_3857_s100 geometry(MultiPolygon,3857),  geom_3857 geometry(MultiPolygon,3857));';
DROP INDEX IF EXISTS communes_bdtopo_reunion_geom_gist; CREATE INDEX communes_bdtopo_reunion_geom_gist  ON observation_reunion.communes_bdtopo_reunion  USING gist  (geom);
-- Table: observation_reunion.grille_10km_zee_974
CREATE TABLE observation_reunion.grille_10km_zee_974(  gid serial NOT NULL,  cd_sig character varying NOT NULL,  code_10km character varying,  geom geometry(MultiPolygon,2975),  CONSTRAINT grille_10km_zee_974_pkey PRIMARY KEY (cd_sig));
EXECUTE 'INSERT INTO observation_reunion.grille_10km_zee_974 SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM observation_reunion.grille_10km_zee_974;'') as t1 (  gid integer,  cd_sig character varying(21),  code_10km character varying,  geom geometry(MultiPolygon,2975));';
DROP INDEX IF EXISTS grille_10km_zee_974_geom_gist;CREATE INDEX grille_10km_zee_974_geom_gist  ON observation_reunion.grille_10km_zee_974  USING gist  (geom);
-- Table: observation_reunion.maille_utm1
CREATE TABLE observation_reunion.maille_utm1(gid serial,  nom_maille character varying NOT NULL,  centroid_x character varying,  centroid_y character varying,  geom geometry(MultiPolygon,2975),  geom_3857_s100 geometry(MultiPolygon,3857),  geom_3857 geometry(MultiPolygon,3857),  CONSTRAINT maille_utm1_pkey PRIMARY KEY (nom_maille));
EXECUTE 'INSERT INTO observation_reunion.maille_utm1 SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM observation_reunion.maille_utm1;'') as t1 (gid integer,  nom_maille character varying,  centroid_x character varying,  centroid_y character varying,  geom geometry(MultiPolygon,2975),  geom_3857_s100 geometry(MultiPolygon,3857),  geom_3857 geometry(MultiPolygon,3857));';
DROP INDEX IF EXISTS maille_1x1_utm_geom_gist;CREATE INDEX maille_1x1_utm_geom_gist  ON observation_reunion.maille_utm1  USING gist  (geom);
-- Table: observation_reunion.index_reunion
CREATE TABLE observation_reunion.index_reunion( code_taxon integer NOT NULL,  nom_taxon character varying NOT NULL,  cd_ref integer,  nom_complet character varying,  CONSTRAINT index_reunion_pkey PRIMARY KEY (code_taxon, nom_taxon));
EXECUTE 'INSERT INTO observation_reunion.index_reunion SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM observation_reunion.index_reunion'') as t1 (code_taxon integer,  nom_taxon character varying,  cd_ref integer,  nom_complet character varying);';
DROP INDEX IF EXISTS idk_code_taxon;CREATE INDEX idk_code_taxon  ON observation_reunion.index_reunion  USING btree  (code_taxon);
-- Table: observation_reunion.observation_taxon_reunion
CREATE TABLE observation_reunion.observation_taxon_reunion(id_flore_fcbn character varying NOT NULL,  code_taxon integer NOT NULL,  nom_taxon character varying,  code_taxon_mere varchar,  referentiel_mere character varying,  nom_taxon_mere character varying,  nom_taxon_originel character varying,  remarque_taxon character varying,  statut_pop character varying NOT NULL,  bd_mere character varying NOT NULL,  usage_donnee integer NOT NULL,  bd_source character varying,  libelle_court_bd_source character varying,  id_flore_source character varying,  sup_donnee boolean,  remarque_donnee_mere character varying,  nature_date character(2) NOT NULL,  remarque_date character varying,  syst_ref_spatial character varying,  nature_objet_geo character varying,  remarque_lieu character varying,  type_source character varying NOT NULL,  type_doc character varying,  cote_biblio_cbn varchar,  titre_doc character varying,  annee_doc integer,  auteur_doc character varying,  ref_doc character varying,  code_herbarium character varying,  code_index_herbariorum character varying,  nom_herbarium character varying,  code_herbier character varying,  nom_herbier character varying,  part_herbier character varying,  id_part character varying,  cote_biblio_bd_mere character varying,  date_debut_obs date NOT NULL,  date_fin_obs date NOT NULL,  id_objet_geo integer,  date_transmission date NOT NULL,  id_flore_mere character varying, cd_jdd character varying, CONSTRAINT observation_taxon_pkey PRIMARY KEY (id_flore_fcbn),  CONSTRAINT observation_taxon_bd_mere_fkey FOREIGN KEY (bd_mere)      REFERENCES observation.bd_mere (bd_mere) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_taxon_code_taxon_fkey FOREIGN KEY (code_taxon, nom_taxon)      REFERENCES observation_reunion.index_reunion (code_taxon, nom_taxon) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_taxon_nature_date_fkey FOREIGN KEY (nature_date)      REFERENCES observation.nature_date (nature_date) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_taxon_nature_objet_geo_fkey FOREIGN KEY (nature_objet_geo)      REFERENCES observation.nature_objet_geo (nature_objet_geo) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_taxon_statut_pop_fkey FOREIGN KEY (statut_pop)      REFERENCES observation.statut_pop (statut_pop) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_taxon_syst_ref_spatial_fkey FOREIGN KEY (syst_ref_spatial)      REFERENCES observation.syst_ref_spatial (syst_ref_spatial) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_taxon_type_source_fkey FOREIGN KEY (type_source)      REFERENCES observation.type_source (type_source) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_taxon_usage_donnee_fkey FOREIGN KEY (usage_donnee)      REFERENCES observation.usage_donnee (usage_donnee) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION);
DROP INDEX IF EXISTS ifk_code_taxon_obs; CREATE INDEX ifk_code_taxon_obs  ON observation_reunion.observation_taxon_reunion  USING btree  (code_taxon, nom_taxon COLLATE pg_catalog."default");
-- Table: observation_reunion.observation_maille_utm1
CREATE TABLE observation_reunion.observation_maille_utm1(  id_flore_fcbn character varying NOT NULL,  nom_maille character varying NOT NULL,  type_localisation_maille_utm1 character(1) NOT NULL,  type_rattachement_maille_utml character(1) NOT NULL,  remarque_lieu character varying,  id serial NOT NULL,  CONSTRAINT observation_maille_utm1_pkey PRIMARY KEY (id_flore_fcbn, nom_maille),  CONSTRAINT observation_maille_utm1_id_flore_fcbn_fkey FOREIGN KEY (id_flore_fcbn)      REFERENCES observation_reunion.observation_taxon_reunion (id_flore_fcbn) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_maille_utm1_nom_maille_fkey FOREIGN KEY (nom_maille)      REFERENCES observation_reunion.maille_utm1 (nom_maille) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_maille_utm1_type_localisation_maille_utm1_fkey FOREIGN KEY (type_localisation_maille_utm1)      REFERENCES observation.type_localisation (type_localisation) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_maille_utm1_type_rattachement_maille_utm1_fkey FOREIGN KEY (type_rattachement_maille_utml)      REFERENCES observation.type_rattachement (type_rattachement) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION);
-- Table: observation_reunion.observation_maille_utm10
CREATE TABLE observation_reunion.observation_maille_utm10(  id_flore_fcbn character varying NOT NULL,  cd_sig character varying NOT NULL,  type_localisation_maille_utm10 character(1) NOT NULL, type_rattachement_maille_utml0 character(1) NOT NULL,  remarque_lieu character varying,  id serial NOT NULL,  CONSTRAINT observation_maille_utm10_pkey PRIMARY KEY (id_flore_fcbn, cd_sig),  CONSTRAINT observation_maille_utm10_cd_sig_fkey FOREIGN KEY (cd_sig)      REFERENCES observation_reunion.grille_10km_zee_974 (cd_sig) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_maille_utm10_id_flore_fcbn_fkey FOREIGN KEY (id_flore_fcbn)      REFERENCES observation_reunion.observation_taxon_reunion (id_flore_fcbn) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_maille_utm10_type_localisation_maille_utm10_fkey FOREIGN KEY (type_localisation_maille_utm10)      REFERENCES observation.type_localisation (type_localisation) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_maille_utm10_type_rattachement_maille_utm10_fkey FOREIGN KEY (type_rattachement_maille_utml0)      REFERENCES observation.type_rattachement (type_rattachement) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION);
-- Table: observation_reunion.observation_commune_reunion
CREATE TABLE observation_reunion.observation_commune_reunion(  id_flore_fcbn character varying NOT NULL,  code_insee character varying NOT NULL,  type_localisation_commune character(1) NOT NULL,  type_rattachement_commune character(1) NOT NULL,  remarque_lieu character varying,  referentiel_communal character varying NOT NULL,  departement character(3),  id serial NOT NULL,  CONSTRAINT observation_commune_reunion_pkey PRIMARY KEY (id_flore_fcbn, code_insee),  CONSTRAINT observation_commune_reunion_code_insee_fkey FOREIGN KEY (code_insee)      REFERENCES observation_reunion.communes_bdtopo_reunion (code_insee) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_commune_reunion_id_flore_fcbn_fkey FOREIGN KEY (id_flore_fcbn)      REFERENCES observation_reunion.observation_taxon_reunion (id_flore_fcbn) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_commune_reunion_type_localisation_commune_fkey FOREIGN KEY (type_localisation_commune)      REFERENCES observation.type_localisation (type_localisation) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_commune_reunion_type_rattachement_commune_fkey FOREIGN KEY (type_rattachement_commune)      REFERENCES observation.type_rattachement (type_rattachement) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION);


--- Log
out.lib_log := 'Siflore créé';out.lib_schema := '-';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_clone';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log ('public', out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;


-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction : siflore_right
--- Description : Gestion des droits d'accès pour le SI FLORE
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_right(opt varchar = 'add', this_user varchar = null) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE ListUser varchar[];
DECLARE oneUser varchar;
DECLARE db_nam varchar;
BEGIN
--- variables
CASE WHEN this_user IS null THEN
	ListUser = ARRAY['cbn_c_interface','cbn_med_interface','cbn_bl_interface','cbn_bp_interface','cbn_alp_interface','cbn_mc_interface','cbn_fc_interface','cbn_mas_interface','cbn_b_interface','a_just_interface','j_gourvil_interface','m_decherf_interface','j_millet_interface','r_gaspard_interface','i_mandon_interface','cbn_sa_interface','cbn_pmp_interface','plateforme_siflore','lecteur_masao'];
ELSE ListUser = ARRAY [this_user];
END CASE;
SELECT catalog_name INTO db_nam FROM information_schema.information_schema_catalog_name;
/*Gestion des droits*/
CASE WHEN opt = 'add' THEN
FOREACH oneUser IN ARRAY ListUser LOOP
	EXECUTE 'GRANT CONNECT ON DATABASE '||db_nam||' TO '||oneUser||';';
	EXECUTE 'GRANT USAGE ON SCHEMA exploitation TO '||oneUser||';';
	EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA exploitation TO '||oneUser||';';
	EXECUTE 'GRANT USAGE ON SCHEMA observation TO '||oneUser||';';
	EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA observation TO '||oneUser||';';
	EXECUTE 'GRANT USAGE ON SCHEMA observation_reunion TO '||oneUser||';';
	EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA observation_reunion TO '||oneUser||';';
	out.lib_log := 'user supprimé';
END LOOP;
WHEN opt = 'drop' THEN
	EXECUTE 'REVOKE ALL PRIVILEGES ON DATABASE '||db_nam||' TO '||oneUser||';';
	out.lib_log := 'user supprimé';
ELSE out.lib_log := 'Paramètre incorrecte';
END CASE;
--- Log
out.lib_schema := '-';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_right';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log ('hub', out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;

-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction : siflore_taxref_no_change
--- Description : génère une table taxref avec les taxons valables pour la version de taxref choisi (= pas de modification notable entre la version défini et les autres versions).
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_taxref_no_change(version integer = 7) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE listVersion varchar;
BEGIN
/*Fonction*/
DROP TABLE IF EXISTS exploitation.taxref_no_change;
CREATE TABLE exploitation.taxref_no_change(version integer,cd_ref integer,nom_valide varchar,CONSTRAINT taxref_no_change_pk PRIMARY KEY (version,cd_ref));

FOR listVersion IN 2..9 LOOP
	EXECUTE 'INSERT INTO exploitation.taxref_no_change
	SELECT '''||listVersion||''' as version, a.cd_ref::integer, a.nom_valide 
	FROM ref.taxref_v'||listVersion||'  a
	JOIN ref.taxref_v'||version||' z ON 
	a.regne = z.regne AND a.classe = z.classe AND a.ordre = z.ordre AND a.famille = z.famille AND a.cd_nom = z.cd_nom AND a.cd_ref = z.cd_ref AND a.rang = z.rang 
	AND a.lb_nom = z.lb_nom AND a.lb_auteur = z.lb_auteur AND a.nom_complet = z.nom_complet AND a.nom_valide = z.nom_valide
	GROUP BY a.cd_ref,a.nom_valide
	;';
END LOOP;
--- Log
out.lib_log := 'exploitation.taxref_no_change créé pour taxref_v'||version;
out.lib_schema := '-';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_taxref_no_change';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log ('hub', out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;


-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction : siflore_ref_refresh
--- Description : Met à jour taxref en fonction de la version souhaité - aujourd'hui ajout de taxref_v7_new
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_ref_refresh(typ varchar = 'all', version integer = 7) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE flag integer;
BEGIN
--- Variable
flag = 0;

--- Commande
/*Mise à jour de la table taxref*/
CASE WHEN (typ = 'taxref' OR typ = 'all') THEN
EXECUTE '
TRUNCATE exploitation.taxref_new;
INSERT INTO exploitation.taxref_new(cd_ref, nom_complet, regne, phylum, classe, ordre, famille, cd_taxsup, rang, lb_nom, lb_auteur, nom_vern, nom_vern_eng, habitat)
SELECT cd_ref::integer, nom_complet, regne, phylum, classe, ordre, famille, cd_taxsup::integer, rang, lb_nom, lb_auteur, nom_vern, nom_vern_eng, habitat
	FROM ref.taxref_v'||version||'
	WHERE cd_nom = cd_ref
	GROUP BY cd_ref, nom_complet, regne, phylum, classe, ordre, famille, cd_taxsup, rang, lb_nom, lb_auteur, nom_vern, nom_vern_eng, habitat;
';
UPDATE exploitation.taxref_new i SET cd_taxsup2 = out.cd_taxsup FROM (SELECT cd_ref, cd_taxsup FROM exploitation.taxref_new) as out WHERE out.cd_ref = i.cd_taxsup;  --- taxsup2
UPDATE exploitation.taxref_new i SET cd_taxsup3 = out.cd_taxsup FROM (SELECT cd_ref, cd_taxsup FROM exploitation.taxref_new) as out WHERE out.cd_ref = i.cd_taxsup2; --- taxsup3
UPDATE exploitation.taxref_new i SET cd_taxsup4 = out.cd_taxsup FROM (SELECT cd_ref, cd_taxsup FROM exploitation.taxref_new) as out WHERE out.cd_ref = i.cd_taxsup3; --- taxsup4
UPDATE exploitation.taxref_new SET liste_bryo = TRUE WHERE cd_ref in (SELECT cd_ref FROM exploitation.convention_berne); /*Bryophyte de la convention de berne*/
EXECUTE 'UPDATE exploitation.taxref_new SET bryophyta = TRUE WHERE cd_ref in (SELECT cd_ref::integer FROM ref.taxref_v'||version||' WHERE group1_inpn = ''Bryophytes'' GROUP BY cd_ref);'; /*Les bryophytes*/
flag = 1;
ELSE END CASE;


--- Log
CASE WHEN flag = 0 THEN out.lib_log := 'Pas de mise à jours'; ELSE out.lib_log := 'référentiel mis à jour : '||typ; END CASE;
out.lib_schema := '-';out.lib_table := 'exploitation.taxref_new';out.lib_champ := '-';out.typ_log := 'siflore_taxref_refresh';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log; out.user_log := current_user;PERFORM hub_log ('hub', out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;

-------------------------------------------------------------
-------------------------------------------------------------
--- Fonction siflore_synthese_refresh
--- Description : Mise à jour des synthèses du SI FLORE
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_synthese_refresh(typ varchar = 'all') RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE cmd varchar;
DECLARE dbname varchar;
DECLARE flag integer;
BEGIN 
--- variable
flag = 0;
dbname = 'outil_evaluation';
--- commande
/*Mise à jour de la liste déroulante des taxons*/
CASE WHEN (typ = 'listx' OR typ = 'all') THEN
--- version Thomas
TRUNCATE exploitation.taxons; 
INSERT INTO exploitation.taxons 
SELECT a.cd_ref::integer, z.nom_complet, z.rang, 
CASE 	WHEN liste_bryo IS TRUE AND bryophyta IS TRUE THEN 'bryo_liste' 
	WHEN liste_bryo IS FALSE AND bryophyta IS TRUE THEN 'bryo_pas_liste' 
	WHEN liste_bryo IS FALSE AND bryophyta IS FALSE THEN 'tracheo' 
	ELSE 'problème' END as type
FROM exploitation.obs_maille_fr10 a
JOIN exploitation.taxref_new z ON a.cd_ref::integer = z.cd_ref
GROUP BY a.cd_ref, z.nom_complet, z.rang, liste_bryo, bryophyta;

-- taxons réunion
INSERT INTO exploitation.taxons
SELECT a.cd_ref::integer, a.nom_complet, z.rang, 
CASE 	WHEN liste_bryo IS TRUE AND bryophyta IS TRUE THEN 'bryo_liste' 
	WHEN liste_bryo IS FALSE AND bryophyta IS TRUE THEN 'bryo_pas_liste' 
	WHEN liste_bryo IS FALSE AND bryophyta IS FALSE THEN 'tracheo' 
	ELSE 'problème' END as type
FROM observation_reunion.index_reunion a
JOIN exploitation.taxref_new z ON a.cd_ref::integer = z.cd_ref
LEFT JOIN exploitation.taxons e ON a.cd_ref::integer = e.cd_ref
WHERE e.cd_ref IS NULL AND a.cd_ref IS NOT NULL
GROUP BY a.cd_ref, a.nom_complet, z.rang, liste_bryo, bryophyta;
flag = 1;
ELSE END CASE;

--- Version Anaïs
/*
TRUNCATE exploitation.taxons;
INSERT INTO exploitation.taxons
SELECT all_tax.cd_ref, all_tax.nom_complet, all_tax.rang, 
CASE 	WHEN (all_tax.bryophyta=true and all_tax.liste_bryo=true) THEN 'bryo_liste' 
	WHEN (all_tax.bryophyta=true and all_tax.liste_bryo=false) THEN 'bryo_pas_liste' 
	WHEN (all_tax.bryophyta=false) THEN 'tracheo' 
	ELSE 'non connu' END as "type"
FROM (
WITH RECURSIVE hierarchie(cd_ref, nom_complet, cd_taxsup, rang) AS (
SELECT cd_ref, nom_complet, cd_taxsup, rang, liste_bryo, bryophyta   FROM exploitation.taxref_v7_new WHERE cd_ref::text  in (select distinct obs.cd_ref from hub.observation obs)
  UNION
SELECT e.cd_ref, e.nom_complet, e.cd_taxsup, e.rang, e.liste_bryo,e.bryophyta   FROM hierarchie AS h, exploitation.taxref_v7_new AS e  WHERE h.cd_taxsup = e.cd_ref
) SELECT cd_ref, nom_complet, rang, liste_bryo, bryophyta FROM hierarchie order by nom_complet) all_tax order by nom_complet;
*/

/*Mise à jour des tables de synthese utilisées par les onglets "Synthèse par taxon" en bas de l'application*/
-- Remplir la table synthese_taxon_comm contenant la synthese pour les taxons liées aux communes
CASE WHEN typ = 'com' OR typ = 'all' THEN
TRUNCATE exploitation.synthese_taxon_comm;
INSERT INTO exploitation.synthese_taxon_comm
SELECT obs.cd_ref, obs.nom_complet, count(*) AS nb_obs, 
	count(CASE WHEN obs.date_fin_obs >= '1500-01-01'::date AND obs.date_fin_obs < '1980-01-01'::date THEN 1 ELSE NULL::integer END) AS nb_obs_1500_1980, 
	count(CASE WHEN obs.date_fin_obs >= '1980-01-01'::date AND obs.date_fin_obs < '2000-01-01'::date THEN 1 ELSE NULL::integer END) AS nb_obs_1981_2000, 
	count(CASE WHEN obs.date_fin_obs >= '2000-01-01'::date THEN 1 ELSE NULL::integer END) AS nb_obs_2001_2013, 
	count(CASE WHEN obs.libelle_type_localisation = 'Averée' THEN 1 ELSE NULL::integer END) AS nb_obs_averee, 
	count(CASE WHEN obs.libelle_type_localisation = 'Interpretée' THEN 1 ELSE NULL::integer END) AS nb_obs_interpretee, 
	min(obs.date_debut_obs) AS date_premiere_obs, max(obs.date_fin_obs) AS date_derniere_obs
FROM exploitation.obs_commune obs
GROUP BY obs.cd_ref, obs.nom_complet;
flag = 1;
ELSE END CASE;

--Remplir la table synthese_taxon_fr10 contenant la synthese pour les taxons liées aux mailles 10
CASE WHEN typ = 'm10' OR typ = 'all' THEN
TRUNCATE exploitation.synthese_taxon_fr10;
INSERT INTO exploitation.synthese_taxon_fr10
SELECT obs.cd_ref, obs.nom_complet, count(*) AS nb_obs, 
count(CASE WHEN obs.date_fin_obs >= '1500-01-01'::date AND obs.date_fin_obs < '1980-01-01'::date THEN 1 ELSE NULL::integer END) AS nb_obs_1500_1980, 
count(CASE WHEN obs.date_fin_obs >= '1980-01-01'::date AND obs.date_fin_obs < '2000-01-01'::date THEN 1 ELSE NULL::integer END) AS nb_obs_1981_2000, 
count(CASE WHEN obs.date_fin_obs >= '2000-01-01'::date THEN 1 ELSE NULL::integer END) AS nb_obs_2001_2013, 
count(CASE WHEN obs.libelle_type_localisation = 'Averée' THEN 1 ELSE NULL::integer END) AS nb_obs_averee, 
count(CASE WHEN obs.libelle_type_localisation = 'Interpretée' THEN 1 ELSE NULL::integer END) AS nb_obs_interpretee, 
min(obs.date_debut_obs) AS date_premiere_obs, max(obs.date_fin_obs) AS date_derniere_obs
FROM exploitation.obs_maille_fr10 obs
GROUP BY obs.cd_ref, obs.nom_complet;
flag = 1;
ELSE END CASE;

--Remplir la table synthese_taxon_fr5 contenant la synthese pour les taxons liées aux mailles 5
CASE WHEN typ = 'm5' OR typ = 'all' THEN
TRUNCATE exploitation.synthese_taxon_fr5;
INSERT INTO exploitation.synthese_taxon_fr5
SELECT obs.cd_ref, obs.nom_complet, count(*) AS nb_obs, 
count(CASE WHEN obs.date_fin_obs >= '1500-01-01'::date AND obs.date_fin_obs < '1980-01-01'::date THEN 1 ELSE NULL::integer END) AS nb_obs_1500_1980, 
count(CASE WHEN obs.date_fin_obs >= '1980-01-01'::date AND obs.date_fin_obs < '2000-01-01'::date THEN 1 ELSE NULL::integer END) AS nb_obs_1981_2000, 
count(CASE WHEN obs.date_fin_obs >= '2000-01-01'::date THEN 1 ELSE NULL::integer END) AS nb_obs_2001_2013, 
count(CASE WHEN obs.libelle_type_localisation = 'Averée' THEN 1 ELSE NULL::integer END) AS nb_obs_averee, 
count(CASE WHEN obs.libelle_type_localisation = 'Interpretée' THEN 1 ELSE NULL::integer END) AS nb_obs_interpretee, 
min(obs.date_debut_obs) AS date_premiere_obs, max(obs.date_fin_obs) AS date_derniere_obs
FROM exploitation.obs_maille_fr5 obs
GROUP BY obs.cd_ref, obs.nom_complet;
flag = 1;
ELSE END CASE;
    
/*Mise à jour de la table qui alimente l'onglet "liens externes" (lien avec telabotanica et INPN) */
CASE WHEN typ = 'tela' OR typ = 'all' THEN
TRUNCATE exploitation.information_taxons;
INSERT INTO exploitation.information_taxons SELECT t.cd_ref, t.nom_complet,'<a href="http://www.tela-botanica.org/bdtfx-nn-'||l.num_nom|| '-synthese" target="_blank"> Lien Tela Botanica </a> <br /> <a href="http://inpn.mnhn.fr/espece/cd_nom/'||t.cd_ref||'" target="_blank"> Lien INPN </a> ' as url, l.num_nom as num_nom_tela, l.num_nom_retenu as num_nom_retenu_tela, l.nom_sci, l.cd_nom
FROM exploitation.taxref_new t inner join exploitation.lien_bdtfx_taxref l on (t.cd_ref=l.cd_nom);
flag = 1;
ELSE END CASE;


/*Mise à jour de la table qui alimente l'onglet "Statuts listes rouges" (à partir de l'outil d'évaluation Codex/rubrique catanat) */
-- peuplement: stt_lr_reg_catnat
CASE WHEN typ = 'lr_reg' OR typ = 'all' THEN
TRUNCATE exploitation.stt_lr_reg_catnat;
INSERT INTO exploitation.stt_lr_reg_catnat 
SELECT * FROM dblink('dbname=outil_evaluation','SELECT reg.uid, cd_ref, famille, nom_sci, cd_rang, reg.id_reg, CASE WHEN stt2 IS NULL THEN ''-'' ELSE stt2 END as statuts 
	FROM (SELECT uid, cd_ref, famille, nom_sci, cd_rang, id_reg FROM referentiels.regions CROSS JOIN catnat.taxons_nat) as reg
	LEFT JOIN  (SELECT sr.uid, sr.id_reg, CASE WHEN lr IS NULL THEN ''-'' ELSE lr END as stt2
		FROM catnat.taxons_nat tn
		LEFT JOIN catnat.statut_reg sr ON tn.uid = sr.uid
		LEFT JOIN (SELECT uid, id_reg, id_statut as lr FROM catnat.statut_reg WHERE type_statut = ''LR'') 
		as two on two.uid = sr.uid AND two.id_reg = sr.id_reg
	GROUP BY sr.uid, sr.id_reg, stt2) as stt ON reg.id_reg = stt.id_reg AND reg.uid = stt.uid;') as t1 (uid integer,cd_ref integer,famille text,nom_sci text,cd_rang text,id_reg integer,statuts text);
flag = 1;
ELSE END CASE;

-- peuplement: stt_lr_nat_catnat
CASE WHEN typ = 'lr_nat' OR typ = 'all' THEN
TRUNCATE exploitation.stt_lr_nat_catnat;
INSERT INTO exploitation.stt_lr_nat_catnat 
SELECT * FROM dblink('dbname=outil_evaluation','SELECT sub.uid, stt2 as statuts_nat FROM 
	(SELECT sn.uid, CASE WHEN lr = ''à évaluer'' THEN ''-'' ELSE lr END as stt2 FROM catnat.statut_nat sn) as sub
	GROUP BY sub.uid, statuts_nat
	ORDER BY sub.uid;') as t1 (uid integer,statuts text);
flag = 1;
ELSE END CASE;

-- peuplement: info taxa
CASE WHEN typ = 'taxa' OR typ = 'all' THEN
TRUNCATE exploitation.information_taxa_taxons;
INSERT INTO exploitation.information_taxa_taxons
SELECT stt_lr_reg_catnat.cd_ref, stt_lr_reg_catnat.famille, stt_lr_reg_catnat.nom_sci, stt_lr_reg_catnat.cd_rang, stt_lr_nat_catnat.statuts_nat as "National",
"Alsace" , "Aquitaine", "Auvergne", "Basse-Normandie", "Bourgogne", "Bretagne", "Centre", "Champagne-Ardenne", "Corse", "Franche-Comté", "Haute-Normandie", "Île-de-France", "Languedoc-Roussillon", 
 "Limousin", "Lorraine",  "Midi-Pyrénées", "Nord-Pas-de-Calais", "Pays de la Loire", "Picardie", "Poitou-Charentes", "Provence-Alpes-Côte d'Azur", "Rhône-Alpes"
FROM exploitation.stt_lr_reg_catnat
LEFT JOIN exploitation.stt_lr_nat_catnat ON stt_lr_nat_catnat.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Alsace" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 42) as a on a.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Aquitaine" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 72) as b on b.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Auvergne" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 83) as c on c.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Basse-Normandie" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 25) as d on d.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Bourgogne" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 26) as e on e.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Bretagne" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 53) as f on f.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Centre" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 24) as g on g.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Champagne-Ardenne" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 21) as h on h.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Corse" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 94) as i on i.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Franche-Comté" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 43) as j on j.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Haute-Normandie" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 23) as m on m.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Île-de-France" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 11) as n on n.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Languedoc-Roussillon" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 91) as o on o.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Limousin" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 74) as q on q.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Lorraine" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 41) as r on r.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Midi-Pyrénées" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 73) as u on u.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Nord-Pas-de-Calais" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 31) as v on v.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Pays de la Loire" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 52) as w on w.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Picardie" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 22) as x on x.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Poitou-Charentes" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 54) as y on y.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Provence-Alpes-Côte d'Azur" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 93) as z on z.uid=stt_lr_reg_catnat.uid
LEFT JOIN (SELECT uid, statuts as "Rhône-Alpes" FROM exploitation.stt_lr_reg_catnat WHERE id_reg = 82) as aa on aa.uid=stt_lr_reg_catnat.uid
GROUP BY stt_lr_reg_catnat.cd_ref, stt_lr_reg_catnat.famille, stt_lr_reg_catnat.nom_sci, stt_lr_reg_catnat.cd_rang, "National",
"Alsace" , "Aquitaine", "Auvergne", "Basse-Normandie", "Bourgogne", "Bretagne", "Centre", "Champagne-Ardenne", "Corse", "Franche-Comté", "Haute-Normandie", "Île-de-France", "Languedoc-Roussillon", 
 "Limousin", "Lorraine", "Midi-Pyrénées", "Nord-Pas-de-Calais", "Pays de la Loire", "Picardie", "Poitou-Charentes", "Provence-Alpes-Côte d'Azur", "Rhône-Alpes"
ORDER BY nom_sci;
flag = 1;
ELSE END CASE;

--- Log
CASE WHEN flag = 0 THEN out.lib_log := 'Pas de mise à jour'; ELSE out.lib_log := 'synthèses mis à jour : '||typ; END CASE;
out.lib_schema := 'hub';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_synthese';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;PERFORM hub_log ('hub', out); RETURN next out;
END; $BODY$ LANGUAGE plpgsql;


-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction siflore_insert
--- Description : Met à jour toutes les données du SI FLORE depuis la copie du hub dans le SIFLORE
--- Sauf partie REUNION
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_insert(version integer = 7) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE jdd threecol%rowtype;
DECLARE cmd varchar;
DECLARE ct integer;
DECLARE list_jdd varchar;
DECLARE test_jdd integer;
BEGIN 
--- log
out.lib_schema := 'hub';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_insert';SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;

--- Les communes
--teste la présence de relevés à l'échelle de la commune
SELECT count(*) INTO ct FROM hub.observation as obs 
	JOIN hub.releve rel ON rel.cd_jdd = obs.cd_jdd AND rel.cd_releve = obs.cd_releve
	JOIN hub.releve_territoire ter ON rel.cd_jdd = ter.cd_jdd AND rel.cd_releve = ter.cd_releve
	JOIN hub.metadonnees meta ON meta.cd_jdd = obs.cd_jdd
	JOIN ref.geo_commune com ON ter.cd_geo = com.insee_comm
	JOIN ref.voca_ctrl stp ON stp.cd_champ = 'statut_pop' AND stp.code_valeur = statut_pop::varchar
	JOIN ref.voca_ctrl prp ON prp.cd_champ = 'propriete_obs' AND prp.code_valeur = propriete_obs
	JOIN ref.voca_ctrl ntd ON ntd.cd_champ = 'nature_date' AND ntd.code_valeur = nature_date
	JOIN ref.voca_ctrl tso ON tso.cd_champ = 'typ_source' AND tso.code_valeur = typ_source
	JOIN ref.voca_ctrl cfg ON cfg.cd_champ = 'confiance_geo' AND cfg.code_valeur = confiance_geo
	JOIN ref.voca_ctrl mog ON mog.cd_champ = 'moyen_geo' AND mog.code_valeur = moyen_geo
	JOIN exploitation.taxref_no_change tnc ON obs.cd_ref::integer =  tnc.cd_ref AND obs.version_reftaxo::integer =  tnc.version
	WHERE typ_geo = 'com'
	AND date_debut IS NOT NULL AND date_fin IS NOT NULL
	AND cd_validite = 1;
-- intégration des relevés à l'échelle de la commune
CASE WHEN ct <> 0 THEN
	INSERT INTO exploitation.obs_commune
	SELECT	obs.cd_jdd||'_'||cd_obs_mere as id_flore_fcbn,	obs.cd_ref::integer as cd_ref, nom_ent_ref as nom_complet,cd_ent_mere as code_taxon_mere,null as referentiel_mere,nom_ent_mere as nom_taxon_mere,nom_ent_orig as nom_taxon_originel, 
	obs.rmq as remarque_taxon,stp.libelle_valeur as libelle_statut_pop, lib_jdd as libelle_court_bd_mere, prp.libelle_valeur as libelle_usage_donnee,lib_jdd_orig as libelle_court_bd_source, cd_obs_orig as id_flore_source, rel.rmq as remarque_donnee_mere,ntd.libelle_valeur as libelle_nature_date,rel.rmq as remarque_date, ter.rmq as remarque_lieu,tso.libelle_valeur as libelle_type_source,	null as type_doc,cd_biblio as cote_biblio_cbn,lib_biblio as titre_doc,null as annee_doc,	null as auteur_doc,null as ref_doc,null as code_herbarium,null as code_index_herbariorum,null as nom_herbarium,	cd_herbier as code_herbier,
	lib_herbier as nom_herbier,null as part_herbier,null as id_part,null as cote_biblio_bd_mere,date_debut::date as date_debut_obs,	date_fin::date as date_fin_obs,	null as date_transmission,cd_ent_mere as id_flore_mere,	cfg.libelle_valeur, mog.libelle_valeur,cd_geo, lib_geo, com.geom,statut_pop,null as nom_observateur,	null as prenom_observateur,'à mettre à jour' as libelle_organisme,'à mettre à jour' as observateur,meta.cd_jdd as cd_jdd
	FROM hub.observation as obs
	JOIN hub.releve rel ON rel.cd_jdd = obs.cd_jdd AND rel.cd_releve = obs.cd_releve
	JOIN hub.releve_territoire ter ON rel.cd_jdd = ter.cd_jdd AND rel.cd_releve = ter.cd_releve
	JOIN hub.metadonnees meta ON meta.cd_jdd = obs.cd_jdd
	JOIN ref.geo_commune com ON ter.cd_geo = com.insee_comm
	JOIN ref.voca_ctrl stp ON stp.cd_champ = 'statut_pop' AND stp.code_valeur = statut_pop::varchar
	JOIN ref.voca_ctrl prp ON prp.cd_champ = 'propriete_obs' AND prp.code_valeur = propriete_obs
	JOIN ref.voca_ctrl ntd ON ntd.cd_champ = 'nature_date' AND ntd.code_valeur = nature_date
	JOIN ref.voca_ctrl tso ON tso.cd_champ = 'typ_source' AND tso.code_valeur = typ_source
	JOIN ref.voca_ctrl cfg ON cfg.cd_champ = 'confiance_geo' AND cfg.code_valeur = confiance_geo
	JOIN ref.voca_ctrl mog ON mog.cd_champ = 'moyen_geo' AND mog.code_valeur = moyen_geo
	JOIN exploitation.taxref_no_change tnc ON obs.cd_ref::integer =  tnc.cd_ref AND obs.version_reftaxo::integer =  tnc.version
	WHERE typ_geo = 'com'
	AND date_debut IS NOT NULL AND date_fin IS NOT NULL
	AND cd_validite = 1;
--- les acteurs des communes
	UPDATE exploitation.obs_commune input SET libelle_organisme = lib_orgm, observateur = nom_acteur
	FROM (SELECT obs.cd_jdd||'_'||obs.cd_obs_mere as id_flore_fcbn, string_agg(nom_acteur,', ') as nom_acteur, string_agg(lib_orgm,', ') as lib_orgm 
	FROM hub.releve_acteur rel JOIN hub.observation as obs ON rel.cd_jdd = obs.cd_jdd AND rel.cd_releve = obs.cd_releve 
	GROUP BY obs.cd_jdd||'_'||obs.cd_obs_mere) as result
	WHERE result.id_flore_fcbn = input.id_flore_fcbn;
	out.lib_log := 'Communes transférées';out.nb_occurence := ct||' occurence(s)';PERFORM hub_log ('hub', out); RETURN next out;
ELSE 	out.lib_log := 'Aucune commune transférée';out.nb_occurence := '0';PERFORM hub_log ('hub', out); RETURN next out;
END CASE;

--- Les maille10
--teste la présence de relevés à l'échelle de la maille
SELECT count(*) INTO ct FROM hub.observation as obs
	JOIN hub.releve rel ON rel.cd_jdd = obs.cd_jdd AND rel.cd_releve = obs.cd_releve
	JOIN hub.releve_territoire ter ON rel.cd_jdd = ter.cd_jdd AND rel.cd_releve = ter.cd_releve
	JOIN hub.metadonnees meta ON meta.cd_jdd = obs.cd_jdd
	JOIN ref.geo_maille10 m10 ON ter.cd_geo = m10.cd_sig
	JOIN ref.voca_ctrl stp ON stp.cd_champ = 'statut_pop' AND stp.code_valeur = statut_pop::varchar
	JOIN ref.voca_ctrl prp ON prp.cd_champ = 'propriete_obs' AND prp.code_valeur = propriete_obs
	JOIN ref.voca_ctrl ntd ON ntd.cd_champ = 'nature_date' AND ntd.code_valeur = nature_date
	JOIN ref.voca_ctrl tso ON tso.cd_champ = 'typ_source' AND tso.code_valeur = typ_source
	JOIN ref.voca_ctrl cfg ON cfg.cd_champ = 'confiance_geo' AND cfg.code_valeur = confiance_geo
	JOIN ref.voca_ctrl mog ON mog.cd_champ = 'moyen_geo' AND mog.code_valeur = moyen_geo
	JOIN exploitation.taxref_no_change tnc ON obs.cd_ref::integer =  tnc.cd_ref AND obs.version_reftaxo::integer =  tnc.version
	WHERE typ_geo = 'm10'
	AND date_debut IS NOT NULL AND date_fin IS NOT NULL
	AND cd_validite = 1;
-- intégration des relevés à l'échelle de la maille
CASE WHEN ct <> 0 THEN
INSERT INTO exploitation.obs_maille_fr10
	SELECT	obs.cd_jdd||'_'||cd_obs_mere as id_flore_fcbn,obs.cd_ref::integer as cd_ref,nom_ent_ref as nom_complet,cd_ent_mere as code_taxon_mere,null as referentiel_mere,nom_ent_mere as nom_taxon_mere,nom_ent_orig as nom_taxon_originel, 
	obs.rmq as remarque_taxon,stp.libelle_valeur as libelle_statut_pop,lib_jdd as libelle_court_bd_mere, prp.libelle_valeur as libelle_usage_donnee,lib_jdd_orig as libelle_court_bd_source, cd_obs_orig as id_flore_source, rel.rmq as remarque_donnee_mere,ntd.libelle_valeur as libelle_nature_date,rel.rmq as remarque_date, ter.rmq as remarque_lieu,
	tso.libelle_valeur as libelle_type_source,null as type_doc,cd_biblio as cote_biblio_cbn,lib_biblio as titre_doc,null as annee_doc,null as auteur_doc,null as ref_doc,null as code_herbarium,	null as code_index_herbariorum,	null as nom_herbarium,cd_herbier as code_herbier,lib_herbier as nom_herbier,null as part_herbier,null as id_part,
	null as cote_biblio_bd_mere,date_debut::date as date_debut_obs,date_fin::date as date_fin_obs,	null as date_transmission,	cd_ent_mere as id_flore_mere,cd_geo, cfg.libelle_valeur, mog.libelle_valeur,m10.geom, statut_pop,null as nom_observateur,null as prenom_observateur,'à mettre à jour' as libelle_organisme,'à mettre à jour' as observateur,meta.cd_jdd as cd_jdd
	FROM hub.observation as obs
	JOIN hub.releve rel ON rel.cd_jdd = obs.cd_jdd AND rel.cd_releve = obs.cd_releve
	JOIN hub.releve_territoire ter ON rel.cd_jdd = ter.cd_jdd AND rel.cd_releve = ter.cd_releve
	JOIN hub.metadonnees meta ON meta.cd_jdd = obs.cd_jdd
	JOIN ref.geo_maille10 m10 ON ter.cd_geo = m10.cd_sig
	JOIN ref.voca_ctrl stp ON stp.cd_champ = 'statut_pop' AND stp.code_valeur = statut_pop::varchar
	JOIN ref.voca_ctrl prp ON prp.cd_champ = 'propriete_obs' AND prp.code_valeur = propriete_obs
	JOIN ref.voca_ctrl ntd ON ntd.cd_champ = 'nature_date' AND ntd.code_valeur = nature_date
	JOIN ref.voca_ctrl tso ON tso.cd_champ = 'typ_source' AND tso.code_valeur = typ_source
	JOIN ref.voca_ctrl cfg ON cfg.cd_champ = 'confiance_geo' AND cfg.code_valeur = confiance_geo
	JOIN ref.voca_ctrl mog ON mog.cd_champ = 'moyen_geo' AND mog.code_valeur = moyen_geo
	JOIN exploitation.taxref_no_change tnc ON obs.cd_ref::integer =  tnc.cd_ref AND obs.version_reftaxo::integer =  tnc.version
	WHERE typ_geo = 'm10'
	AND date_debut IS NOT NULL AND date_fin IS NOT NULL
	AND cd_validite = 1;
--- les acteurs des mailles 10
	UPDATE exploitation.obs_maille_fr10 input SET libelle_organisme = lib_orgm, observateur = nom_acteur
	FROM (SELECT obs.cd_jdd||'_'||obs.cd_obs_mere as id_flore_fcbn, string_agg(nom_acteur,', ') as nom_acteur, string_agg(lib_orgm,', ') as lib_orgm FROM hub.releve_acteur rel JOIN hub.observation as obs ON rel.cd_jdd = obs.cd_jdd AND rel.cd_releve = obs.cd_releve 
	GROUP BY obs.cd_jdd||'_'||obs.cd_obs_mere) as result
	WHERE result.id_flore_fcbn = input.id_flore_fcbn;
	out.lib_log := 'Maille10 transférées';out.nb_occurence := ct||' occurence(s)';RETURN next out;PERFORM hub_log ('hub', out);
ELSE out.lib_log := 'aucune Maille10 transférée';out.nb_occurence := '0'; RETURN next out;PERFORM hub_log ('hub', out);
END CASE;

--- Les maille5
--teste la présence de relevés à l'échelle de la maille
SELECT count(*) INTO ct FROM hub.observation as obs
	JOIN hub.releve rel ON rel.cd_jdd = obs.cd_jdd AND rel.cd_releve = obs.cd_releve
	JOIN hub.releve_territoire ter ON rel.cd_jdd = ter.cd_jdd AND rel.cd_releve = ter.cd_releve
	JOIN hub.metadonnees meta ON meta.cd_jdd = obs.cd_jdd
	JOIN ref.geo_maille5 m5 ON ter.cd_geo = m5.cd_sig
	JOIN ref.voca_ctrl stp ON stp.cd_champ = 'statut_pop' AND stp.code_valeur = statut_pop::varchar
	JOIN ref.voca_ctrl prp ON prp.cd_champ = 'propriete_obs' AND prp.code_valeur = propriete_obs
	JOIN ref.voca_ctrl ntd ON ntd.cd_champ = 'nature_date' AND ntd.code_valeur = nature_date
	JOIN ref.voca_ctrl tso ON tso.cd_champ = 'typ_source' AND tso.code_valeur = typ_source
	JOIN ref.voca_ctrl cfg ON cfg.cd_champ = 'confiance_geo' AND cfg.code_valeur = confiance_geo
	JOIN ref.voca_ctrl mog ON mog.cd_champ = 'moyen_geo' AND mog.code_valeur = moyen_geo
	JOIN exploitation.taxref_no_change tnc ON obs.cd_ref::integer =  tnc.cd_ref AND obs.version_reftaxo::integer =  tnc.version
	WHERE typ_geo = 'm5'
	AND date_debut IS NOT NULL AND date_fin IS NOT NULL
	AND cd_validite = 1;
-- intégration des relevés à l'échelle de la maille	
CASE WHEN ct <> 0 THEN
INSERT INTO exploitation.obs_maille_fr5
	SELECT	obs.cd_jdd||'_'||cd_obs_mere as id_flore_fcbn,obs.cd_ref::integer as cd_ref,nom_ent_ref as nom_complet, cd_ent_mere as code_taxon_mere,null as referentiel_mere,nom_ent_mere as nom_taxon_mere,nom_ent_orig as nom_taxon_originel, 	obs.rmq as remarque_taxon,stp.libelle_valeur as libelle_statut_pop, lib_jdd as libelle_court_bd_mere, prp.libelle_valeur as libelle_usage_donnee,	lib_jdd_orig as libelle_court_bd_source, cd_obs_orig as id_flore_source, rel.rmq as remarque_donnee_mere,
	ntd.libelle_valeur as libelle_nature_date,rel.rmq as remarque_date, ter.rmq as remarque_lieu,tso.libelle_valeur as libelle_type_source,	null as type_doc,cd_biblio as cote_biblio_cbn,	lib_biblio as titre_doc,null as annee_doc,null as auteur_doc,null as ref_doc,	null as code_herbarium,	null as code_index_herbariorum,	null as nom_herbarium,cd_herbier as code_herbier,lib_herbier as nom_herbier,
	null as part_herbier,null as id_part,null as cote_biblio_bd_mere,date_debut::date as date_debut_obs,date_fin::date as date_fin_obs,null as date_transmission,cd_ent_mere as id_flore_mere,cd_geo, cfg.libelle_valeur, mog.libelle_valeur,m5.geom,statut_pop,null as nom_observateur,null as prenom_observateur,'à mettre à jour' as libelle_organisme,'à mettre à jour' as observateur	,meta.cd_jdd as cd_jdd
	FROM hub.observation as obs
	JOIN hub.releve rel ON rel.cd_jdd = obs.cd_jdd AND rel.cd_releve = obs.cd_releve
	JOIN hub.releve_territoire ter ON rel.cd_jdd = ter.cd_jdd AND rel.cd_releve = ter.cd_releve
	JOIN hub.metadonnees meta ON meta.cd_jdd = obs.cd_jdd
	JOIN ref.geo_maille5 m5 ON ter.cd_geo = m5.cd_sig
	JOIN ref.voca_ctrl stp ON stp.cd_champ = 'statut_pop' AND stp.code_valeur = statut_pop::varchar
	JOIN ref.voca_ctrl prp ON prp.cd_champ = 'propriete_obs' AND prp.code_valeur = propriete_obs
	JOIN ref.voca_ctrl ntd ON ntd.cd_champ = 'nature_date' AND ntd.code_valeur = nature_date
	JOIN ref.voca_ctrl tso ON tso.cd_champ = 'typ_source' AND tso.code_valeur = typ_source
	JOIN ref.voca_ctrl cfg ON cfg.cd_champ = 'confiance_geo' AND cfg.code_valeur = confiance_geo
	JOIN ref.voca_ctrl mog ON mog.cd_champ = 'moyen_geo' AND mog.code_valeur = moyen_geo
	JOIN exploitation.taxref_no_change tnc ON obs.cd_ref::integer =  tnc.cd_ref AND obs.version_reftaxo::integer =  tnc.version
	WHERE typ_geo = 'm5'
	AND date_debut IS NOT NULL AND date_fin IS NOT NULL
	AND cd_validite = 1;
--- les acteurs des mailles 5
	UPDATE exploitation.obs_maille_fr5 input SET libelle_organisme = lib_orgm, observateur = nom_acteur
	FROM (SELECT obs.cd_jdd||'_'||obs.cd_obs_mere as id_flore_fcbn, string_agg(nom_acteur,', ') as nom_acteur, string_agg(lib_orgm,', ') as lib_orgm FROM hub.releve_acteur rel JOIN hub.observation as obs ON rel.cd_jdd = obs.cd_jdd AND rel.cd_releve = obs.cd_releve 
	GROUP BY obs.cd_jdd||'_'||obs.cd_obs_mere) as result
	WHERE result.id_flore_fcbn = input.id_flore_fcbn;
	out.lib_log := 'Maille5 transférées';		out.nb_occurence := ct||' occurence(s)'; 	RETURN next out;PERFORM hub_log ('hub', out);
ELSE 	out.lib_log := 'Aucune Maille5 transférée';	out.nb_occurence := '0'; 			RETURN next out;PERFORM hub_log ('hub', out);
END CASE;

--- Suivi des mises à jour
PERFORM siflore_data_log();

END; $BODY$ LANGUAGE plpgsql;


-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction siflore_insert_reunion
--- Description : Met à jour toutes les données du SI FLORE depuis la copie du hub dans le SIFLORE
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_insert_reunion(version integer = 7) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE jdd threecol%rowtype;
DECLARE cmd varchar;
DECLARE ct integer;
DECLARE list_jdd varchar;
DECLARE test_jdd integer;
BEGIN 
--- log
out.lib_schema := 'hub';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_insert_reunion';SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;

--- Le jeu de données
SELECT count(*)  INTO ct FROM hub.metadonnees meta;
CASE WHEN ct <> 0 THEN
INSERT INTO observation.bd_mere
	SELECT cd_jdd as "bd_mere", lib_jdd as "libelle_court_bd_mere"
	FROM hub.metadonnees;
ELSE out.lib_log := 'Aucun jdd dans la table metadonnees';out.nb_occurence := '0';PERFORM hub_log ('hub', out); RETURN next out;
END CASE;

--- Mise à jour index de la reunion : ajout des taxons manquant
INSERT INTO observation_reunion.index_reunion (code_taxon, nom_taxon, cd_ref, nom_complet)
SELECT cd_ent_mere::integer, nom_ent_mere, a.cd_ref::integer, nom_ent_ref FROM hub.observation a
LEFT JOIN observation_reunion.index_reunion z ON cd_ent_mere::integer = code_taxon
WHERE code_taxon IS NULL
GROUP BY cd_ent_mere, nom_ent_mere, a.cd_ref, nom_ent_ref;
--- Mise à jour index de la reunion : cd_ref manquant
UPDATE observation_reunion.index_reunion SET cd_ref = h.cd_ref, nom_complet = h.nom_ent_ref FROM (
SELECT cd_ent_mere::integer, nom_ent_mere,a.cd_ref::integer, nom_ent_ref FROM hub.observation a
LEFT JOIN observation_reunion.index_reunion z ON a.cd_ref::integer = z.cd_ref
WHERE z.cd_ref IS NULL
GROUP BY cd_ent_mere::integer, nom_ent_mere,a.cd_ref, nom_ent_ref
ORDER BY cd_ent_mere
) as h
WHERE code_taxon = h.cd_ent_mere AND index_reunion.cd_ref IS NULL;
--- Mise à jour index de la reunion : correspondance nom_ent_mere
UPDATE observation_reunion.index_reunion SET nom_taxon = h.nom_ent_mere FROM (
SELECT cd_ent_mere::integer, nom_ent_mere FROM hub.observation a
JOIN observation_reunion.index_reunion z ON a.cd_ent_mere::integer = z.code_taxon
GROUP BY cd_ent_mere::integer, nom_ent_mere
ORDER BY cd_ent_mere
) as h
WHERE code_taxon = h.cd_ent_mere;

--- les observations 	
SELECT count(*) INTO ct FROM hub.observation as obs
	JOIN hub.releve rel ON rel.cd_jdd = obs.cd_jdd AND rel.cd_releve = obs.cd_releve
	JOIN hub.metadonnees meta ON meta.cd_jdd = obs.cd_jdd
	JOIN ref.voca_ctrl prp ON prp.cd_champ = 'propriete_obs' AND prp.code_valeur = propriete_obs
	WHERE date_debut IS NOT NULL AND date_fin IS NOT NULL;
-- intégration
CASE WHEN ct <> 0 THEN
INSERT INTO observation_reunion.observation_taxon_reunion
	SELECT	obs.cd_jdd||'_'||cd_obs_mere as id_flore_fcbn,cd_ent_mere::integer as code_taxon,nom_ent_mere as nom_taxon,cd_ent_mere::integer as code_taxon_mere,null as referentiel_mere,nom_ent_mere as nom_taxon_mere,nom_ent_orig as nom_taxon_originel,obs.rmq as remarque_taxon,CASE statut_pop WHEN 2 THEN 'Q' WHEN 3 THEN 'Q' WHEN 5 THEN 'W' WHEN 6 THEN 'W' WHEN 0 THEN '0' ELSE 'DD' END as statut_pop,obs.cd_jdd as bd_mere,2 as usage_donnee,cd_jdd_orig as bd_source,lib_jdd_orig as libelle_court_bd_source,
	cd_obs_orig as id_flore_source,null as sup_donnee,null as remarque_donnee_mere,nature_date as nature_date,rel.rmq as remarque_date,null as syst_ref_spatial,null as nature_objet_geo,null as remarque_lieu,CASE typ_source WHEN 'Te' THEN 'T' WHEN 'Co' THEN 'H' WHEN 'Li' THEN 'B' END as type_source,null as type_doc,cd_biblio::integer as cote_biblio_cbn,lib_biblio as titre_doc,null as annee_doc,null as auteur_doc,null as ref_doc,null as code_herbarium,
	null as code_index_herbariorum,null as nom_herbarium,cd_herbier as code_herbier,lib_herbier as nom_herbier,null as part_herbier,null as id_part,null as cote_biblio_bd_mere,date_debut::date as date_debut_obs,date_fin::date as date_fin_obs,null as id_objet_geo,date_publication as date_transmission,cd_ent_mere as id_flore_mere,meta.cd_jdd as cd_jdd
	FROM hub.observation as obs
	JOIN hub.releve rel ON rel.cd_jdd = obs.cd_jdd AND rel.cd_releve = obs.cd_releve
	JOIN hub.metadonnees meta ON meta.cd_jdd = obs.cd_jdd
	JOIN ref.voca_ctrl prp ON prp.cd_champ = 'propriete_obs' AND prp.code_valeur = propriete_obs
	JOIN exploitation.taxref_no_change tnc ON obs.cd_ref::integer =  tnc.cd_ref AND obs.version_reftaxo::integer =  tnc.version
	WHERE date_debut IS NOT NULL AND date_fin IS NOT NULL
	AND cd_validite = 1;
ELSE 	out.lib_log := 'Aucune commune transférée';out.nb_occurence := '0';PERFORM hub_log ('hub', out); RETURN next out;
END CASE;


--- Les communes
SELECT count(*) INTO ct FROM hub.observation obs
	JOIN hub.releve_territoire ter ON obs.cd_jdd = ter.cd_jdd AND obs.cd_releve = ter.cd_releve
	JOIN exploitation.taxref_no_change tnc ON obs.cd_ref::integer =  tnc.cd_ref AND obs.version_reftaxo::integer =  tnc.version
	WHERE typ_geo = 'com_reu'
	AND cd_validite = 1;
-- intégration	
CASE WHEN ct <> 0 THEN
INSERT INTO observation_reunion.observation_commune_reunion (id_flore_fcbn, code_insee, type_localisation_commune, type_rattachement_commune, remarque_lieu, referentiel_communal, departement)
	SELECT obs.cd_jdd||'_'||cd_obs_mere as id_flore_fcbn, cd_geo as code_insee, confiance_geo as type_localisation_commune, moyen_geo as type_rattachement_commune, ter.rmq as remarque_lieu, cd_refgeo as referentiel_communal, null as departement
	FROM hub.observation as obs
	JOIN hub.releve_territoire ter ON obs.cd_jdd = ter.cd_jdd AND obs.cd_releve = ter.cd_releve
	JOIN exploitation.taxref_no_change tnc ON obs.cd_ref::integer =  tnc.cd_ref AND obs.version_reftaxo::integer =  tnc.version
	WHERE typ_geo = 'com_reu'
	AND cd_validite = 1;
	out.lib_log := 'communes transférées';out.nb_occurence := ct||' occurence(s)';RETURN next out;PERFORM hub_log ('hub', out);
ELSE 	out.lib_log := 'Aucune commune transférée';out.nb_occurence := '0';PERFORM hub_log ('hub', out); RETURN next out;
END CASE;

--- Les maille10
SELECT count(*) INTO ct FROM hub.observation obs
	JOIN hub.releve_territoire ter ON obs.cd_jdd = ter.cd_jdd AND obs.cd_releve = ter.cd_releve
	JOIN exploitation.taxref_no_change tnc ON obs.cd_ref::integer =  tnc.cd_ref AND obs.version_reftaxo::integer =  tnc.version
	WHERE typ_geo = 'utm10'
	AND cd_validite = 1;
-- intégration	
CASE WHEN ct <> 0 THEN
INSERT INTO observation_reunion.observation_maille_utm10 (id_flore_fcbn, cd_sig, type_localisation_maille_utm10, type_rattachement_maille_utml0,remarque_lieu)
	SELECT obs.cd_jdd||'_'||cd_obs_mere as id_flore_fcbn, cd_geo as cd_sig, confiance_geo as type_localisation_maille_utm10, moyen_geo as type_rattachement_maille_utml0, ter.rmq as remarque_lieu
	FROM hub.observation as obs
	JOIN hub.releve_territoire ter ON obs.cd_jdd = ter.cd_jdd AND obs.cd_releve = ter.cd_releve
	JOIN exploitation.taxref_no_change tnc ON obs.cd_ref::integer =  tnc.cd_ref AND obs.version_reftaxo::integer =  tnc.version
	WHERE typ_geo = 'utm10'
	AND cd_validite = 1;
	out.lib_log := 'Maille10 transférées';out.nb_occurence := ct||' occurence(s)';RETURN next out;PERFORM hub_log ('hub', out);
ELSE out.lib_log := 'aucune Maille10 transférée';out.nb_occurence := '0'; RETURN next out;PERFORM hub_log ('hub', out);
END CASE;

--- Les maille utm1
SELECT count(*) INTO ct FROM hub.observation obs
	JOIN hub.releve_territoire ter ON obs.cd_jdd = ter.cd_jdd AND obs.cd_releve = ter.cd_releve
	JOIN exploitation.taxref_no_change tnc ON obs.cd_ref::integer =  tnc.cd_ref AND obs.version_reftaxo::integer =  tnc.version
	WHERE typ_geo = 'utm1'
	AND cd_validite = 1;
-- intégration	
CASE WHEN ct <> 0 THEN
INSERT INTO observation_reunion.observation_maille_utm1 (id_flore_fcbn, nom_maille, type_localisation_maille_utm1, type_rattachement_maille_utml, remarque_lieu)
	SELECT obs.cd_jdd||'_'||cd_obs_mere as id_flore_fcbn, cd_geo as nom_maille, confiance_geo as type_localisation_maille_utm1, 	moyen_geo as type_rattachement_maille_utml, ter.rmq as remarque_lieu
	FROM hub.observation as obs
	JOIN hub.releve_territoire ter ON obs.cd_jdd = ter.cd_jdd AND obs.cd_releve = ter.cd_releve
	JOIN exploitation.taxref_no_change tnc ON obs.cd_ref::integer =  tnc.cd_ref AND obs.version_reftaxo::integer =  tnc.version
	WHERE typ_geo = 'utm1'
	AND cd_validite = 1;
	out.lib_log := 'Maille1 transférées';		out.nb_occurence := ct||' occurence(s)'; 	RETURN next out;PERFORM hub_log ('hub', out);
ELSE 	out.lib_log := 'Aucune Maille1 transférée';	out.nb_occurence := '0'; 			RETURN next out;PERFORM hub_log ('hub', out);
END CASE;

--- Suivi des mises à jour
PERFORM siflore_data_log();

END; $BODY$ LANGUAGE plpgsql;

-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction siflore_data_log
--- Description : Enregistre la date de mise à jour des données
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_data_log() RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE list_jdd varchar;
DECLARE test_jdd integer;
BEGIN 
--- Variable

--- Commande
FOR list_jdd IN SELECT cd_jdd FROM hub.metadonnees GROUP BY cd_jdd LOOP
	EXECUTE 'SELECT 1 FROM exploitation.suivi_maj_data WHERE cd_jdd = '''||list_jdd||'''' INTO test_jdd;
	CASE WHEN test_jdd = 1 THEN
		EXECUTE 'UPDATE exploitation.suivi_maj_data  SET date_maj = current_date WHERE cd_jdd = '''||list_jdd||''' ';
	ELSE
		EXECUTE 'INSERT INTO exploitation.suivi_maj_data VALUES ('''||list_jdd||''',now())';
	END CASE;
END LOOP;
-- log
out.lib_schema := 'hub';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_data_log';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
out.lib_log = 'Date enregistrée'; 
PERFORM hub_log ('hub', out); RETURN next out;
END; $BODY$ LANGUAGE plpgsql;


-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction siflore_data_drop
--- Description : Supprime un jeu de données
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_data_drop(libSchema varchar,jdd varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
BEGIN 
--- Variable

--- Commande
CASE WHEN libSchema = 'mas' THEN
	TRUNCATE observation_reunion.observation_taxon_reunion CASCADE;
	TRUNCATE observation_reunion.observation_commune_reunion CASCADE;
	TRUNCATE observation_reunion.observation_maille_utm1 CASCADE;
	TRUNCATE observation_reunion.observation_maille_utm10 CASCADE;
	EXECUTE 'DELETE FROM exploitation.suivi_maj_data WHERE cd_jdd = '''||jdd||''';';
	EXECUTE 'DELETE FROM observation.bd_mere WHERE bd_mere = '''||jdd||''';';
ELSE
	EXECUTE 'DELETE FROM exploitation.obs_commune WHERE cd_jdd = '''||jdd||''';';
	EXECUTE 'DELETE FROM exploitation.obs_maille_fr10 WHERE cd_jdd = '''||jdd||''';';
	EXECUTE 'DELETE FROM exploitation.obs_maille_fr5 WHERE cd_jdd = '''||jdd||''';';
	EXECUTE 'DELETE FROM exploitation.suivi_maj_data WHERE cd_jdd = '''||jdd||''';';
END CASE;
-- log
out.lib_schema := 'hub';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_data_drop';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;
out.lib_log = 'Jdd supprimé : '||jdd; 
PERFORM hub_log ('hub', out); RETURN next out;
END; $BODY$ LANGUAGE plpgsql;

-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction siflore_data_refresh
--- Description : Met à jour les données SI FLORE
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_data_refresh(libSchema varchar, jdd varchar = 'data', version integer = 7) RETURNS setof zz_log AS 
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
-- 1. ... HUB FCBN => HUB SIFLORE - on récupère sur le HUB SI FLORE les données provenant du schéma du CBN choisi (tables propres) dans le HUB FCBN
SELECT * INTO out FROM hub_truncate('hub','propre'); RETURN next out;
EXECUTE 'SELECT * FROM hub_connect_simple('''||jdd||''', '''||libSchema||''', ''hub'', '''||connction||''');' into out; RETURN next out;

/*problèmes code maille*/
--UPDATE hub.releve_territoire SET cd_geo = '10kmL93'||cd_geo WHERE typ_geo = 'm10' AND cd_geo NOT LIKE '10kmL93%';
--UPDATE hub.releve_territoire SET cd_geo = '5kmL93'||cd_geo WHERE typ_geo = 'm5' AND cd_geo NOT LIKE '5kmL93%';

-- 2. ... (SIFLORE) on pousse les données au sein du hub SI FLORE (suppression + ajout)
CASE WHEN jdd = 'data' THEN
	FOR listJdd IN SELECT cd_jdd FROM hub.metadonnees LOOP
		EXECUTE 'SELECT * FROM siflore_data_drop('''||libSchema||''','''||listJdd||''')' INTO out;RETURN next out;
	END LOOP;	
ELSE 
	EXECUTE 'SELECT * FROM siflore_data_drop('''||libSchema||''','''||jdd||''')' INTO out;RETURN next out;
END CASE;

/*Insertion des données dans les tables SI FLORE*/
CASE WHEN libSchema = 'mas' THEN
	SELECT * INTO out FROM siflore_insert_reunion(version); RETURN next out;
ELSE
	SELECT * INTO out FROM siflore_insert(version); RETURN next out;
END CASE;


-- log
out.lib_schema := 'hub';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_data_refresh';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;out.lib_log = 'mise à jour OK : '||libSchema; 
PERFORM hub_log ('hub', out); RETURN next out;
END; $BODY$ LANGUAGE plpgsql;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--- Nom : siflore_stat
--- Description : Produit des stats bilan sur les données d'un schema
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_stat(typ varchar = 'all', chemin varchar = '/home/export_pgsql/siflore/') RETURNS setof zz_log  AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE cmd varchar;
DECLARE prefixe varchar;
DECLARE sstyp varchar;
BEGIN
--- Variables 
out.lib_schema := libSchema;out.lib_table := '-'; out.lib_champ := '-'; out.typ_log := 'siflore_stat';SELECT CURRENT_TIMESTAMP INTO out.date_log;out.user_log := current_user;out.nb_occurence := '-';
out.lib_log := '';
prefixe := 'siflore_stat_';
--- Commandes

/*Problèmes de rattachement*/
CASE WHEN (typ = 'pb_rattachement_geo' OR typ = 'all') THEN
	cmd = 'SELECT bd_mere, ''pas de maille 10'' as pb, count(*) FROM observation.observation_taxon obs where obs.id_flore_fcbn not in (SELECT distinct id_flore_fcbn from observation.observation_maille_fr10) group by bd_mere ;
		UNION
		SELECT bd_mere, ''pas de maille 5'' as pb, count(*) FROM observation.observation_taxon obs where obs.id_flore_fcbn not in (SELECT distinct id_flore_fcbn from observation.observation_maille_fr5) group by bd_mere ;
		UNION
		SELECT bd_mere, ''pas de commune'' as pb, count(*) FROM observation.observation_taxon obs where obs.id_flore_fcbn  not in (SELECT distinct id_flore_fcbn from observation.observation_commune) group by bd_mere ;
		';
	sstyp = 'pb_rattachement_geo'; SELECT * FROM hub_file(chemin, prefixe||sstyp, cmd) INTO out.lib_log; RETURN NEXT OUT;PERFORM hub_log (libSchema, out);
ELSE END CASE;

/*nombre totale d'observation*/
CASE WHEN (typ = 'observation_total' OR typ = 'all') THEN
	cmd = 'SELECT bd_mere, count(*) as nb FROM observation_taxon GROUP BY bd_mere';
	sstyp = 'observation_total'; SELECT * FROM hub_file(chemin, prefixe||sstyp, cmd) INTO out.lib_log; RETURN NEXT OUT;PERFORM hub_log (libSchema, out);
ELSE END CASE;

/*nombre total de taxons*/
CASE WHEN (typ = 'releve' OR typ = 'all') THEN
	cmd = 'SELECT count(distinct cd_ref) FROM observation.observation_taxon GROUP BY bd_mere';
	sstyp = 'releve'; SELECT * FROM hub_file(chemin, prefixe||sstyp, cmd) INTO out.lib_log; RETURN NEXT OUT;PERFORM hub_log (libSchema, out);
ELSE END CASE;

--- Output&Log
CASE WHEN out.lib_log = '' THEN out.lib_log = 'typ mal renseigné';PERFORM hub_log (libSchema, out);RETURN NEXT out; ELSE END CASE;
END; $BODY$ LANGUAGE plpgsql;
