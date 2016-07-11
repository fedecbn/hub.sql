-------------------------------------------------------------
-------------------------------------------------------------
--- Pas à pas pour la mise à jour du SI FLORE

--- 1. Création de la base de données
-- CREATE DATABASE si_flore_national_v4 OWNER user ENCODING 'UTF-8';

--- 2. Création de la structure de la base de données SI FLORE
-- SELECT * FROM siflore_clone();

--- 3. Création d'un hub pour récupérer les données
-- récupérer les fonction du hub avec hub.sql
-- SELECT * FROM hub_connect_ref('94.23.218.10','5433','si_flore_national','user','mdp','fsd');
-- SELECT * FROM hub_admin_clone('hub');

--- 4. Création de la version SI FLORE de taxref (avec les nouvelles colonnées
-- SELECT * FROM hub_connect_ref('94.23.218.10','5433','si_flore_national','user','mdp','taxref_v7');
-- SELECT * FROM siflore_taxref_refresh(7);

--- 5. Import des nouvelles données dans le hub du SI FLORE despuis le Hub FCBN
/*-- SELECT * FROM siflore_hub_pull('94.23.218.10','5433','si_flore_national','user','mdp'); ancienne version*/ 
-- SELECT * FROM siflore_hub_pull('si_flore_national','5433');

--- 6. Mise à jour de la tables taxons
-- SELECT * FROM siflore_taxon_refresh();

--- 7. Mise à jour des données SI FLORE
-- SELECT * FROM siflore_data_refresh();
-------------------------------------------------------------
-------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.zz_log (lib_schema character varying,lib_table character varying,lib_champ character varying,typ_log character varying,lib_log character varying,nb_occurence character varying,date_log timestamp);
CREATE TABLE IF NOT EXISTS public.threecol (col1 varchar, col2 varchar, col3 varchar);
CREATE TABLE IF NOT EXISTS public.twocol (col1 varchar, col2 varchar);

-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction : siflore_clone
--- Description : Construit l'architecture du SI FLORE
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_clone() RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE listFunction varchar[];
DECLARE fonction varchar;
DECLARE exist varchar;
DECLARE base_source varchar;
BEGIN
/*variables*/
base_source = 'si_flore_national_v3';

-- Fonctions utilisées par le siflore
listFunction = ARRAY['dblink','postgis'];
FOREACH fonction IN ARRAY listFunction LOOP
	EXECUTE 'SELECT extname from pg_extension WHERE extname = '''||fonction||''';' INTO exist;
	CASE WHEN exist IS NULL THEN EXECUTE 'CREATE EXTENSION "'||fonction||'";';
	ELSE END CASE;
END LOOP;

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
CREATE TABLE exploitation.obs_commune(id_flore_fcbn character varying NOT NULL,  cd_ref integer,  nom_complet character varying,  code_taxon_mere varchar,  referentiel_mere character varying,  nom_taxon_mere character varying,  nom_taxon_originel character varying,  remarque_taxon character varying,  libelle_statut_pop character varying,  libelle_court_bd_mere character varying,  libelle_usage_donnee character varying,  libelle_court_bd_source character varying,  id_flore_source character varying,  remarque_donnee_mere character varying,  libelle_nature_date character varying,  remarque_date character varying,  remarque_lieu character varying,  libelle_type_source character varying,  type_doc character varying,  cote_biblio_cbn varchar,  titre_doc character varying,  annee_doc integer,  auteur_doc character varying,  ref_doc character varying,  code_herbarium character varying,  code_index_herbariorum character varying,  nom_herbarium character varying,  code_herbier character varying,  nom_herbier character varying,  part_herbier character varying,  id_part character varying,  cote_biblio_bd_mere character varying,  date_debut_obs date,  date_fin_obs date,  date_transmission date,  id_flore_mere character varying,  libelle_type_localisation character varying,  libelle_type_rattachement character varying,  insee_comm character varying NOT NULL,  nom_comm character varying,  geom geometry,  statut_pop character varying,  nom_observateur character varying,  prenom_observateur character varying,  libelle_organisme character varying,  observateur character varying,  CONSTRAINT obs_commune_pkey PRIMARY KEY (id_flore_fcbn, insee_comm));
DROP INDEX IF EXISTS obs_commune_cd_ref_idk;CREATE INDEX obs_commune_cd_ref_idk  ON exploitation.obs_commune  USING btree  (cd_ref);
DROP INDEX IF EXISTS obs_commune_geom_gist;CREATE INDEX obs_commune_geom_gist  ON exploitation.obs_commune  USING gist  (geom);
DROP INDEX IF EXISTS obs_commune_insee_comm_idk;CREATE INDEX obs_commune_insee_comm_idk  ON exploitation.obs_commune  USING btree  (insee_comm COLLATE pg_catalog."default");
DROP INDEX IF EXISTS obs_commune_nom_comm_idk;CREATE INDEX obs_commune_nom_comm_idk  ON exploitation.obs_commune  USING btree  (nom_comm COLLATE pg_catalog."default");
--- Table: exploitation.obs_maille_fr10
CREATE TABLE exploitation.obs_maille_fr10(id_flore_fcbn character varying NOT NULL,  cd_ref integer,  nom_complet character varying,  code_taxon_mere varchar,  referentiel_mere character varying,  nom_taxon_mere character varying,  nom_taxon_originel character varying,  remarque_taxon character varying,  libelle_statut_pop character varying,  libelle_court_bd_mere character varying,  libelle_usage_donnee character varying,  libelle_court_bd_source character varying,  id_flore_source character varying,  remarque_donnee_mere character varying,  libelle_nature_date character varying,  remarque_date character varying,  remarque_lieu character varying,  libelle_type_source character varying,  type_doc character varying,  cote_biblio_cbn varchar,  titre_doc character varying,  annee_doc integer,  auteur_doc character varying,  ref_doc character varying,  code_herbarium character varying,  code_index_herbariorum character varying,  nom_herbarium character varying,  code_herbier character varying,  nom_herbier character varying,  part_herbier character varying,  id_part character varying,  cote_biblio_bd_mere character varying,  date_debut_obs date,  date_fin_obs date,  date_transmission date,  id_flore_mere character varying,  cd_sig character varying NOT NULL,  libelle_type_localisation character varying,  libelle_type_rattachement character varying,  geom geometry(MultiPolygon,2154),  statut_pop character varying,  nom_observateur character varying,  prenom_observateur character varying,  libelle_organisme character varying,  observateur character varying,  CONSTRAINT obs_maille_fr10_pkey PRIMARY KEY (id_flore_fcbn, cd_sig));
DROP INDEX IF EXISTS obs_maille_fr10_cd_ref_idk; CREATE INDEX obs_maille_fr10_cd_ref_idk  ON exploitation.obs_maille_fr10  USING btree  (cd_ref);
DROP INDEX IF EXISTS obs_maille_fr10_cd_sig_idk; CREATE INDEX obs_maille_fr10_cd_sig_idk  ON exploitation.obs_maille_fr10  USING btree  (cd_sig COLLATE pg_catalog."default");
DROP INDEX IF EXISTS obs_maille_fr10_geom_gist; CREATE INDEX obs_maille_fr10_geom_gist ON exploitation.obs_maille_fr10  USING gist  (geom);
-- Table: exploitation.obs_maille_fr5
CREATE TABLE exploitation.obs_maille_fr5 (id_flore_fcbn character varying NOT NULL,  cd_ref integer,  nom_complet character varying,  code_taxon_mere varchar,  referentiel_mere character varying,  nom_taxon_mere character varying,  nom_taxon_originel character varying,  remarque_taxon character varying,  libelle_statut_pop character varying,  libelle_court_bd_mere character varying,  libelle_usage_donnee character varying,  libelle_court_bd_source character varying,  id_flore_source character varying,  remarque_donnee_mere character varying,  libelle_nature_date character varying,  remarque_date character varying,  remarque_lieu character varying,  libelle_type_source character varying,  type_doc character varying,  cote_biblio_cbn varchar,  titre_doc character varying,  annee_doc integer,  auteur_doc character varying,  ref_doc character varying,  code_herbarium character varying,  code_index_herbariorum character varying,  nom_herbarium character varying,  code_herbier character varying,  nom_herbier character varying,  part_herbier character varying,  id_part character varying,  cote_biblio_bd_mere character varying,  date_debut_obs date,  date_fin_obs date,  date_transmission date,  id_flore_mere character varying,  cd_sig character varying NOT NULL,  libelle_type_localisation character varying(20),  libelle_type_rattachement character varying,  geom geometry(MultiPolygon,2154),  statut_pop character varying,  nom_observateur character varying,  prenom_observateur character varying,  libelle_organisme character varying,  observateur character varying,  CONSTRAINT obs_maille_fr5_pkey PRIMARY KEY (id_flore_fcbn, cd_sig));
DROP INDEX IF EXISTS obs_maille_fr5_cd_ref_idk;CREATE INDEX obs_maille_fr5_cd_ref_idk  ON exploitation.obs_maille_fr5  USING btree  (cd_ref);
DROP INDEX IF EXISTS obs_maille_fr5_cd_sig_idk;CREATE INDEX obs_maille_fr5_cd_sig_idk  ON exploitation.obs_maille_fr5  USING btree  (cd_sig COLLATE pg_catalog."default");
DROP INDEX IF EXISTS obs_maille_fr5_geom_gist;CREATE INDEX obs_maille_fr5_geom_gist  ON exploitation.obs_maille_fr5  USING gist  (geom);
-- Table: exploitation.taxref_v5
CREATE TABLE exploitation.taxref_v5_new (cd_ref integer NOT NULL,  nom_complet character varying NOT NULL,  regne character varying,  phylum character varying,  classe character varying,  ordre character varying, famille character varying,  cd_taxsup integer,  rang character varying,  lb_nom character varying,  lb_auteur character varying,  nom_vern character varying,  nom_vern_eng character varying, habitat character varying,  liste_bryo boolean DEFAULT false,  bryophyta boolean DEFAULT false,  cd_taxsup2 integer,  cd_taxsup3 integer,  cd_taxsup4 integer,  CONSTRAINT taxref_v5_pkey PRIMARY KEY (cd_ref, nom_complet));
DROP INDEX IF EXISTS cd_ref_5_idk;CREATE INDEX cd_ref_5_idk  ON exploitation.taxref_v5_new  USING btree (cd_ref);
DROP INDEX IF EXISTS cd_taxsup2_5_idk;CREATE INDEX cd_taxsup2_5_idk ON exploitation.taxref_v5_new USING btree (cd_taxsup2);
DROP INDEX IF EXISTS cd_taxsup3_5_idk;CREATE INDEX cd_taxsup3_5_idk ON exploitation.taxref_v5_new USING btree (cd_taxsup3);
DROP INDEX IF EXISTS cd_taxsup4_5_idk;CREATE INDEX cd_taxsup4_5_idk ON exploitation.taxref_v5_new USING btree (cd_taxsup4);
DROP INDEX IF EXISTS cd_taxsup_5_idk;CREATE INDEX cd_taxsup_5_idk ON exploitation.taxref_v5_new USING btree (cd_taxsup);
EXECUTE 'INSERT INTO exploitation.taxref_v5_new SELECT * FROM dblink(''dbname='||base_source||''',''SELECT * FROM exploitation.taxref_v5_new;'') as t1(cd_ref integer,  nom_complet character varying,  regne character varying,  phylum character varying,  classe character varying,  ordre character varying, famille character varying,  cd_taxsup integer,  rang character varying,  lb_nom character varying,  lb_auteur character varying,  nom_vern character varying,  nom_vern_eng character varying, habitat character varying,  liste_bryo boolean,  bryophyta boolean,  cd_taxsup2 integer,  cd_taxsup3 integer,  cd_taxsup4 integer)';
-- Table: exploitation.taxref_v7
CREATE TABLE exploitation.taxref_v7_new (cd_ref integer NOT NULL,  nom_complet character varying NOT NULL,  regne character varying,  phylum character varying,  classe character varying,  ordre character varying, famille character varying,  cd_taxsup integer,  rang character varying,  lb_nom character varying,  lb_auteur character varying,  nom_vern character varying,  nom_vern_eng character varying, habitat character varying,  liste_bryo boolean DEFAULT false,  bryophyta boolean DEFAULT false,  cd_taxsup2 integer,  cd_taxsup3 integer,  cd_taxsup4 integer,  CONSTRAINT taxref_v7_pkey PRIMARY KEY (cd_ref, nom_complet));
DROP INDEX IF EXISTS cd_ref_7_idk;CREATE INDEX cd_ref_7_idk  ON exploitation.taxref_v7_new  USING btree (cd_ref);
DROP INDEX IF EXISTS cd_taxsup2_7_idk;CREATE INDEX cd_taxsup2_7_idk ON exploitation.taxref_v7_new USING btree (cd_taxsup2);
DROP INDEX IF EXISTS cd_taxsup3_7_idk;CREATE INDEX cd_taxsup3_7_idk ON exploitation.taxref_v7_new USING btree (cd_taxsup3);
DROP INDEX IF EXISTS cd_taxsup4_7_idk;CREATE INDEX cd_taxsup4_7_idk ON exploitation.taxref_v7_new USING btree (cd_taxsup4);
DROP INDEX IF EXISTS cd_taxsup_7_idk;CREATE INDEX cd_taxsup_7_idk ON exploitation.taxref_v7_new USING btree (cd_taxsup);
-- Table: exploitation.taxons
CREATE TABLE exploitation.taxons(cd_ref integer NOT NULL, nom_complet text NOT NULL,  rang character varying,  type character varying,  CONSTRAINT taxons_new_pkey PRIMARY KEY (cd_ref, nom_complet));
-- Table: exploitation.taxons_communs
CREATE TABLE exploitation.taxons_communs(  cd_ref integer NOT NULL,  nom_complet character varying,  taxons_communs_ss_inf character varying,  taxons_communs_av_inf character varying,  CONSTRAINT pk_taxons_communs PRIMARY KEY (cd_ref));
-- TAbles : les synthèses
CREATE TABLE exploitation.synthese_taxon_comm(cd_ref integer,  nom_complet character varying,  nb_obs bigint,  nb_obs_1500_1980 bigint,  nb_obs_1981_2000 bigint,  nb_obs_2001_2013 bigint,  nb_obs_averee bigint,  nb_obs_interpretee bigint,  date_premiere_obs date,  date_derniere_obs date);
CREATE TABLE exploitation.synthese_taxon_fr10(cd_ref integer,  nom_complet character varying,  nb_obs bigint,  nb_obs_1500_1980 bigint,  nb_obs_1981_2000 bigint,  nb_obs_2001_2013 bigint,  nb_obs_averee bigint,  nb_obs_interpretee bigint,  date_premiere_obs date,  date_derniere_obs date);
CREATE TABLE exploitation.synthese_taxon_fr5  (cd_ref integer,  nom_complet character varying,  nb_obs bigint,  nb_obs_1500_1980 bigint,  nb_obs_1981_2000 bigint,  nb_obs_2001_2013 bigint,  nb_obs_averee bigint,  nb_obs_interpretee bigint,  date_premiere_obs date,  date_derniere_obs date);
--CREATE TABLE exploitation.synthese_maille_fr10( cd_sig character varying,  nb_taxons integer,  nb_obs integer,  nb_obs_1500_1980 integer,  nb_obs_1981_2000 integer,  nb_obs_2001_2013 integer,  nb_obs_averee integer,  nb_obs_interpretee integer,  date_premiere_obs date,  date_derniere_obs date,  geom geometry(MultiPolygon,2154),  gid integer,  CONSTRAINT synthese_maille_fr10_pkey PRIMARY KEY (cd_sig));
CREATE TABLE exploitation.information_taxa_taxons(  cd_ref text,  famille text,  nom_sci text,  cd_rang text,  "national" text,  alsace text,  aquitaine text,  auvergne text,  basse_normandie text,  bourgogne text,  bretagne text,  centre text,  champagne_ardenne text,  corse text,  franche_comte text,  haute_normandie text,  ile_de_france text,  languedoc_roussillon text,  limousin text,  lorraine text,  midi_pyrenees text,  nord_pas_de_calais text,  pays_de_la_loire text,  picardie text,  poitou_charentes text,  paca text,  rhone_alpes text);
CREATE TABLE exploitation.information_taxons(  cd_ref integer,  nom_complet character varying,  url text,  num_nom_tela character varying,  num_nom_retenu_tela character varying,  nom_sci character varying,  cd_nom integer);

--- Schema: observation_reunion;
DROP SCHEMA IF EXISTS observation_reunion CASCADE; CREATE SCHEMA observation_reunion;
-- Table: observation_reunion.communes_bdtopo_reunion
CREATE TABLE observation_reunion.communes_bdtopo_reunion(  gid serial NOT NULL,  id character varying,  prec_plani double precision,  nom character varying,  code_insee character varying NOT NULL,  statut character varying,  canton character varying,  arrondisst character varying,  depart character varying,  region character varying,  popul integer,  multican character varying,  geom geometry(MultiPolygon,2975),  geom_3857_s100 geometry(MultiPolygon,3857),  geom_3857 geometry(MultiPolygon,3857),  CONSTRAINT communes_bd_topo_reunion_pkey PRIMARY KEY (code_insee));
INSERT INTO observation_reunion.communes_bdtopo_reunion SELECT * FROM dblink('dbname=si_flore_national_v3','SELECT * FROM observation_reunion.communes_bdtopo_reunion;') as t1(gid integer,  id character varying(24),  prec_plani double precision,  nom character varying,  code_insee character varying,  statut character varying,  canton character varying(45),  arrondisst character varying,  depart character varying,  region character varying,  popul integer,  multican character varying,  geom geometry(MultiPolygon,2975),  geom_3857_s100 geometry(MultiPolygon,3857),  geom_3857 geometry(MultiPolygon,3857));
DROP INDEX IF EXISTS communes_bdtopo_reunion_geom_gist; CREATE INDEX communes_bdtopo_reunion_geom_gist  ON observation_reunion.communes_bdtopo_reunion  USING gist  (geom);
-- Table: observation_reunion.grille_10km_zee_974
CREATE TABLE observation_reunion.grille_10km_zee_974(  gid serial NOT NULL,  cd_sig character varying NOT NULL,  code_10km character varying,  geom geometry(MultiPolygon,2975),  CONSTRAINT grille_10km_zee_974_pkey PRIMARY KEY (cd_sig));
INSERT INTO observation_reunion.grille_10km_zee_974 SELECT * FROM dblink('dbname=si_flore_national_v3','SELECT * FROM observation_reunion.grille_10km_zee_974;') as t1 (  gid integer,  cd_sig character varying(21),  code_10km character varying,  geom geometry(MultiPolygon,2975));
DROP INDEX IF EXISTS grille_10km_zee_974_geom_gist;CREATE INDEX grille_10km_zee_974_geom_gist  ON observation_reunion.grille_10km_zee_974  USING gist  (geom);
-- Table: observation_reunion.maille_utm1
CREATE TABLE observation_reunion.maille_utm1(gid serial,  nom_maille character varying NOT NULL,  centroid_x character varying,  centroid_y character varying,  geom geometry(MultiPolygon,2975),  geom_3857_s100 geometry(MultiPolygon,3857),  geom_3857 geometry(MultiPolygon,3857),  CONSTRAINT maille_utm1_pkey PRIMARY KEY (nom_maille));
INSERT INTO observation_reunion.maille_utm1 SELECT * FROM dblink('dbname=si_flore_national_v3','SELECT * FROM observation_reunion.maille_utm1;') as t1 (gid integer,  nom_maille character varying,  centroid_x character varying,  centroid_y character varying,  geom geometry(MultiPolygon,2975),  geom_3857_s100 geometry(MultiPolygon,3857),  geom_3857 geometry(MultiPolygon,3857));
DROP INDEX IF EXISTS maille_1x1_utm_geom_gist;CREATE INDEX maille_1x1_utm_geom_gist  ON observation_reunion.maille_utm1  USING gist  (geom);
-- Table: observation_reunion.index_reunion
CREATE TABLE observation_reunion.index_reunion( code_taxon integer NOT NULL,  nom_taxon character varying NOT NULL,  cd_ref integer,  nom_complet character varying,  CONSTRAINT index_reunion_pkey PRIMARY KEY (code_taxon, nom_taxon));
DROP INDEX IF EXISTS idk_code_taxon;CREATE INDEX idk_code_taxon  ON observation_reunion.index_reunion  USING btree  (code_taxon);
-- Table: observation_reunion.observation_taxon_reunion
CREATE TABLE observation_reunion.observation_taxon_reunion(  id_flore_fcbn character varying NOT NULL,  code_taxon integer NOT NULL,  nom_taxon character varying,  code_taxon_mere varchar,  referentiel_mere character varying,  nom_taxon_mere character varying,  nom_taxon_originel character varying,  remarque_taxon character varying,  statut_pop character varying NOT NULL,  bd_mere character varying NOT NULL,  usage_donnee integer NOT NULL,  bd_source character varying,  libelle_court_bd_source character varying,  id_flore_source character varying,  sup_donnee boolean,  remarque_donnee_mere character varying,  nature_date character(2) NOT NULL,  remarque_date character varying,  syst_ref_spatial character varying,  nature_objet_geo character varying,  remarque_lieu character varying,  type_source character(1) NOT NULL,  type_doc character varying,  cote_biblio_cbn varchar,  titre_doc character varying,  annee_doc integer,  auteur_doc character varying,  ref_doc character varying,  code_herbarium character varying,  code_index_herbariorum character varying,  nom_herbarium character varying,  code_herbier character varying,  nom_herbier character varying,  part_herbier character varying,  id_part character varying,  cote_biblio_bd_mere character varying,  date_debut_obs date NOT NULL,  date_fin_obs date NOT NULL,  id_objet_geo integer,  date_transmission date NOT NULL,  id_flore_mere character varying,  CONSTRAINT observation_taxon_pkey PRIMARY KEY (id_flore_fcbn),  CONSTRAINT observation_taxon_bd_mere_fkey FOREIGN KEY (bd_mere)      REFERENCES observation.bd_mere (bd_mere) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_taxon_code_taxon_fkey FOREIGN KEY (code_taxon, nom_taxon)      REFERENCES observation_reunion.index_reunion (code_taxon, nom_taxon) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_taxon_nature_date_fkey FOREIGN KEY (nature_date)      REFERENCES observation.nature_date (nature_date) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_taxon_nature_objet_geo_fkey FOREIGN KEY (nature_objet_geo)      REFERENCES observation.nature_objet_geo (nature_objet_geo) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_taxon_statut_pop_fkey FOREIGN KEY (statut_pop)      REFERENCES observation.statut_pop (statut_pop) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_taxon_syst_ref_spatial_fkey FOREIGN KEY (syst_ref_spatial)      REFERENCES observation.syst_ref_spatial (syst_ref_spatial) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_taxon_type_source_fkey FOREIGN KEY (type_source)      REFERENCES observation.type_source (type_source) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_taxon_usage_donnee_fkey FOREIGN KEY (usage_donnee)      REFERENCES observation.usage_donnee (usage_donnee) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION);
DROP INDEX IF EXISTS ifk_code_taxon_obs; CREATE INDEX ifk_code_taxon_obs  ON observation_reunion.observation_taxon_reunion  USING btree  (code_taxon, nom_taxon COLLATE pg_catalog."default");
-- Table: observation_reunion.observation_maille_utm1
CREATE TABLE observation_reunion.observation_maille_utm1(  id_flore_fcbn character varying NOT NULL,  nom_maille character varying NOT NULL,  type_localisation_maille_utm1 character(1) NOT NULL,  type_rattachement_maille_utml character(1) NOT NULL,  remarque_lieu character varying,  id serial NOT NULL,  CONSTRAINT observation_maille_utm1_pkey PRIMARY KEY (id_flore_fcbn, nom_maille),  CONSTRAINT observation_maille_utm1_id_flore_fcbn_fkey FOREIGN KEY (id_flore_fcbn)      REFERENCES observation_reunion.observation_taxon_reunion (id_flore_fcbn) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_maille_utm1_nom_maille_fkey FOREIGN KEY (nom_maille)      REFERENCES observation_reunion.maille_utm1 (nom_maille) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_maille_utm1_type_localisation_maille_utm1_fkey FOREIGN KEY (type_localisation_maille_utm1)      REFERENCES observation.type_localisation (type_localisation) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_maille_utm1_type_rattachement_maille_utm1_fkey FOREIGN KEY (type_rattachement_maille_utml)      REFERENCES observation.type_rattachement (type_rattachement) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION);
-- Table: observation_reunion.observation_maille_utm10
CREATE TABLE observation_reunion.observation_maille_utm10(  id_flore_fcbn character varying NOT NULL,  cd_sig character varying NOT NULL,  type_localisation_maille_utm10 character(1) NOT NULL, type_rattachement_maille_utml0 character(1) NOT NULL,  remarque_lieu character varying,  id serial NOT NULL,  CONSTRAINT observation_maille_utm10_pkey PRIMARY KEY (id_flore_fcbn, cd_sig),  CONSTRAINT observation_maille_utm10_cd_sig_fkey FOREIGN KEY (cd_sig)      REFERENCES observation_reunion.grille_10km_zee_974 (cd_sig) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_maille_utm10_id_flore_fcbn_fkey FOREIGN KEY (id_flore_fcbn)      REFERENCES observation_reunion.observation_taxon_reunion (id_flore_fcbn) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_maille_utm10_type_localisation_maille_utm10_fkey FOREIGN KEY (type_localisation_maille_utm10)      REFERENCES observation.type_localisation (type_localisation) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_maille_utm10_type_rattachement_maille_utm10_fkey FOREIGN KEY (type_rattachement_maille_utml0)      REFERENCES observation.type_rattachement (type_rattachement) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION);
-- Table: observation_reunion.observation_commune_reunion
CREATE TABLE observation_reunion.observation_commune_reunion(  id_flore_fcbn character varying NOT NULL,  code_insee character varying NOT NULL,  type_localisation_commune character(1) NOT NULL,  type_rattachement_commune character(1) NOT NULL,  remarque_lieu character varying,  referentiel_communal character varying NOT NULL,  departement character(3),  id serial NOT NULL,  CONSTRAINT observation_commune_reunion_pkey PRIMARY KEY (id_flore_fcbn, code_insee),  CONSTRAINT observation_commune_reunion_code_insee_fkey FOREIGN KEY (code_insee)      REFERENCES observation_reunion.communes_bdtopo_reunion (code_insee) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_commune_reunion_id_flore_fcbn_fkey FOREIGN KEY (id_flore_fcbn)      REFERENCES observation_reunion.observation_taxon_reunion (id_flore_fcbn) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_commune_reunion_type_localisation_commune_fkey FOREIGN KEY (type_localisation_commune)      REFERENCES observation.type_localisation (type_localisation) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION,  CONSTRAINT observation_commune_reunion_type_rattachement_commune_fkey FOREIGN KEY (type_rattachement_commune)      REFERENCES observation.type_rattachement (type_rattachement) MATCH SIMPLE      ON UPDATE NO ACTION ON DELETE NO ACTION);

--- Log
out.lib_log := 'Siflore créé';out.lib_schema := '-';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_clone';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;PERFORM hub_log ('public', out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;


-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction : siflore_right
--- Description : Met à jour taxref en fonction de la version souhaité - aujourd'hui ajout de taxref_v7_new
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_right(opt varchar = 'add', this_user varchar = null) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE ListUser varchar[];
DECLARE oneUser varchar;
BEGIN
CASE WHEN user IS null THEN
	ListUser = ARRAY['cbn_c_interface','cbn_med_interface','cbn_bl_interface','cbn_bp_interface','cbn_alp_interface','cbn_mc_interface','cbn_fc_interface','cbn_mas_interface','cbn_b_interface','a_just_interface','j_gourvil_interface','m_decherf_interface','j_millet_interface','r_gaspard_interface','i_mandon_interface','cbn_sa_interface','cbn_pmp_interface','plateforme_siflore','lecteur_masao'];
ELSE ListUser = ARRAY [this_user];
END CASE;
/*Gestion des droits*/
CASE WHEN opt = 'add' THEN
FOREACH oneUser IN ARRAY ListUser LOOP
	EXECUTE 'GRANT CONNECT ON DATABASE si_flore_national_v4 TO '||oneUser||';';
	EXECUTE 'GRANT USAGE ON SCHEMA exploitation TO '||oneUser||';';
	EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA exploitation TO '||oneUser||';';
	EXECUTE 'GRANT USAGE ON SCHEMA observation TO '||oneUser||';';
	EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA observation TO '||oneUser||';';
	EXECUTE 'GRANT USAGE ON SCHEMA observation_reunion TO '||oneUser||';';
	EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA observation_reunion TO '||oneUser||';';
	out.lib_log := 'user supprimé';
END LOOP;
WHEN opt = 'drop' THEN
	EXECUTE 'REVOKE ALL PRIVILEGES ON DATABASE si_flore_national_v4 TO '||oneUser||';';
	out.lib_log := 'user supprimé';
ELSE out.lib_log := 'Paramètre incorrecte';
END CASE;
--- Log
out.lib_schema := '-';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_right';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log;PERFORM hub_log ('public', out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;


-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction : siflore_taxref_refresh
--- Description : Met à jour taxref en fonction de la version souhaité - aujourd'hui ajout de taxref_v7_new
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_taxref_refresh(version integer) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
BEGIN
/*Mise à jour de la table taxref - suppression et recréation*/
DROP TABLE IF EXISTS exploitation.taxref;
CREATE TABLE exploitation.taxref (cd_ref integer NOT NULL,  nom_complet character varying NOT NULL,  regne character varying,  phylum character varying,  classe character varying,  ordre character varying, famille character varying,  cd_taxsup integer,  rang character varying,  lb_nom character varying,  lb_auteur character varying,  nom_vern character varying,  nom_vern_eng character varying, habitat character varying,  liste_bryo boolean DEFAULT false,  bryophyta boolean DEFAULT false,  cd_taxsup2 integer,  cd_taxsup3 integer,  cd_taxsup4 integer,  CONSTRAINT taxref_pkey PRIMARY KEY (cd_ref, nom_complet));
DROP INDEX IF EXISTS cd_ref_idk;CREATE INDEX cd_ref_idk  ON exploitation.taxref  USING btree (cd_ref);
DROP INDEX IF EXISTS cd_taxsup2_idk;CREATE INDEX cd_taxsup2_idk ON exploitation.taxref USING btree (cd_taxsup2);
DROP INDEX IF EXISTS cd_taxsup3_idk;CREATE INDEX cd_taxsup3_idk ON exploitation.taxref USING btree (cd_taxsup3);
DROP INDEX IF EXISTS cd_taxsup4_idk;CREATE INDEX cd_taxsup4_idk ON exploitation.taxref USING btree (cd_taxsup4);
DROP INDEX IF EXISTS cd_taxsup_idk;CREATE INDEX cd_taxsup_idk ON exploitation.taxref USING btree (cd_taxsup);

/*Alimentation de la table taxref*/
EXECUTE '
INSERT INTO exploitation.taxref(cd_ref, nom_complet, regne, phylum, classe, ordre, famille, cd_taxsup, rang, lb_nom, lb_auteur, nom_vern, nom_vern_eng, habitat)
SELECT cd_ref::integer, nom_complet, regne, phylum, classe, ordre, famille, cd_taxsup::integer, rang, lb_nom, lb_auteur, nom_vern, nom_vern_eng, habitat
FROM ref.taxref_v'||version||'
WHERE (regne = ''Plantae'' OR  regne = ''Fungi'' OR regne = ''Chromista'') AND cd_nom = cd_ref
GROUP BY cd_ref, nom_complet, regne, phylum, classe, ordre, famille, cd_taxsup, rang, lb_nom, lb_auteur, nom_vern, nom_vern_eng, habitat;
';

--- taxsup2
UPDATE exploitation.taxref i SET cd_taxsup2 = out.cd_taxsup FROM (SELECT cd_ref, cd_taxsup FROM exploitation.taxref) as out WHERE out.cd_ref = i.cd_taxsup;
--- taxsup3
UPDATE exploitation.taxref i SET cd_taxsup3 = out.cd_taxsup FROM (SELECT cd_ref, cd_taxsup FROM exploitation.taxref) as out WHERE out.cd_ref = i.cd_taxsup2;
--- taxsup4
UPDATE exploitation.taxref i SET cd_taxsup4 = out.cd_taxsup FROM (SELECT cd_ref, cd_taxsup FROM exploitation.taxref) as out WHERE out.cd_ref = i.cd_taxsup3;
/*Bryophyta et liste bryo*/
UPDATE exploitation.taxref SET liste_bryo = TRUE WHERE famille in (SELECT famille FROM exploitation.taxref_v5_new WHERE liste_bryo IS TRUE GROUP BY famille, liste_bryo);
UPDATE exploitation.taxref SET bryophyta = TRUE WHERE famille in (SELECT famille FROM exploitation.taxref_v5_new WHERE bryophyta IS TRUE GROUP BY famille, bryophyta);


--- Log
out.lib_log := 'Taxref OK pour la version '||version;out.lib_schema := '-';out.lib_table := 'exploitation.taxref';out.lib_champ := '-';out.typ_log := 'siflore_taxref_refresh';out.nb_occurence := 1; SELECT CURRENT_TIMESTAMP INTO out.date_log; PERFORM hub_log ('public', out);RETURN NEXT out;
END; $BODY$ LANGUAGE plpgsql;


-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction siflore_taxon_refresh
--- Description : Mise à jour de la liste des taxons
--------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_taxon_refresh() RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
BEGIN 
--- exploitation.taxon
TRUNCATE exploitation.taxons; 
INSERT INTO exploitation.taxons 
SELECT a.cd_ref::integer, nom_ent_ref, z.rang, 
CASE WHEN liste_bryo IS TRUE AND bryophyta IS TRUE THEN 'bryo_liste' WHEN liste_bryo IS FALSE AND bryophyta IS TRUE THEN 'bryo_pas_liste' WHEN liste_bryo IS FALSE AND bryophyta IS FALSE THEN 'tracheo' ELSE 'problème' END as type
FROM hub.observation a
JOIN exploitation.taxref_v7_new z ON a.cd_ref::integer = z.cd_ref
GROUP BY a.cd_ref, nom_ent_ref, z.rang, liste_bryo, bryophyta;
/*
TRUNCATE exploitation.taxons;
INSERT INTO exploitation.taxons SELECT * FROM dblink('dbname =si_flore_national_v3','SELECT * FROM exploitation.taxons') as t1(cd_ref integer , nom_complet character varying,  rang character varying, type character varying);

*/
--- exploitation.taxon_commune
/*
TRUNCATE exploitation.taxons_communs; 
INSERT INTO exploitation.taxons_communs SELECT * FROM dblink('dbname =si_flore_national_v3','SELECT * FROM exploitation.taxons_communs') as t1(cd_ref integer , nom_complet character varying,  taxons_communs_ss_inf character varying, taxons_communs_av_inf character varying);
*/
--- Log
out.lib_schema := 'hub';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_taxon_refresh';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.lib_log = '-'; PERFORM hub_log ('public', out); RETURN next out;
END; $BODY$ LANGUAGE plpgsql;

-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction siflore_hub_pull
--- Description : Copie les données mises à jour récemment dans le hub dans un hub SI FLORE
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
-- CREATE OR REPLACE FUNCTION siflore_hub_pull(hote varchar, port varchar,dbname varchar,utilisateur varchar,mdp varchar) RETURNS setof zz_log AS 
CREATE OR REPLACE FUNCTION siflore_hub_pull(dbname varchar, port varchar) RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE out_maj twocol%rowtype;
DECLARE connction varchar;
DECLARE cmd varchar;
BEGIN 
--- Variable
--connction = 'hostaddr='||hote||' port='||port||' dbname='||dbname||' user='||utilisateur||' password='||mdp||'';
connction = 'dbname='||dbname||' port='||port;
--- Commande
-- 0. pour tous les jeux de données nouvellement poussées...
cmd = 'SELECT lib_schema, nb_occurence as typ_jdd FROM public.zz_log
	WHERE typ_log = ''''hub_push'''' AND date_log >= current_date -1 AND lib_log LIKE ''''Données poussées%''''
	GROUP BY lib_schema, nb_occurence ORDER BY lib_schema;
	';
FOR out_maj IN EXECUTE 'SELECT * from dblink('''||connction||''', '''||cmd||''') as t1 (cbn varchar, jdd varchar);'
	LOOP
	-- 1. ... on récupère sur le SI FLORE les données du Hub FCBN - schéma agrégation
	--EXECUTE 'SELECT * FROM hub_connect('''||hote||''', '''|port||''','''||dbname||''','''||utilisateur||''','''||mdp||''', '''||out_maj.col2||''', '''||out_maj.col1||''', ''hub'');';
	EXECUTE 'SELECT * FROM hub_simple_connect('''||connction||''', '''||out_maj.col2||''', '''||out_maj.col1||''', ''hub'');';
	-- 2. ... (SIFLORE) on pousse les données dans le hub SI FLORE.
	EXECUTE 'SELECT * FROM hub_push(''hub'','''||out_maj.col2||''', ''add'');';
	-- 3. ... (SIFLORE) on nettoie les données dans le hub SI FLORE
	EXECUTE 'SELECT * FROM hub_clear(''hub'','''||out_maj.col2||''');';
	-- log
	out.lib_schema := 'hub';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_refresh';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;out.lib_log = 'mise à jour OK : '||out_maj.col2; PERFORM hub_log ('public', out); RETURN next out;
END LOOP;
END; $BODY$ LANGUAGE plpgsql;


-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction siflore_data_refresh
--- Description : Met à jour les données du SI FLORE depuis la copie du hub dans le SIFLORE (utilise siflore_data_query)
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_data_refresh() RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE jdd threecol%rowtype;
DECLARE cmd varchar;
BEGIN 
--- Commande
-- 0. pour tous les jeux de données nouvellement poussées (c'est à dire dans la partie temporaire du hub SI FLORE)
cmd = 'SELECT typ_geo, cd_geo, cd_jdd FROM hub.metadonnees_territoire WHERE cd_geo = ''cor'';';
FOR jdd IN EXECUTE cmd
	LOOP
	-- 4. on mets à jour les tables du SI FLORE
	CASE WHEN (jdd.col1 = 'dpt' AND jdd.col2 = '974') OR (jdd.col1 = 'reg' AND jdd.col2 = '4') OR (jdd.col1 = 'cbn' AND jdd.col2 = 'mas') THEN
		--- obs_commune
		--EXECUTE 'SELECT * FROM siflore_data_query(''commune_mas'','''||jdd.col3||''');' INTO out; RETURN next out;
		--- obs_maille5
		--EXECUTE 'SELECT * FROM siflore_data_query(''maille1_mas'','''||jdd.col3||''');' INTO out; RETURN next out;
		--- obs_maill10
		--EXECUTE 'SELECT * FROM siflore_data_query(''maille10_mas'','''||jdd.col3||''');' INTO out; RETURN next out;
	ELSE
		--- obs_commune
		EXECUTE 'SELECT * FROM siflore_data_query(''commune'','''||jdd.col3||''');' INTO out; RETURN next out;
		--- obs_maille5
		EXECUTE 'SELECT * FROM siflore_data_query(''maille5'','''||jdd.col3||''');' INTO out; RETURN next out;
		--- obs_maill10
		EXECUTE 'SELECT * FROM siflore_data_query(''maille10'','''||jdd.col3||''');' INTO out; RETURN next out;	
	END CASE;END LOOP;
END; $BODY$ LANGUAGE plpgsql;


-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction siflore_data_global_refresh
--- Description : Met à jour toutes les données du SI FLORE depuis la copie du hub dans le SIFLORE
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_data_global_refresh() RETURNS setof zz_log AS 
$BODY$
DECLARE out zz_log%rowtype;
DECLARE jdd threecol%rowtype;
DECLARE cmd varchar;
BEGIN 
--- Les communes
TRUNCATE exploitation.obs_commune;
INSERT INTO exploitation.obs_commune
SELECT	obs.cd_jdd||'_'||cd_obs_mere as id_flore_fcbn,	cd_ref::integer as cd_ref, 	nom_ent_ref as nom_complet, 	cd_ent_mere as code_taxon_mere,	null as referentiel_mere,	nom_ent_mere as nom_taxon_mere,	nom_ent_orig as nom_taxon_originel, 
	obs.rmq as remarque_taxon,	stp.libelle_valeur as libelle_statut_pop, 	lib_jdd as libelle_court_bd_mere, 	prp.libelle_valeur as libelle_usage_donnee,	lib_jdd_orig as libelle_court_bd_source, 	cd_obs_orig as id_flore_source, 
	rel.rmq as remarque_donnee_mere,	ntd.libelle_valeur as libelle_nature_date,	rel.rmq as remarque_date, 	ter.rmq as remarque_lieu,	tso.libelle_valeur as libelle_type_source,	null as type_doc,
	cd_biblio as cote_biblio_cbn,	lib_biblio as titre_doc,	null as annee_doc,	null as auteur_doc,	null as ref_doc,	null as code_herbarium,	null as code_index_herbariorum,	null as nom_herbarium,	cd_herbier as code_herbier,
	lib_herbier as nom_herbier,	null as part_herbier,	null as id_part,	null as cote_biblio_bd_mere,	date_debut::date as date_debut_obs,	date_fin::date as date_fin_obs,	null as date_transmission,	cd_ent_mere as id_flore_mere,
	cfg.libelle_valeur, mog.libelle_valeur,cd_geo, lib_geo, com.geom,	statut_pop,	null as nom_observateur,	null as prenom_observateur,	'à mettre à jour' as libelle_organisme,	'à mettre à jour' as observateur
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
	WHERE typ_geo = 'com'
	AND obs.cd_jdd <> 'MAS02'
	AND cd_validite = 1;

--- Les maille10
TRUNCATE exploitation.obs_maille_fr10;
INSERT INTO exploitation.obs_maille_fr10
SELECT	obs.cd_jdd||'_'||cd_obs_mere as id_flore_fcbn,	cd_ref::integer as cd_ref, 	nom_ent_ref as nom_complet, 	cd_ent_mere as code_taxon_mere,	null as referentiel_mere,	nom_ent_mere as nom_taxon_mere,	nom_ent_orig as nom_taxon_originel, 
	obs.rmq as remarque_taxon,	stp.libelle_valeur as libelle_statut_pop, 	lib_jdd as libelle_court_bd_mere, 	prp.libelle_valeur as libelle_usage_donnee,	lib_jdd_orig as libelle_court_bd_source, 
	cd_obs_orig as id_flore_source, 	rel.rmq as remarque_donnee_mere,	ntd.libelle_valeur as libelle_nature_date,	rel.rmq as remarque_date, 	ter.rmq as remarque_lieu,
	tso.libelle_valeur as libelle_type_source,	null as type_doc,	cd_biblio as cote_biblio_cbn,	lib_biblio as titre_doc,	null as annee_doc,	null as auteur_doc,	null as ref_doc,
	null as code_herbarium,	null as code_index_herbariorum,	null as nom_herbarium,	cd_herbier as code_herbier,	lib_herbier as nom_herbier,	null as part_herbier,	null as id_part,
	null as cote_biblio_bd_mere,	date_debut::date as date_debut_obs,	date_fin::date as date_fin_obs,	null as date_transmission,	cd_ent_mere as id_flore_mere,cd_geo, cfg.libelle_valeur, mog.libelle_valeur,m10.geom,
	statut_pop,	null as nom_observateur,	null as prenom_observateur,	'à mettre à jour' as libelle_organisme,	'à mettre à jour' as observateur
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
	WHERE typ_geo = 'm10'
	AND obs.cd_jdd <> 'MAS02'
	AND cd_validite = 1;

--- Les maille5
TRUNCATE exploitation.obs_maille_fr5;
INSERT INTO exploitation.obs_maille_fr5
SELECT	obs.cd_jdd||'_'||cd_obs_mere as id_flore_fcbn,	cd_ref::integer as cd_ref, 	nom_ent_ref as nom_complet, 	cd_ent_mere as code_taxon_mere,	null as referentiel_mere,	nom_ent_mere as nom_taxon_mere,	nom_ent_orig as nom_taxon_originel, 
	obs.rmq as remarque_taxon,	stp.libelle_valeur as libelle_statut_pop, 	lib_jdd as libelle_court_bd_mere, 	prp.libelle_valeur as libelle_usage_donnee,	lib_jdd_orig as libelle_court_bd_source, 	cd_obs_orig as id_flore_source, 	rel.rmq as remarque_donnee_mere,
	ntd.libelle_valeur as libelle_nature_date,	rel.rmq as remarque_date, 	ter.rmq as remarque_lieu,	tso.libelle_valeur as libelle_type_source,	null as type_doc,	cd_biblio as cote_biblio_cbn,	lib_biblio as titre_doc,
	null as annee_doc,	null as auteur_doc,	null as ref_doc,	null as code_herbarium,	null as code_index_herbariorum,	null as nom_herbarium,	cd_herbier as code_herbier,	lib_herbier as nom_herbier,
	null as part_herbier,	null as id_part,	null as cote_biblio_bd_mere,	date_debut::date as date_debut_obs,	date_fin::date as date_fin_obs,	null as date_transmission,	cd_ent_mere as id_flore_mere,cd_geo, cfg.libelle_valeur, mog.libelle_valeur,m5.geom,
	statut_pop,	null as nom_observateur,	null as prenom_observateur,	'à mettre à jour' as libelle_organisme,	'à mettre à jour' as observateur	
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
	WHERE typ_geo = 'm5'
	AND obs.cd_jdd <> 'MAS02'
	AND cd_validite = 1;

	
/*NB : il faut mettre à jour les observateurs*/
	--string_agg(lib_orgm,',') as libelle_organisme,
	--string_agg(nom_acteur,',') as observateur
	-- JOIN hub.releve_acteur act ON rel.cd_jdd = act.cd_jdd AND rel.cd_releve = act.cd_releve
out.lib_log := 'transfert OK';out.lib_schema := 'hub';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_data_global_refresh';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;
--PERFORM hub_log ('public', out); 
RETURN next out;
END; $BODY$ LANGUAGE plpgsql;

-------------------------------------------------------------
-------------------------------------------------------------
--------------------------------
--- Fonction siflore_data_query 
--- Description : Construit les requêtes pour la mise à jour des données
--------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
CREATE OR REPLACE FUNCTION siflore_data_query(partie varchar,cd_jdd varchar) RETURNS setof zz_log AS 
$BODY$ 
DECLARE out zz_log%rowtype;
DECLARE champ_geo varchar;
DECLARE jointure_geo varchar;
DECLARE tabl varchar;
DECLARE typ_geo varchar;
DECLARE cmd varchar;
DECLARE flag integer;
BEGIN 
-- commande
CASE WHEN partie = 'commune' THEN
	champ_geo = 'cfg.libelle_valeur, mog.libelle_valeur,cd_geo, lib_geo, com.geom,';
	jointure_geo = 'JOIN ref.geo_commune com ON ter.cd_geo = com.insee_comm';
	tabl = 'obs_commune';
	typ_geo = 'com';
	flag = 1;
WHEN partie = 'maille5' THEN
	champ_geo = 'cd_geo, cfg.libelle_valeur, mog.libelle_valeur,m5.geom,';
	jointure_geo = 'JOIN ref.geo_maille5 m5 ON ter.cd_geo = m5.cd_sig';
	tabl = 'obs_maille_fr5';
	typ_geo = 'm5';
	flag = 1;
WHEN partie = 'maille10' THEN
	champ_geo = 'cd_geo, cfg.libelle_valeur, mog.libelle_valeur,m10.geom,';
	jointure_geo = 'JOIN ref.geo_maille10 m10 ON ter.cd_geo = m10.cd_sig';
	tabl = 'obs_maille_fr10';
	typ_geo = 'm10';
	flag = 1;
WHEN partie = 'commune_mas' THEN
	champ_geo = 'cd_geo, lib_geo, com.geom,';
	jointure_geo = 'JOIN ref.geo_commune com ON ter.cd_geo = com.insee_comm';
	tabl = 'observation_taxon_reunion';
	typ_geo = 'com';
	flag = 2; /*Attention*/
ELSE	
	flag = 0;
END CASE;

CASE WHEN flag = 1 THEN
	cmd = 'DELETE FROM exploitation.'||tabl||' WHERE id_flore_fcbn LIKE '''||cd_jdd||'_%'';
	INSERT INTO exploitation.'||tabl||'
	SELECT 
	obs.cd_jdd||''_''||cd_obs_mere as id_flore_fcbn,
	cd_ref::integer as cd_ref, 
	nom_ent_ref as nom_complet, 
	cd_ent_mere as code_taxon_mere,
	null as referentiel_mere,
	nom_ent_mere as nom_taxon_mere,
	nom_ent_orig as nom_taxon_originel, 
	obs.rmq as remarque_taxon,
	stp.libelle_valeur as libelle_statut_pop, 
	lib_jdd as libelle_court_bd_mere, 
	prp.libelle_valeur as libelle_usage_donnee,
	lib_jdd_orig as libelle_court_bd_source, 
	cd_obs_orig as id_flore_source, 
	rel.rmq as remarque_donnee_mere,
	ntd.libelle_valeur as libelle_nature_date,
	rel.rmq as remarque_date, 
	ter.rmq as remarque_lieu,
	tso.libelle_valeur as libelle_type_source,
	null as type_doc,
	cd_biblio as cote_biblio_cbn,
	lib_biblio as titre_doc,
	null as annee_doc,
	null as auteur_doc,
	null as ref_doc,
	null as code_herbarium,
	null as code_index_herbariorum,
	null as nom_herbarium,
	cd_herbier as code_herbier,
	lib_herbier as nom_herbier,
	null as part_herbier,
	null as id_part,
	null as cote_biblio_bd_mere,
	date_debut::date as date_debut_obs,
	date_fin::date as date_fin_obs,
	null as date_transmission,
	cd_ent_mere as id_flore_mere,
	'||champ_geo||'
	statut_pop,
	null as nom_observateur,
	null as prenom_observateur,
	--string_agg(lib_orgm,'','') as libelle_organisme,
	--string_agg(nom_acteur,'','') as observateur
	''à mettre à jour'' as libelle_organisme,
	''à mettre à jour'' as observateur
	FROM hub.observation as obs
	JOIN hub.releve rel ON rel.cd_jdd = obs.cd_jdd AND rel.cd_releve = obs.cd_releve
	JOIN hub.releve_territoire ter ON rel.cd_jdd = ter.cd_jdd AND rel.cd_releve = ter.cd_releve
	-- JOIN hub.releve_acteur act ON rel.cd_jdd = act.cd_jdd AND rel.cd_releve = act.cd_releve
	JOIN hub.metadonnees meta ON meta.cd_jdd = obs.cd_jdd
	'||jointure_geo||'
	JOIN ref.voca_ctrl stp ON stp.cd_champ = ''statut_pop'' AND stp.code_valeur = statut_pop::varchar
	JOIN ref.voca_ctrl prp ON prp.cd_champ = ''propriete_obs'' AND prp.code_valeur = propriete_obs
	JOIN ref.voca_ctrl ntd ON ntd.cd_champ = ''nature_date'' AND ntd.code_valeur = nature_date
	JOIN ref.voca_ctrl tso ON tso.cd_champ = ''typ_source'' AND tso.code_valeur = typ_source
	JOIN ref.voca_ctrl cfg ON cfg.cd_champ = ''confiance_geo'' AND cfg.code_valeur = confiance_geo
	JOIN ref.voca_ctrl mog ON mog.cd_champ = ''moyen_geo'' AND mog.code_valeur = moyen_geo
	WHERE typ_geo = '''||typ_geo||'''
	-- AND typ_acteur = ''obs'' 
	AND meta.cd_jdd = '''||cd_jdd||'''
	AND cd_validite = 1
	;';
	EXECUTE cmd;
	out.lib_log = partie||' - '||cd_jdd||' transféré';
	out.lib_log = cmd;
WHEN flag = 2 THEN
	cmd = 'DELETE FROM observation_reunion.'||tabl||' WHERE id_flore_fcbn LIKE '''||cd_jdd||'_%'';
	INSERT INTO observation_reunion.'||tabl||'
	SELECT 
	obs.cd_jdd||''_''||cd_obs_mere as id_flore_fcbn,
	cd_ref as code_taxon, 
	nom_ent_ref as nom_taxon, 
	cd_ent_mere as code_taxon_mere,
	null as referentiel_mere,
	nom_ent_mere as nom_taxon_mere,
	nom_ent_orig as nom_taxon_originel, 
	obs.rmq as remarque_taxon,
	stp.libelle_valeur as libelle_statut_pop, 
	lib_jdd as libelle_court_bd_mere, 
	prp.libelle_valeur as libelle_usage_donnee,
	lib_jdd_orig as libelle_court_bd_source, 
	cd_obs_orig as id_flore_source, 
	null as sup_donnee, 
	null as remarque_donnee_mere,
	ntd.libelle_valeur as libelle_nature_date,
	ter.rmq as remarque_date, 
	cd_refgeo as syst_ref_spatial, 
	nature_geo as nature_objet_geo, 
	null as remarque_lieu,
	tso.libelle_valeur as type_source,
	tso.libelle_valeur as type_source,
	cd_biblio as libelle_type_source,
	lib_biblio as type_doc,
	null as cote_biblio_cbn,
	null as titre_doc,
	null as annee_doc,
	null as auteur_doc,
	null as ref_doc,
	null as code_herbarium,
	null as code_index_herbariorum,
	null as nom_herbarium,
	cd_herbier as code_herbier,
	lib_herbier as nom_herbier,
	null as part_herbier,
	null as id_part,
	null as cote_biblio_bd_mere,
	date_debut as date_debut_obs,
	date_fin as date_fin_obs,
	null as date_transmission,
	cd_ent_mere as id_flore_mere,
	'||champ_geo||'
	statut_pop,
	null as nom_observateur,
	null as prenom_observateur,
	string_agg(lib_orgm,'','') as libelle_organisme,
	string_agg(nom_acteur,'','') as observateur
	FROM hub.observation as obs
	JOIN hub.releve rel ON rel.cd_jdd = obs.cd_jdd AND rel.cd_releve = obs.cd_releve
	JOIN hub.releve_territoire ter ON rel.cd_jdd = ter.cd_jdd AND rel.cd_releve = ter.cd_releve
	JOIN hub.releve_acteur act ON rel.cd_jdd = act.cd_jdd AND rel.cd_releve = act.cd_releve
	JOIN hub.metadonnees meta ON meta.cd_jdd = obs.cd_jdd
	'||jointure_geo||'
	JOIN ref.voca_ctrl stp ON stp.cd_champ = ''statut_pop'' AND stp.code_valeur = statut_pop::varchar
	JOIN ref.voca_ctrl prp ON prp.cd_champ = ''propriete_obs'' AND prp.code_valeur = propriete_obs
	JOIN ref.voca_ctrl ntd ON ntd.cd_champ = ''nature_date'' AND ntd.code_valeur = nature_date
	JOIN ref.voca_ctrl tso ON tso.cd_champ = ''typ_source'' AND tso.code_valeur = typ_source
	JOIN ref.voca_ctrl cfg ON cfg.cd_champ = ''confiance_geo'' AND cfg.code_valeur = confiance_geo
	JOIN ref.voca_ctrl mog ON mog.cd_champ = ''moyen_geo'' AND mog.code_valeur = moyen_geo
	WHERE typ_geo = '''||typ_geo||'''
	AND typ_acteur = ''obs'' 
	AND meta.cd_jdd = '''||cd_jdd||'''
	AND cd_validite = 1;';	
	--EXECUTE cmd;
	--out.lib_log = partie||' - '||cd_jdd||' transféré';
	--out.lib_log = cmd;
ELSE
	out.lib_log = 'mauvais champ partie';
END CASE;
out.lib_schema := 'hub';out.lib_table := '-';out.lib_champ := '-';out.typ_log := 'siflore_push';out.nb_occurence := 1;SELECT CURRENT_TIMESTAMP INTO out.date_log;
--PERFORM hub_log ('public', out); 
RETURN next out;
END; $BODY$ LANGUAGE plpgsql;