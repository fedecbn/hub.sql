#!/bin/bash
#---------Script d envoi de mail à l issu d actions sur le hub----------------------
############################
#Requete sql sur le HUB FCBN
psql -p 5433 -c "SELECT lib_schema, jdd, version FROM public.publicating_queue GROUP BY lib_schema, jdd, version;" si_flore_national | sed '1,2d' | sed '$d' | sed '$d' | 

#boucle
while read  lib_schema pipe1 jdd pipe2 version; do 
 #output  pour debug
 # echo "$lib_schema - $jdd - $version"

 #migration des données
 psql -q -p 5432 -c "SELECT * FROM siflore_data_refresh('$lib_schema','$jdd',$version);"  si_flore_national_v4

 #agregation des données
 psql -q -p 5433 -c "SELECT * FROM hub_aggregate('$lib_schema','data');"  si_flore_national
done

#mise à jour des synthèses et réf (liste taxon)
psql -q -p 5432 -c "SELECT * FROM siflore_synthese_refresh();"  si_flore_national_v4

# on vide la table queue
psql -q -p 5433 -c "TRUNCATE public.publicating_queue;" si_flore_national

