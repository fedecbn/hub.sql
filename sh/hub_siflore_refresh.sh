#!/bin/bash
#---------Script d envoi de mail à l issu d actions sur le hub----------------------
############################
#Drapeau pour savoir s'il y a des mises à jours
flag=0

#Liste des jeux de données à mettre à jour
psql -p 5433 -c "SELECT lib_schema, jdd, version FROM public.publicating_queue GROUP BY lib_schema, jdd, version;" hub_fcbn | sed '1,2d' | sed '$d' | sed '$d' |

#boucle sur la liste des jdd
while read  lib_schema pipe1 jdd pipe2 version; do
 #output  pour debug
 #echo "$lib_schema - $jdd - $version"

 #migration des données du hub fcbn vers le SI FLORE temp
 psql -q -p 5432 -c "SELECT * FROM siflore_data_refresh('$lib_schema','$jdd',$version);"  siflore_data_temp


 #agregation des données sur le hub
 psql -q -p 5433 -c "SELECT * FROM hub_aggregate('$lib_schema','data');"  hub_fcbn

 #Modification du drapeau
 flag=1
done

## Dans le cas où il y a eu des mises à jours
if [ $flag = 1 ]
then
 #mise à jour des synthèses et réf (liste taxon) sur le SI FLORE temp
 psql -q -p 5432 -c "SELECT * FROM siflore_synthese_refresh();"  siflore_data_temp

 #Switch de base de données
 psql -q -p 5432 -c "ALTER DATABASE siflore_data RENAME TO siflore_data_temp2;"
 psql -q -p 5432 -c "ALTER DATABASE siflore_data_temp RENAME TO siflore_data;"
 psql -q -p 5432 -c "ALTER DATABASE siflore_data_temp2 RENAME TO siflore_data_temp;"

 # on vide la table queue
 psql -q -p 5433 -c "TRUNCATE public.publicating_queue;" hub_fcbn

 #migration des données du hub fcbn vers le SI FLORE temp
 psql -q -p 5432 -c "SELECT * FROM siflore_data_refresh('$lib_schema','$jdd',$version);"  siflore_data_temp
fi
