#!/bin/bash
#---------Script d envoi de mail à l issu d actions sur le hub----------------------
############################
#Requete sql sur le HUB FCBN
from_mail="thomas.milon@fcbn.fr"
admin_mail="informatique@fcbn.fr"
#psql -p 5433 -c "SELECT lib_schema, action, date_log, user_log FROM public.emailing_queue GROUP BY lib_schema, action, date_log, user_log;" hub_fcbn | sed '1,2d' | sed '$d' | sed '$d' | 
psql -p 5433 -c "SELECT lib_schema, action, date_log, user_log FROM public.emailing_queue GROUP BY lib_schema, action, date_log, user_log;" si_flore_national | sed '1,2d' | sed '$d' | sed '$d' | 

#boucle
while read  lib_schema pipe1 action pipe3 date_log heure_log pipe4 user_log; do 
 #output  pour debug
 #echo "$lib_schema - $action - $date_log - $heure_log - $user_log"

 #constitution du message
 message="Message automatique\n"
 message=$message"Action réalisée sur le Hub FCBN : "$action"\n"
 message=$message"Schema sur laquelle se porte l'action : "$lib_schema"\n"
 message=$message"Date : "$date_log" "$heure_log"\n"

 #envoi à l utilisateur
 if [ $user_log != "postgres" ]; 
  then  echo -e $message | mail -s "[FCBN] Hub -"$action -t $user_log -a From:$from_mail
 fi

 #envoi à l'admin
 echo -e $message | mail -s "[FCBN] Hub "$lib_schema" - "$action -t $admin_mail -a From:$from_mail
done

# on vide la table queue
#psql -q -p 5433 -c "TRUNCATE public.emailing_queue;" hub_fcbn
psql -q -p 5433 -c "TRUNCATE public.emailing_queue;" si_flore_national

############################
#Requete sql sur le HUB FCBN
psql -p 5432 -c "SELECT lib_schema, action, date_log, user_log FROM public.emailing_queue GROUP BY lib_schema, action, date_log, user_log;" siflore_data_temp | sed '1,2d' | sed '$d' | sed '$d' |

#boucle
while read  lib_schema pipe1 action pipe3 date_log heure_log pipe4 user_log; do
 #output  pour debug
 #echo "$lib_schema - $action - $date_log - $heure_log - $user_log"

 #constitution du message
 message="Message automatique\n"
 message=$message"Action réalisée sur le Hub FCBN : "$action"\n"
 message=$message"Schema sur laquelle se porte l'action : "$lib_schema"\n"
 message=$message"Date : "$date_log" "$heure_log"\n"

 #envoi à l utilisateur
 if [ $user_log != "postgres" ];
  then  echo -e $message | mail -s "[FCBN] Hub -"$action -t $user_log -a From:$from_mail
 fi

 #envoi à l'admin
 echo -e $message | mail -s "[FCBN] Hub "$lib_schema" - "$action -t $admin_mail -a From:$from_mail
done

# on vide la table queue
psql -q -p 5432 -c "TRUNCATE public.emailing_queue;" siflore_data_temp

