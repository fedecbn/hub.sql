# Hub
Hub contient un script pl/pgsql permettant de gerer un hub CBN. 
Celui-ci permet de construire, importer, vérifier, exporter des données dans une base de données Postgresql dans le Format Standard de Données du réseau.

# Installation
Pour l'utiliser, il suffit de la charger le script hub.sql dans une base Postgresql (version min. : 9.1).

#Utilisation
Une fois chargée, toutes les fonctions peuvent être appelées de la manière suivante : SELECT * FROM nom_de_la_fontion('variables');

Pour avoir plus d'information sur les fonctions utiles :
- SELECT * FROM hub_help(); --> donne la liste des fonctions disponibles
- SELECT * FROM hub_help('nom_de_la_fonction'); --> donne la derscription de la fonction en question.

La table zz_log garde en mémoire les résultats des différentes fonctions (notamment les fonctions de vérification).

#Channel de discussion :
[![Join the chat at https://gitter.im/TomMilon/hub](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/TomMilon/hub?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
