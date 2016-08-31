# Hub
hub.sql contient un script pl/pgsql permettant de construire et manipuler une hub. 
Celui-ci permet de construire, importer, vérifier, exporter des données dans une base de données Postgresql dans le Format Standard de Données du réseau.

# Installation
Pour l'utiliser, il suffit de la charger le script hub.sql dans une base Postgresql (version min. : 9.1).

#Utilisation
Une fois chargée, toutes les fonctions peuvent être appelées de la manière suivante : SELECT * FROM nom_de_la_fontion('variables');

Pour avoir plus d'information sur les fonctions utiles : SELECT * FROM hub_help(); --> donne la liste des fonctions disponibles

La table zz_log garde en mémoire les résultats des différentes fonctions (notamment les fonctions de vérification).

Plus de documentation sur l'utilisation des différentes fonctions est disponible sur http://wiki.fcbn.fr/doku.php?id=outil:hub