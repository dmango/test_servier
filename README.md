
# Test technique Servier

Ce projet contient trois grandes parties : 

- La partie “Python - Data Engineering”
Contenu dans le répertoire ‘data_pipeline’. Ce répertoire contient tous les éléments nécessaires pour mettre en place un pipeline permettant de traiter les données afin de ressortir les médicaments ayant cité dans des titres d'essais cliniques et PubMed.
- La partie ad-hoc:Le but est d'extraire les données depuis un fichier JSON,le nom du journal qui mentionne le plus de médicament.
- La partie “SQL” correspondant au répertoire ‘sql’. Ce répertoire contient les scripts permettant de réaliser les requêtes qui permettent de déterminer, par client et sur une période donnée, les ventes meuble et déco réalisées ainsi que le CA par jour.
   
## Les différentes dépendances utilisées dans notre projet:

Comme librairie,nous avons utilisé Pandas afin de lire et transformer nos fichiers drugs.csv, pubmed.csv, clinical_trials.csv et pubmed.json. 
Le module UnitTest pour effectuer les tests unitaires de la partie Extract et du Transform.
Le package setup qui permet d’appeler un fichier se trouvant dans un répertoire différent que celui de l’origine du script

## Les différents répertoires: 

###  ETL:
Dans ce dossier nous avons les 3 scripts permettant de réaliser l’extraction, le nettoyage et le chargement du fichier json cible demandé sur l’exercice, qui par la suite pourra être chargé en local, sur GCP,etc.

###  Ressources:
Nous trouverons toutes les informations pouvant être utiles pour mener à bien notre projet. Nous avons le fichier pdf ou sont spécifiées toutes les informations sur le travail à faire ,les trois fichiers csv sources ,notre fichier json ,notre fichier Ad-hoc et enfin notre fichier output demandé.

###  Test:
Nous avons dans ce dossier les tests unitaires sur notre code d'extraction et de transformation appelés respectivement: test_extract et test_transform.

## Proposition d’élément pour faire évoluer le code en cas de données volumineuses:
#### Solution 1:
Utiliser Apache spark, en python (PySpark), cela permettra de lancer les pipelines en parallèle sur un cluster distribué de plusieurs nœuds (sur un seul nœud, en fonction de la volumétrie des données / complexité des opérations à effectuer).

#### Solution 2 :
Importer les données dans une base de données en premise (exp : MariaDB), ou sur une base de données managée par un cloud provider (ex : Bigquery sur gcp) et effectuer les traitement en sql, l’avantage de l’utilisation d’un cloud provider est la gestion de l'auto scaling par ce dernier.