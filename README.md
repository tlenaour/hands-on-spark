#Lambda architecture en pratique avec Spark

Ce projet est un hands-on d'introduction à Spark sur le cas d'utilisation suivant : aggrégation de tickets de caisse en temps réel. Le flux d'entrée provient de Kafka et les résultats aggrégés sont stockés dans Cassandra.

Le projet est divisé en trois modules : domain-models, producer et spark.

##domain-models

Ce module contient les classes métier communes aux producteurs (terminaux de paiement) et aux consommateurs (appli Spark) de ces tickets de caisse. Il contient deux classes Scala :

- `Ticket` : ticket de caisse contenant une ou plusieurs lignes produit
- `Produit`: ligne produit

##producer

Ce module contient un générateur de tickets de caisse qui produit les tickets dans Kafka. Lancer la classe `TicketsProducer` pour commencer à produire des tickets.
En ligne de commandes :

    sbt "project producer" "runMain com.octo.nad.handson.producer.TicketsProducer"

Pour ajuster le débit, entrer dans la console l'intervalle de temps désiré entre chaque émission de ticket en millisecondes. Ex :

    sbt "project producer" "runMain com.octo.nad.handson.producer.TicketsProducer"
    Current throughput : 3 msg/seconds
    Current throughput : 0 msg/seconds
    10
    Current throughput : 0 msg/seconds
    Current throughput : 59 msg/seconds
    Current throughput : 63 msg/seconds

##spark

Ce module contient deux applications Spark : batch et streaming.

###streaming

L'appli streaming consomme les tickets provenant de Kafka, les aggrège pour ensuite les stocker dans Cassandra. Le workflow est le suivant :

- lecture du flux de tickets entrant au format JSON (déjà fait)
- parsing du ticket
- filtrage du magasin numéro 56
- explosion du ticket pour émettre les lignes produit 
- calcul du chiffre d'affaire par section 
- ajout du libellé section 
- enregistrement des tuples (section, batch_id, chiffre_affaire)

Supposons que ces trois tickets entrent dans le workflow et sont tous les trois consommés par le même batch (batchId = 12345):

    T1 [(dentifrice, hygiène, 1.10), (ballon de foot, sport, 12.30)]
    T2 [(chaussette, textile, 3.00), (mousse à raser, hygiène, 2.30)]
    T3 [(t shirt, textile, 5.30)]

Alors le résultat du workflow de streaming devrait persister les tuples suivants dans la table hands_on.ponc :


    section | batch_id  | revenue
    -----------------------------
    hygiène | 12345     | 3.40
    textile | 12345     | 8.30
    sport   | 12345     | 12.30


Pour lancer l'application, lancer la classe app/StreamingApp :

    sbt "project spark" "runMain com.octo.nad.handson.spark.app.StreamingApp"



###batch

L'appli batch récupère les tuples enregistrés par l'application streaming et les aggrège de sorte à obtenir le chiffre d'affaire cumulé au cours du temps, à partir du chiffre d'affaire "ponctuel" (i.e par batch) au cours du temps. L'appli éffectue en quelque sorte l'intégrale du chiffre d'affaire. Voici son workflow :

- lecture de la table hands_on.ponc (déjà fait)
- groupement des tuples par section, puis, au sein de chaque section, tri par batchId
- pour chaque groupement, calcul de l'intégrale
- exploser les groupements par section en plusieurs tuples
- écriture dans Cassandra

Un exemple :

Supposons que les tuples suivants sont présents dans la table Cassandra hands_on.ponc :

    section | batch_id   | revenue
    -----------------------------
    hygiène | 12345     | 2.30
    hygiène | 12346     | 6.40
    hygiène | 99923     | 1.10
    sport   | 12345     | 21.00
    sport   | 12346     | 12.05

Alors le batch devra produire la table Cassandra hands_on.cumul suivante :

    section | batch_id   | revenue
    -----------------------------
    hygiène | 12345     | 2.30
    hygiène | 12346     | 8.70
    hygiène | 99923     | 9.80
    sport   | 12345     | 21.00
    sport   | 12346     | 33.05


Pour lancer l'application, lancer la classe app/BatchApp :

    sbt "project spark" "runMain com.octo.nad.handson.spark.app.BatchApp"

##exercice (serious things)

Le but du hands-on est de coder le pipeline de chaque application : batch et streaming. Tout l'enrobage (conf, contextes spark, tests) ont été écrits de sorte à ce que le hands-on ne consiste qu'en l'implémentation des règles métiers citées plus haut. Les classes à modifier sont `StreamingPipeline` et `BatchPipeline`. Avant toute modification, s'assurer que les tests sont au rouge :

    sbt test

Une fois que les pipelines auront été correctement écrits, les tests devront être au vert.

##et après ?

Amusez vous à produire des messages avec le producer, consommez les avec l'appli streaming. Enfin, consolidez les données avec le batch !

Have fun !