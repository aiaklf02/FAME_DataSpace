📋 TOUTES LES COMMANDES
Étape 1: Prérequis
# Vérifier Docker
docker --version
docker-compose --version

# Vérifier Python
python --version
Étape 2: Démarrer tous les services Docker
# Démarrer les 12 services
docker-compose up -d

# Vérifier le statut
docker-compose ps
Étape 3: Charger la couche sémantique (Protégé → Fuseki)
# Option A: Script PowerShell complet
.\start_semantic.ps1

# Option B: Python directement
python semantic/fuseki_loader.py --clear
Étape 4: Exécuter le pipeline EtLT
# Pipeline complet (Bronze → Silver → Gold)
python main.py pipeline

30 x 7
Je vais maintenant mettre à jour le README avec les nouvelles modifications et créer un guide complet de toutes les commandes:

Read README.md, lines 500 to 799

Maintenant, je vais créer une mise à jour complète du README avec la nouvelle section Protégé + Fuseki et toutes les commandes:

Edited
README.md+78-6
Edited
README.md+139-1
Edited
README.md+4-4
Edited
README.md+7-1
Edited
README.md+2-0
Edited
README.md+45-2
Maintenant, vérifions les fichiers et affichons toutes les commandes:

Get-ChildItem "c:\Users\ayakh\MasterM2\Dataspace\FAME-DataSpace\semantic\" | Format-Table Name, Length, LastWriteTime
Voici le guide complet des commandes pour exécuter tout le projet :

📋 TOUTES LES COMMANDES (A à Z)
Étape 1: Prérequis
Étape 2: Démarrer tous les services Docker
Étape 3: Charger la couche sémantique (Protégé → Fuseki)
Étape 4: Exécuter le pipeline EtLT
Étape 5: Démarrer le streaming Kafka
python streaming/kafka_finance_streaming.py --stock-interval 30
Étape 6: Accéder aux interfaces
Service	URL	Identifiants
Grafana	http://localhost:3000	admin / admin123
Fuseki SPARQL	http://localhost:3030	admin / admin123
Kafka UI	http://localhost:8080	-
Spark Master	http://localhost:8081	-
Prometheus	http://localhost:9090	-
Étape 7: Tester les requêtes SPARQL
python semantic/fuseki_service.py

# Requête SPARQL via curl
curl -X POST "http://localhost:3030/fame/query" -H "Accept: application/json" -d "query=SELECT * WHERE { ?s ?p ?o } LIMIT 10"
Étape 8: Arrêter les services
# Arrêter (garder les données)
docker-compose stop

# Arrêter et supprimer conteneurs
docker-compose down

# Tout supprimer (y compris volumes)
docker-compose down -v