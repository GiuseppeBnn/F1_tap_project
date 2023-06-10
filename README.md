# F1_tap_project
Progetto per il corso di Technology for Advanced Programming (TAP) dell'università di Catania. 

## Descrizione
Il progetto consiste nella creazione di un sistema di elaborazione dati live del server ufficiale Formula 1 e la visualizzazione dei dati elaborati in una dashboard. Il sistema è composto da 9 container: 
- 1 container python per la ricezione dei dati dal server ufficiale Formula 1
- 1 container Logstash per il filtro e la suddivisione dei dati in 3 pipeline
- 1 container broker Kafka per la gestione e l'accodamento dei dati in ingresso
- 1 container PySpark driver per l'elaborazione dei dati e la previsione dei giri futuri
- 3 container Spark che formano un cluster per l'elaborazione dei dati
- 1 container Elasticsearch per la memorizzazione dei dati
- 1 container Kibana per la visualizzazione dei dati


## Avvio
Per avviare il progetto è necessario avere installato Docker e Docker compose.
Clona il repository e posizionati nella cartella del progetto, le cartelle sono due: una contiene una demo con un replay-script e l'altra contiene il connettore live.

### Demo
Per avviare la demo posizionati nella cartella "f1-live-replay" ed esegui il comando:

```docker compose build```
e successivamente
```docker compose up```

Nota: la demo è stata testata su macchina E2 standard (8 vCPU, 32 GB RAM) di Google Cloud Platform. Riadattare eventualmente i parametri di configurazione nel docker-compose.yml e nel Dockerfile (spark-submit) per eseguire in locale.

### Live
Per avviare il progetto live posizionati nella cartella "f1-live" ed esegui il comando:

```docker compose build```
e successivamente
```docker compose up```


## Dashboard
In entrambi i casi, per visualizzare la dashboard, apri il browser e vai all'indirizzo: http://localhost:5601  (o l'indirizzo del server se eseguito in cloud e porta 5601)



BUONA  ANALISI! :D
