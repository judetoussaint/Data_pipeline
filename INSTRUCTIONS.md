First-Step: Install docker, then go to the folder directory  and run below
docker-compose -f .\docker-compose-LocalExecutor.yml up -d

Second-Step: On your webbrowser type localhost:8080 and turn the dag on. Trigger dag if necessary

Third-STep: Install pyarrow in the docker container by running below commands
docker exec -it docker-airflow-master-scale_webserver_1 /bin/bash
pip install pyarrow
