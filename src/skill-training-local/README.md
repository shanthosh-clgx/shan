

# Setting up a local Airflow instance

## Prerequisites
Ensure that you have installed Docker Desktop on Windows, and in settings -> general, choose "use WSL2 based engine". You can confirm the successful installation of Docker by running `docker ps` in the WSL terminal.

## Setup Instructions
Navigate to the `./skill-training-local` directory and ensure that you have both `Dockerfile` and `docker-compose.yaml`:

```bash
$ cd ./src/skill-training-local
$ sudo chmod +x entrypoint.sh
$ mkdir -p ./cred ./working
$ echo -e "AIRFLOW_UID=50000" > .env
$ echo -e "service account secret to be filled in later" > ./cred/clgx-gcp.json
```

Then execute the following commands under `./skill-training-local` to start the local Airflow instance:

```bash
$ docker compose build (this generally takes 10 - 15 mins for the first time)
$ docker compose up -d
```

The first step could take 10 minutes the very first time you run it because Docker is building a customized image that includes installing the Python libraries. The gcloud SDK packages are large. Run `watch -n 5 docker ps` to check if all Airflow services are running healthily. An example of a successful start looks like the following:

```bash
CONTAINER ID   IMAGE                                   COMMAND                  CREATED        STATUS                  PORTS                    NAMES
174924a4a081   skill-training-local-airflow-scheduler   "./entrypoint.sh sch…"   18 hours ago   Up 18 hours (healthy)   8080/tcp                 skill-training-local-airflow-scheduler-1
e131a1e5983c   skill-training-local-airflow-worker      "./entrypoint.sh cel…"   18 hours ago   Up 18 hours (healthy)   8080/tcp                 skill-training-local-airflow-worker-1
3cae1243633f   skill-training-local-airflow-webserver   "./entrypoint.sh web…"   18 hours ago   Up 18 hours (healthy)   0.0.0.0:8080->8080/tcp   skill-training-local-airflow-webserver-1
603be2c62725   postgres:13                             "docker-entrypoint.s…"   18 hours ago   Up 18 hours (healthy)   5432/tcp                 skill-training-local-postgres-1
0d4d6fb056ef   redis:latest                            "docker-entrypoint.s…"   18 hours ago   Up 18 hours (healthy)   6379/tcp                 skill-training-local-redis-1
```

`ctrl-c` to stop the watching process.

Now, you should be able to access the local Airflow instance from http://localhost:8080/ on your Windows/Mac host(usename airflow and password airflow). Airflow Variables and Connections are configured when the Docker containers start. The details are stored in `./skill-training-local/variables.json` and `./skill-training-local/connections.yaml`.

Once you have finished your development or testing, ensure that you run `docker compose stop` so that the containers are stopped. If you forget this step, then next text you restart WSL, the containers will also start, which may not what you want. Obviously you can always stop the containers then. 

To develop airflow DAGs, please refer to the simple example DAG src/dags/demo_test_bash_dag.py 
