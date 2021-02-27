ENV_FILE="deploys/prod.env"
COMPOSE=

case $2 in
  -env|--env-file)
    ENV_FILE=$3
esac

echo "ENV FILE - $ENV_FILE"
set -a
. $ENV_FILE
set +a

case $1 in
  rebuild)
    COMMAND="docker-compose $COMPOSE build search_api"
  ;;
  start)
    COMMAND="./run.sh stop -env $ENV_FILE; ./run.sh rebuild -env $ENV_FILE; docker-compose $COMPOSE up redis elasticsearch search_api"
  ;;
  load_es_index)
    COMMAND="curl  -XPUT http://localhost:9200/movies -H 'Content-Type: application/json' -d @schemas/es.movies.schema.json \
      && curl  -XPUT http://localhost:9200/persons -H 'Content-Type: application/json' -d @schemas/es.persons.schema.json \
      && curl  -XPUT http://localhost:9200/genres -H 'Content-Type: application/json' -d @schemas/es.genres.schema.json"
  ;;
  start_etl)
    COMMAND="docker-compose $COMPOSE up etl"
  ;;
  test)
    ENV_FILE="tests/functional/tests.env"
    COMPOSE="-f tests/functional/docker-compose.yml -p api_test"
    COMMAND="./run.sh stop -env $ENV_FILE; ./run.sh rebuild -env $ENV_FILE; docker-compose $COMPOSE up"
  ;;
  start-local)
    COMMAND="cd src; python3 main.py"
  ;;
  start-environment)
    COMMAND="./run.sh stop -env $ENV_FILE; docker-compose $COMPOSE up -d postgres redis elasticsearch etl"
  ;;
  stop)
    COMMAND="docker-compose $COMPOSE down -v --remove-orphans"
  ;;
  *)
    echo "Use 'start' command"
esac

echo $COMMAND
bash -c "$COMMAND"
