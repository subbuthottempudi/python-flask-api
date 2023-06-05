
docker build . -t pyflask
docker image ls
docker logs pyflask
docker run -p 8080:8080 --name flask -d pyflask

docker build -t spark-flask .
docker run -d  --net=host -e FLASK_PORT=8081 spark-flask