PROJECT_NAME='ashraf-magic'

# Check if the network exists; if not, create it
if ! docker network inspect ${PROJECT_NAME}-network &>/dev/null; then
    docker network create ${PROJECT_NAME}-network
else
    echo "Network ${PROJECT_NAME}-network already exists."
fi

# Start Kafka Containers
start-kafka(){
    docker-compose -f docker/kafka/docker-compose.yml up -d
}


# Start Mage Containers
start-mage(){
    docker-compose -f docker/mage/docker-compose.yml up -d
}

#Start producing stream data
stream-data(){
    docker-compose -f docker/streaming/docker-compose.yml up 
}
gitting(){
    git add .
    git commit -m "Update from local"
    sleep 2
    git push -u origin main
}

