build_base:
	docker build -t hadoop-base -f docker/hadoop/Dockerfile docker
build_containers:
	docker-compose build
build_all: build_base build_containers
run:
	RUN_ON_TRAVIS="False" docker-compose up -d
