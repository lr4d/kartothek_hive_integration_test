build_base:
	docker build -t hadoop-base -f docker/hadoop/Dockerfile docker
build_containers:
	docker-compose build
build_all: build_base build_containers
run:
	FOR_CI="False" docker-compose up -d
