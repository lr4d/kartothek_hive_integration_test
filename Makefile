build_base:
	docker build -t hadoop-base -f docker/hadoop/Dockerfile docker
build_containers:
	docker-compose build
build: build_base build_containers
run:
	DEBUG="True" docker-compose up -d
