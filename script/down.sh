#!/usr/bin/env bash

case "$1" in
  1|Celery|MultiNode)
	echo "1, Stop MultiNode"	
	docker-compose -f docker-compose-Celery.yml down
	;;
  
  2|Tutor|Seq|Sequential)
	echo "2, Stop Tutorial"
	docker rm  -f air_webserver_2
	;;
	
  3|Example)
	echo "3, Stop Example"
	docker rm  -f air_webserver_3
	;;

  4|2.3)
	echo "3, Stop Example"
	docker-compose -f docker-compose-2.3.yml down
	;;	

  *)
  # exec "$@"
	echo "0, Stop SingleNode"
	docker-compose -f docker-compose.yml down
	;;
esac  