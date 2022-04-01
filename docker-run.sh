#!/bin/sh
IMAGE_NAME="demopy"
CONTAINER_NAME="demopy"
COUCHBASE_HOST=""
COUCHBASE_USER="Administrator"
COUCHBASE_PASS="password"
COUCHBASE_BUCKET="sample_app"
STOP_CONTAINER=0

err_exit () {
   if [ -n "$1" ]; then
      echo "$1"
   else
      echo "Usage: $0 -n node_name [ -u user | -p password | -b bucket ]"
   fi
   exit 1
}

while getopts "n:u:p:b:c:k" opt
do
  case $opt in
    n)
      COUCHBASE_HOST=$OPTARG
      ;;
    u)
      COUCHBASE_USER=$OPTARG
      ;;
    p)
      COUCHBASE_PASS=$OPTARG
      ;;
    b)
      COUCHBASE_BUCKET=$OPTARG
      ;;
    k)
      STOP_CONTAINER=1
      ;;
    c)
      IMAGE_NAME=$OPTARG
      ;;
    \?)
      err_exit "Invalid Argument"
      ;;
  esac
done

if [ "$STOP_CONTAINER" -eq 1 ]; then
  docker stop $CONTAINER_NAME
  docker rm $CONTAINER_NAME
  exit
fi

docker run -d --name $CONTAINER_NAME \
	-p 8080:8080 \
	-e COUCHBASE_HOST=$COUCHBASE_HOST \
	-e COUCHBASE_USER=$COUCHBASE_USER \
	-e COUCHBASE_PASSWORD=$COUCHBASE_PASS \
	-e COUCHBASE_BUCKET=$COUCHBASE_BUCKET \
	$IMAGE_NAME
