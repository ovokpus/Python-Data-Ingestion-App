# Python-Data-Ingestion-App

## Design and build of a Full Stack Data Ingestion Application using Python

## Source Code Structure

This source code is used as a part of the Cloud Academy Advanced Python development course.

The code is broken into multiple modules.

`ingest/` is responsible for data ingestion, processing, and persistence.

`simultator/` is responsible for sending data to the ingestion front-end.

`web/` is responsible for serving up a web user interface.

## Running the Web Server

data_storage="firestore" blob_storage="cloudstorage" blob_storage_bucket="advanced_python_cloud_academy" GOOGLE_APPLICATION_CREDENTIALS="/vagrant/service_account.json"  gunicorn -b "0.0.0.0:8080" -w 1 "web.main:create_app()" --timeout=60

curl -XPOST <http://127.0.0.1:8080/images> -H "Authorization:8h45ty"
