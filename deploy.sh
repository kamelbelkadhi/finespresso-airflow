#!/bin/bash

# Exit script on any error
set -e

# Set variables
PROJECT_ID=$(gcloud config get-value project)
REGION=${REGION:-europe-north1}
SERVICE_NAME=airflow-webserver
IMAGE_NAME=airflow-image
CLOUD_SQL_INSTANCE=airflow-sql-instance
DB_NAME=airflow
DB_USER=airflow
SERVICE_ACCOUNT_NAME=airflow-sa
SERVICE_ACCOUNT_EMAIL=$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com
BUCKET_NAME=${PROJECT_ID}-airflow-dags
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin

# Set or load the database password
DB_PASSWORD=${DB_PASSWORD:-$(openssl rand -hex 16)}
ROOT_DB_PASSWORD=${ROOT_DB_PASSWORD:-$(openssl rand -hex 16)}

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null
then
    echo "Google Cloud SDK is not installed. Please install it first."
    exit 1
fi

# Enable required services
echo "Enabling required Google Cloud services..."
gcloud services enable \
    compute.googleapis.com \
    sqladmin.googleapis.com \
    cloudbuild.googleapis.com \
    run.googleapis.com

# Create service account if not exist
if ! gcloud iam service-accounts list --filter="email:$SERVICE_ACCOUNT_EMAIL" --format="value(email)" | grep "$SERVICE_ACCOUNT_EMAIL" &> /dev/null
then
    echo "Creating service account: $SERVICE_ACCOUNT_NAME"
    gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
        --display-name "Airflow Service Account"
fi

# Grant necessary roles to the service account
echo "Assigning roles to the service account..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member serviceAccount:$SERVICE_ACCOUNT_EMAIL \
    --role roles/cloudsql.client
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member serviceAccount:$SERVICE_ACCOUNT_EMAIL \
    --role roles/storage.objectViewer

# Create Cloud SQL instance if not exist
if ! gcloud sql instances list --filter="name=$CLOUD_SQL_INSTANCE" --format="value(name)" | grep "$CLOUD_SQL_INSTANCE" &> /dev/null
then
    echo "Creating Cloud SQL instance: $CLOUD_SQL_INSTANCE"
    gcloud sql instances create $CLOUD_SQL_INSTANCE \
        --database-version=POSTGRES_13 \
        --cpu=1 \
        --memory=4GiB \
        --region=$REGION
fi

echo "Setting database root password..."
gcloud sql users set-password postgres \
    --instance=$CLOUD_SQL_INSTANCE \
    --password=$ROOT_DB_PASSWORD

# Create database if not exist
if ! gcloud sql databases list --instance=$CLOUD_SQL_INSTANCE --filter="name=$DB_NAME" --format="value(name)" | grep "$DB_NAME" &> /dev/null
then
    echo "Creating database: $DB_NAME"
    gcloud sql databases create $DB_NAME --instance=$CLOUD_SQL_INSTANCE
fi

# Create database user if not exist
if ! gcloud sql users list --instance=$CLOUD_SQL_INSTANCE --filter="name=$DB_USER" --format="value(name)" | grep "$DB_USER" &> /dev/null
then
    echo "Creating database user: $DB_USER"
    gcloud sql users create $DB_USER --instance=$CLOUD_SQL_INSTANCE --password=$DB_PASSWORD
fi

# Always set the password for the database user
echo "Setting password for database user: $DB_USER"
gcloud sql users set-password $DB_USER --instance=$CLOUD_SQL_INSTANCE --password=$DB_PASSWORD

# Build the Docker image and push to GCR
echo "Building and pushing Docker image to Google Container Registry..."
gcloud builds submit --tag gcr.io/$PROJECT_ID/$IMAGE_NAME .

# Create a GCS bucket for DAGs if not exist
if ! gsutil ls -b gs://$BUCKET_NAME &> /dev/null
then
    echo "Creating GCS bucket for DAGs: $BUCKET_NAME"
    gsutil mb -l $REGION gs://$BUCKET_NAME
fi

# Grant the service account access to the GCS bucket
echo "Granting service account access to the GCS bucket..."
gsutil iam ch serviceAccount:$SERVICE_ACCOUNT_EMAIL:roles/storage.objectViewer gs://$BUCKET_NAME

# Upload DAGs to GCS bucket
echo "Uploading DAGs to GCS bucket..."
gsutil cp -r dags/* gs://$BUCKET_NAME/dags/

# Prepare environment variables
#ENV_VARS="AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://$DB_USER:$DB_PASSWORD@/$DB_NAME?host=/cloudsql/$PROJECT_ID:$REGION:$CLOUD_SQL_INSTANCE"
ENV_VARS="AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://$DB_USER:$DB_PASSWORD@/$DB_NAME?host=/cloudsql/$PROJECT_ID:$REGION:$CLOUD_SQL_INSTANCE"
ENV_VARS+=",AIRFLOW__CORE__LOAD_EXAMPLES=false"
ENV_VARS+=",AIRFLOW__CORE__FERNET_KEY=$(openssl rand -base64 32)"
ENV_VARS+=",AIRFLOW__WEBSERVER__SECRET_KEY=$(openssl rand -base64 32)"
ENV_VARS+=",GOOGLE_CLOUD_PROJECT=$PROJECT_ID"
ENV_VARS+=",AIRFLOW__CORE__EXECUTOR=LocalExecutor"
ENV_VARS+=",AIRFLOW__CORE__DEFAULT_TIMEZONE=UTC"
ENV_VARS+=",AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True"
ENV_VARS+=",AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=360"

# Deploy Airflow webserver to Cloud Run
echo "Deploying Airflow webserver to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
    --image gcr.io/$PROJECT_ID/$IMAGE_NAME \
    --platform managed \
    --region $REGION \
    --service-account $SERVICE_ACCOUNT_EMAIL \
    --add-cloudsql-instances $PROJECT_ID:$REGION:$CLOUD_SQL_INSTANCE \
    --set-env-vars "$ENV_VARS" \
    --memory 8Gi \
    --cpu 4 \
    --allow-unauthenticated \
    --min-instances 1


# Output the webserver service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format 'value(status.url)')
echo "Airflow is deployed and accessible at: $SERVICE_URL"
