# DTC Project: brooklyn food waste
This repo contains my code for DataTalkClub's DE Zoomcamp Project (project assignment [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_7_project)). 

A huge thanks to DataTalks Club Team for creating this great [DE Zoompcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)!!

# Table of Contents
1. [Goal](#goal)
2. [Dataset](#dataset)
3. [Architecture](#architecture)
4. [Dashboard](#dashboard)
5. [Recreating the project](#recreating-the-project)
6. [Future Development](#future-development)

# Goal
This project aims to create a data pipeline that processes data that will help answer the following questions:
* Which food products are most wasted in Brooklyn?
* Which kind of food is wasted the most? (ready-to-eat, perishable, packaged or shelf stable)

# Dataset
The dataset can be found [here](https://www.kaggle.com/datasets/ursulakaczmarek/brooklyn-food-waste), it contains data used to research the relationship between food waste and the date labels found on those wasted food items. The data was collected by picking up items directly from retailer trash piles at random in the Downtown Brooklyn neighborhood in New York City.

# Architecture
Techonologies used for the project:
* Cloud: GCP
* Infrastructure as code (IaC): Terraform
* Workflow orchestration: Airflow
* Data Wareshouse: BigQuery
* Batch processing: Spark
* Containerization: Docker

![image](https://github.com/ritaafranco/dtc-project-brooklyn-food-waste/blob/main/99_files/Architecture.png)

# Dashboard
The dashboard can be found [here](https://datastudio.google.com/reporting/c01d8ee8-3423-465d-b8dc-c3ed280cb9d2). In case the data is not showing it means the free credits in my Google Cloud account are over, then please consult image bellow.
This dashboard allows some conclusions on the data such as:
* The most wasted food product are yogurts.
* The average price of wasted products is approximatly $5.57.
* The majority of produtcs is collected on expiration date day, however some are wasted before that date.
* Around 60% of wasted food products are perishable.

![image](https://github.com/ritaafranco/dtc-project-brooklyn-food-waste/blob/main/99_files/Dashboard.png)

# Recreating the project
For recreating this project please follow the instructions bellow.

## Kaggle Credentials
The dataset used for this project is from Kaggle, so in order to download it we need to use Kaggle API and pass env variables to docker.

1. Create a Kaggle account [here](https://www.kaggle.com/account/login?phase=startRegisterTab&returnUrl=%2Fritamafranco%2Faccount).
2. Log in to your account, navigate to account settings and create a **new API Token**:
![image](https://github.com/ritaafranco/dtc-project-brooklyn-food-waste/blob/main/99_files/Kaggle%20API.png)
3. Copy the username and key to the [Docker Compose](https://github.com/ritaafranco/dtc-project-brooklyn-food-waste/blob/main/02_airflow/docker-compose.yaml) file (lines 72 and 73) and uncomment them.
```
  #KAGGLE_USERNAME: <paste here>
  #KAGGLE_KEY: <paste here>
```
Note: a better way to do this is being investigated, for some reason the API was not being able to read the file when the file was copied to the container to the location `~/.kaggle/kaggle.json`.

## Google Cloud Account
For this project a free GCP account is all you need.

1. Create an account with your Google email ID 
2. Setup your first [project](https://console.cloud.google.com/) if you haven't already
    * eg. "dtc-project-ritaafranco"
3. Copy the project id to the [Docker Compose](https://github.com/ritaafranco/dtc-project-brooklyn-food-waste/blob/main/02_airflow/docker-compose.yaml) file (lines 70).
```
GCP_PROJECT_ID: '<your-gcp-project-id>'
```
3. Setup [service account & authentication](https://cloud.google.com/docs/authentication/getting-started) for this project
    * Grant `Viewer` role to begin with.
    * Download service-account-keys (.json) for auth. And save it under `~/.google/credentials/google_credentials.json`
4. [IAM Roles](https://cloud.google.com/storage/docs/access-control/iam-roles) for Service account:
   * Go to the *IAM* section of *IAM & Admin* https://console.cloud.google.com/iam-admin/iam
   * Click the *Edit principal* icon for your service account.
   * Add these roles in addition to *Viewer* : **Storage Admin** + **Storage Object Admin** + **BigQuery Admin**
   
5. Enable these APIs for your project:
   * https://console.cloud.google.com/apis/library/iam.googleapis.com
   * https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
   * https://console.cloud.google.com/apis/library/datastudio.googleapis.com
   
## Terraform
Terraform is being used to create the insfrastructure inside GCP project. For that run the following commands on your terminal:
1. Go to terraform folder:
```
cd ~/dtc-project-brooklyn-food-waste/01_terraform
```
2. Init terraform
```
terraform init
```
3. Run terraform plan to check if all changes are acording to plan.
```
terraform plan -var="project=<your-gcp-project-id>"
```
4. Run terraform apply to enforce the changes.
```
terraform apply -var="project=<your-gcp-project-id>"
```
Wait for the command to complete and then move on to Airflow!

After the work is done, you can use `terraform destroy` to delete the services created and avoid costs on any running services.

## Airflow
Airflow is orchestraing the whole data pipeline: data ingestion to data lake, data transformation and data storage in the data warehouse. To run Airflow you need `docker compose` and run the following commands on your terminal:
1. Navigate to airflow folder:
```
cd ~/dtc-project-brooklyn-food-waste/02_airfow
```
2. Build de image:
```
docker compose build
```
3. Initiate airflow
```
docker compose up -d
```

After the containers are up navigate to [localhost:8080](http://localhost:8080) and log in to airflow. You should be able to see 3 DAGs that are paused.
![image](https://github.com/ritaafranco/dtc-project-brooklyn-food-waste/blob/main/99_files/Airflow%20DAGs.png)
Please enable all 3. Wait for them to start, just refresh the page. **Do not trigger them manually**. DAGs will be triggered automatically once the previous one is finnished.

* `data_ingestion_to_gcs`: first DAG, fetches data from kaggle and stores it in the Data Lake (GCS bucket). Triggers `process-food-waste-data` DAG.
* `process-food-waste-data`: second DAG, processed all data using Spark, and stores it in the bucket. Triggers `data_to_dw_bq` DAG.
* `data_to_dw_bq`: final DAG, fetches the processed data from the Data Lake and creates the Big Query (data warehouse) tables.

Once the pipeline is completed you can create a Data Studio report to check the data.

## Data Studio
You can now create a new report and add the recently created Big Query tables as your data source.

# Future Development
* Adjust data schema to create an optimized data model for the dashboard.
* Replace Kaggle credentials method. Currently these credentials are wrriten as environment variables in the Docker Compose file, these sould be passed in a more secure way. For example following [this alternative](https://www.kaggle.com/general/51898)
