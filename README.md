[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=12482770&assignment_repo_type=AssignmentRepo)
# CSCI 622 Project

The topic for this data engineering project is IoT Malware Detection. IoT stands for Internet of things which has been a hot topic in the industry and will continuing becoming more popular as it will be used on our daily life such as smart home devices, and industrial such as a smart power grid. However, the security of IoT devices is not giving enough attention to other Internet devices such as cell phones or computers. Attacking IoT devices can cause serious damage such as personal privacy violations or even national security. This project will provide a basic pipeline and analysis on some open-source dataset with the help of Azure cloud services.

## Ingestion
### Data Source

There are two dataset used in this project. The first one is the Malware on IoT dataset which belongs to Aposemat project funded by Avast. The data is provided by a sister project with this one called Malware Capture Facility Project. This project is responsible for making the long-term malware captures. They continually obtained malware and normal data to feed the Stratosphere IPS. The other dataset is called Edge-IIoTset which is a new comprehensive realistic cyber security dataset of IoT and IIoT applications on Kaggel. More details about the datasets can be found in [Summary description.](SupplementaryInfo/IngestionAnalysis)

### Ingestion Method

The datasets are ingested to Azure storage account usnig Python script. Both dataset are being processed so that can be directly read as a dataframe. The Edge-IIoTset dataset is on Kaggel, so the Kaggel API can be used to download this dataset. The Malware on IoT dataset does not provide an API but is can be downloaded using wget package in Python directly from the hosted website, the dataset can also be refreshed by running teh Python script.

### Data Storage

Both dataset are ingested and stored on the Azure storage account that has been created for this project. And more processed files will also be stored in this container under the same storage account.
