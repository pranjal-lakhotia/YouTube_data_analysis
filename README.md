# YouTube_data_analysis

Here are the things that i have taken in this Project :-
1. Taken two datasets of YouTube Sample Data from Kaggle.One Conatins trending videos info and other contains detailed info for a video viz. likes, dislikes, channelid, channelname etc...
   Dataset recieved contained info about 6 different countries.
2. Uploaded Json files in GCS [ Google Cloud Storage ]
3. Created a Google Cloud function to cleanse Json files and push it in a seperate Folder in GCS.
4. Created a Trigger to automatically Cleanse files on every upload in bucket.
5. Created a Spark Script on dataproc to Join different datsets and to push transformed data onto bigquery.
6. Validated final reporting table with Raw Data to ensure that transfomed data is inline with raw data.

   ![image](https://github.com/pranjal-lakhotia/YouTube_data_analysis/assets/50244913/eb74d7c9-8f7d-4a9c-8fd9-8cd1896ba409)

