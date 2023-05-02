
# What are the top 10 songs played in the top 50 longest sessions by tracks count?

## How to run application

    1) Clone this repo to your local folder
    2) Download the input data from http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html
    3) Unpack file "userid-timestamp-artid-artname-traid-traname.tsv" into the "data" folder of the repo
    4) Build an image from the "Dockerfile"
    5) In terminal run the following command "docker run -v <path-to-the-local-repo>/data:/mnt --rm -it  -p 4040:4040 exercise2", 
    6) In a few minutes the result.tsv file must emerge in "data" folder


## Things to improve

1) Add Logging
2) Ad Unit testing
3) Add Data validation (e.g. schema validation)
4) Perfomance tuning (e.g. "int" data type for column "global_session_id")
