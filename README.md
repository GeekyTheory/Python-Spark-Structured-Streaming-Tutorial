# Python Spark Structured Streaming with Twitter tutorial

## Setup the project

Configure a virtual environment:

```shell
python -m venv venv
source venv/bin/activate
```

Install the requirements:

```shell
pip install -r requirements.txt
```

## Configure the settings

Copy the `settings.py.example` file to `settings.py` and configure the variables.

```shell
cp settings.py.example settings.py
```

## Run application

First, run the Twitter streaming application:

```shell
python twitter_streaming.py
```

After that, run the Spark sentiment analysis application:

```shell
python spark_streaming_sentiment_analysis.py
```