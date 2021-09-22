# Twitter Representative Sample


Set these environment variables in the following form before running <br>

export TWITTER_QUERY=sharks <br>
export TWITTER_DBNAME=sharks <br>
export TWITTER_DATE='Jun 21 Aug 21'<br>
export MONGO_CLIENT=\<mongo client address\> <br>

Install from requirements.txt with:<br>
`pip install -r requirements.txt`
<br><br>

`docker run -it -v /home/ubuntu/data:/data/db -d mongo mongod --auth`

or without authentications

`docker run -it -v /home/ubuntu/data:/data/db -d mongo`

or with none default port

`docker run -it -v /home/ubuntu/data:/data/db -p 27042:27017 -d mongo`


Then from daily_twitter_sample directory run:<br>
`python core.py`
