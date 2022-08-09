# Galadriel

Galadriel is a python based web scraper for horse racing data. It primarily focuses on the websites AmWager and Racing and Sports. It will collect the odds from AmWager as the race nears posting. This provides a history of the odds for a given horse over time, and prior to the closing of betting. This behaviour occurs for all available odds - win, exacta, quinella, etc. Static data from Racing and Sports is also collected for applicable international races. Note that Galadriel was developed as an exploratory project, and is not meant for production use. **Its use will likely result in a ban from AmWager.** It is also not maintained, and may not function with the current AmWager website.

### Installation
If you would like to try galadriel, it can be installed by first cloning the repo, navigating into it, and running `pip install .`

### Setup
Prior to running galadriel, a few tasks must be completed. 

#### 1) AmWager
First, [create an account on AmWager](https://www.amwager.com/).

#### 2) Keyring Credentials
Galadriel uses your local keyring to securely store your AmWager credentials. You must add your credentials to the keyring prior to use. This can be accomplished via the `--set_login` flag. `python -m galadriel "path/to/database.db" --set_login`. You will be prompted for your useraname and password. If you would like to add your credentials manually, create an entry titled `username` on the service `galadriel` and store your username in it. Create an entry titled `password` on the service `galadriel` and store your password in it.

#### 3) Database
The countries and tracks tables of the database are currently populated manually. A quick start database with pre-filled values has been provided in the `default_db` of this repo. Copy the database to a location of your choosing.

### Running
Galadriel will continue scraping data until the results of the final race have posted. It is recommended to create a cron job or systemd timer to run galadriel at the beginning of each day. Regardless, run the script with your preferred options. This might look like: `python -m galadriel "~/galadriel/racing.db" --log_dir "~/galadriel/logs" --max_preppers 8 --max_watchers 15 --max_memory_percent 90`


### Notes
Galadriel is multithreaded and will open a new thread and selenium instance for each race. This can use substantial amounts of memory if the number of watchers is not limited.
