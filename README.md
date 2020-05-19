# Marcellus Spider



## Crawl
```shell
# Create your environment
conda create --name=marcellus python=3.8
conda activate marcellus
pip install -r requirements.txt

# Add your email and password
export EMAIL=my.email@domain.com
export PASSWORD=1234secretSauce

# You'll need Splash running for the javascript to render
docker run -it -p 8050:8050 -rm scrapinghub/splash --name splash

# The pipeline dumps the parsed item into MongoDB
docker run -d -p 27017:27017 --network=host --name=mongo mongo

# Launch the spider. Crawling the full site will take some hours.
# Execute before bed :)
cd /path/to/project/marcellus/marcellus
scrapy crawl marcellus
```

## Explore
```shell
# The local mongo needs populated before this becomes interesting
streamlit run app.py
```
