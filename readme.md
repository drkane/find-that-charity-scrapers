# find-that-charity-scrapers

A collection of scrapers used to add data to <https://findthatcharity.uk/>. The scrapers use [scrapy](https://scrapy.org/) and can currently save data to either an
elasticsearch index or mongodb database.

The standard format is based on the `Organization` object described in the
[threesixtygiving data standard](http://standard.threesixtygiving.org/en/latest/_static/docson/index.html#../360-giving-schema.json$$expand).

## Spiders

Each spider (a particular part of the scraper) looks at a particular type of organisation (in the UK), and aims to transform
a register of that type of organisation into a standard format that can be saved
to the database powering findthatcharity.

The spiders are all designed to be run without human import and should fetch consistent data.

The spiders are found in the `/findthatcharity_import/spiders` directory and cover:

 - `casc`: Community Amateur Sports Clubs regulated by HMRC
 - `ccew`: Registered charities in England and Wales
 - `ccni`: Registered charities in Northern Ireland
 - `oscr`: Registered charities in Scotland
 - `companies`: Companies registered with Companies House (the scraper only imports non-profit company types)
 - `mutuals`: Mutual societies registered with the Financial Conduct Authority
 - `gor`: A register of government organisations
 - `grid`: Entries from the [Global Research Identifier Database](https://www.grid.ac/) - only those that are based in the UK and are not a registered company are included.
 - `hesa`: Organisations covered by the Higher Education Statistics Agency.
 - `lae`: Register of local authorities in England
 - `lan`: Register of local authorities in Northern Ireland
 - `las`: Register of local authorities in Scotland
 - `pla`: Register of principal local authorities in Wales
 - `nhsods`: NHS organisations
 - `rsl`: Registered social landlords
 - `schools_gias`: Schools in England (also includes Universities)
 - `schools_ni`: Schools in Northern Ireland
 - `schools_scotland`: Schools in Scotland
 - `schools_wales`: Schools in Wales

### Running a scraper

If you have [scrapy](https://scrapy.org/) installed then you can run an individual scraper using:

```bash
scrapy crawl <spiderid>
```

For example to crawl charities in England and Wales you would run:

```bash
scrapy crawl ccew
```

## Pipelines

The code comes with two specialist pipelines to add the data to a database, plus one to add postcode data.

Pipelines come with their own settings ([see scrapy docs for how to use these](https://docs.scrapy.org/en/latest/topics/settings.html#populating-the-settings)) 
and need to be [activated in the `ITEM_PIPELINES` setting](https://docs.scrapy.org/en/latest/topics/item-pipeline.html#activating-an-item-pipeline-component).

### Add postcode data

The pipeline found in `pipelines/postcode_lookup_pipeline.py` uses <https://postcodes.findthatcharity.uk/> to lookup data about an organisation's postcode and add the data to the organisation's `location` attribute.

To activate this pipeline add 

```
'findthatcharity_import.pipelines.postcode_lookup_pipeline.PostcodeLookupPipeline': 100,
```

to the `ITEM_PIPELINES` setting. It's is recommended that the value given as 100 above is set as low as possible to ensure that the postcodes are fetched before any data is saved to the database.

The following settings are available for this pipeline:

- `PC_URL`: The URL used to fetch the data for a postcode. An empty set of brackets shows where the postcode will go. (Default `https://postcodes.findthatcharity.uk/postcodes/{}.json`)
- `PC_FIELD`: The field in the `Organisation` Item that contains the postcode. (Default: `postalCode`)
- `PC_FIELDS_TO_ADD`: The area types that will be added to the item based on the postcode. NB in addition to this the lat/long is always added if present. (Default `['cty', 'laua', 'ward', 'ctry', 'rgn', 'gor', 'pcon', 'ttwa', 'lsoa11', 'msoa11']`)

### Elasticsearch pipeline

This pipeline saves data to an elasticsearch index. It is generic, so will work on any object that defines an `id` attribute, but the object returned can be customised by adding a `to_elasticsearch` method to the Item object.

The `to_elasticsearch()` method should ensure that the following attributes are set on each item that is to be saved:

- `_id` - a unique identifier for the item (mandatory)
- `_index` - the elasticsearch index to save to (will use `ES_INDEX` setting if not set)
- `_type` - the elasticsearch type to save to (will use `ES_TYPE_` setting if not set)
- `_op_type` - the operation type (default is `index`)

Every item is added to the elasticsearch index in bulk, without checking whether it already exists. This means it can overwrite data.

The following settings can be defined:

- `ES_URL`: The URL to access the elasticsearch service (Default `http://localhost:9200`)
- `ES_INDEX`: The elasticsearch index that data will be written to (Default `charitysearch`)
- `ES_TYPE`: The elasticsearch type that will be given to the organisation (Default `organisation`)
- `ES_BULK_LIMIT`: The chunk size used for sending data to elasticsearch (Default `500`)

### MongoDB pipeline

This pipeline is very similar to the elasticsearch one, but instead saves data to a MongoDB instance. It saves records in bulk, and will overwrite any existing records with the same ID.

Defining a `to_mongodb` method on an Item will allow you to customise what is saved to the database - this method should return a tuple with the name of the collection it should be saved to and then the item itself.

The following settings are defined:

- `MONGO_URI`: The URI to access the mongoDB instance (Default `mongodb://localhost:27017`)
- `MONGO_DB`: The name of the MongoDB database (Default `charitysearch`)
- `MONGO_COLLECTION`: The default name of the MongoDB collection (only used if not returned by `item.to_mongodb()`) (Default `organisation`)
- `MONGO_BULK_LIMIT`: The chunk size used for sending data to mongoDB (Default `50000`)

## Deploying with Scrapyd

A dockerfile has been included ([source from here](https://github.com/cdrx/scrapyd-authenticated))
which enables use as a dokku or other hosting service server.

To deploy to Dokku use the following configuration:

### 1. Set up dokku server

SSH into server and run:

```bash
# create app
dokku apps:create findthatcharity_scrape

# set up so no checks are run on deploy
dokku config:set --no-restart findthatcharity_scrape DOKKU_DEFAULT_CHECKS_WAIT=0
```

### 2. Add as a git remote and push

On local machine:

```bash
git remote add dokku dokku@SERVER_HOST:findthatcharity_scrape
git push dokku master
```

## Other settings

By default, the `HTTPCACHE` extension is enabled, with resources cached for one week.
This means that any data downloaded or websites visited are cached for one week to prevent
overload of the sites. This means it is relatively risk-free to rerun scraping after 
adjusting other settings for e.g. saving to a database. These settings can be changed
if needed.

The scrapers are also set by default to ignore robots.txt used on sites - this can be changed.
