# config.json help
---
## **api**: entries regarding the API access module.
	- country (string): Country of content being analyzed.
	- keys[] (string array): Access Keys for Youtube API.
	- threads (number): Number of API module worker threads.
	- videoDayLimit (number): Maximum age of videos to be scraped, in days.

## **files**: entries regarding file reading and writting.
* **channelListFile** *(string)*: path and filename containing the list of channel. For *.csv* filetype, must have a column labeled channel_id or channelId. For *.json* filetype, must be a list of objects containing key named channelId or channel_id. For any other filetype, must be a simple line-separated list of channel IDs.
* **csv** *(object)*: entries regarding the CSV reader functionality.
	* **delimiter** *(string)*: Symbol separating values in each row.
	* **quoteChar** *(string)*: Symbol used for string values.
- **outputDirectory** *(string)*: Path to the output directory.
- **encoding** *(string)*: Encoding used in files.
- **filter[]** *(object array)*: (Not implemented) Defines filters for values in other columns (CSV) or keys (JSON) of channel list. 
	* **attribute** *(string)*: Name of attribute (which must exist as a column/key) to check.
	* **type** *(string)*: Type of filter. Must be one of the following: greater, less, greater_equal, less_equal, equal.
	* **value** *(number)*: Numerical value applied to the type operation.
    >Example: `{"name": "subscribers", "type": "greater", "value": 10000}` will only consider channel IDs with **subscribers** attribute **greater** than **10000**, ignoring everything else (unless specified by another filter).

## **database**: entries regarding the Database access module. MySQL based.
- **host** *(string)*: Where the database is hosted.
- **user** *(string)*: Username in database.
- **password** *(string)*: Password to corresponding user.
- **db** *(string)*: Name of database/schema to use.
