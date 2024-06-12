# musiconn.performance-harvester

## Description

A python script to harvest the API of [musiconn.performance](https://performance.musiconn.de/api), map the data from json to [CGIF](https://docs.nfdi4culture.de/ta5-cgif-specification) and generate RDF-triples that are saved in ttl-files.

The  harvester will harvest all items from the categories 'event' and 'work'.

## Requirements

+ [Python 3.x](https://www.python.org/downloads/)
+ Requests library
+ rdflib library
+ (Recommended) at least 1 GB of free disk space for the harvested data

## Installation

Clone the repository or download the harvest.py file and the templates directory. Ensure Python 3.x is installed and install the required libraries using:

``pip install requests rdflib``

If you want to start from scratch and harvest all data on your own, you can use the branch clean of this repository:

``git checkout clean``

## Usage

Run the script with the following command:

``python harvest.py -w [WAIT_TIME] -c [ITEM_COUNT] -a [STARTINDEX_EVENT] -b [STARTINDEX_WORK] -F [SINGLE_FILE] -e [LOAD_EVENTS] -l [LOAD_WORKS] -E [DISABLE_EVENTS -W [DISABLE_WORKS]``

Example:

``python harvest.py -w 0.5 -c 10 -a 5 - b 10 -F``

Flags:
* -w: Time to wait between requests to the API. Takes a float as input, default is one second.
* -c: Number of items to be processed. Takes an integer as input, default is all available items. (Warning: this might take a long time!)
* -a: Index of the first event to be processed. Takes an integer as input, default is the first event.
* -b: Index of the first work to be processed. Takes an integer as input, default ist the first work.
* -F: Set this flag, if you want all ttl-files concatenated into one file. Does not overwrite files that are concatenated.
* -e: Set this flag, if you want to load events from the json-files in the directory event_feed, instead of harvesting them.
* -l: Set this flag, if you want to load works from the json-files in the directory work_feed, instead of harvesting them.
* -E: Set this flag, if you want to disable processing of events. If the F-Flag is set, existing ttl-files will still be concatenated.
* -W: Set this flag, if you want to disable processing of works. If the F-Flag is set, existing ttl-files will still be concatenated.

You can also use:

``python harvest.py --help``

To get an overview over all flags listed above in your terminal.
## Output
The script outputs json-files to the _feed directories, with the harvested items mapped to [CGIF](https://docs.nfdi4culture.de/ta5-cgif-specification), and ttl-files to the _result directories. If the F-Flag is set, the ttl-files will also be concatenated into a single feed.ttl file.

## Contributors

@author: Paul Kehrein [https://orcid.org/0009-0004-6540-6498](https://orcid.org/0009-0004-6540-6498)

Further contributors are very welcome!

## License

All code covered by the [MIT](https://opensource.org/license/MIT) license.