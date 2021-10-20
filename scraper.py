from json.decoder import JSONDecodeError
from requests import get
from json import loads, dump, load
from time import strftime, localtime, sleep
from schedule import every, run_pending
from slack import post_message_to_slack
from os import getcwd, listdir, makedirs
from os.path import join
import plotly
from pandas import DataFrame

from logging import basicConfig, getLogger, ERROR
basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=ERROR,
    datefmt='%d-%m-%Y %H:%M:%S')
logger = getLogger(__name__)


minutes_between_scraping = 30
save_fp = join(getcwd(), "scraped_data")

query_urls = {"NOLAzip": "https://entergy.datacapable.com/datacapable/v1/entergy/EntergyNOLA/zip", 
            "NOLAcounty": "https://entergy.datacapable.com/datacapable/v1/entergy/EntergyNOLA/county", 
            "LOISzip": "https://entergy.datacapable.com/datacapable/v1/entergy/EntergyLouisiana/zip",
            "LOIScounty": "https://entergy.datacapable.com/datacapable/v1/entergy/EntergyLouisiana/county", 
            "MISSzip": "https://entergy.datacapable.com/datacapable/v1/entergy/EntergyMississippi/zip", 
            "MISScounty": "https://entergy.datacapable.com/datacapable/v1/entergy/EntergyMississippi/county", 
            "ARKAzip": "https://entergy.datacapable.com/datacapable/v1/entergy/EntergyArkansas/zip", 
            "ARKAcounty" : "https://entergy.datacapable.com/datacapable/v1/entergy/EntergyArkansas/county", 
            "NOLAfine": "https://arcgis.entergy.datacapable.com/arcgis/rest/services/Public/FeatureServer/1/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=*&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&having=&gdbVersion=&historicMoment=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=*&returnZ=false&returnM=false&multipatchOption=xyFootprint&resultOffset=&resultRecordCount=&returnTrueCurves=false&returnExceededLimitFeatures=false&quantizationParameters=&returnCentroid=false&sqlFormat=none&resultType=&f=json"
}


def scrape_data():

    time = strftime("%d %b %Y %H %M", localtime())
    
    for key, query_url in query_urls.items():
        # Grab the data, decode it and then save to a file
        try:
            content_json = loads(get(query_url).content.decode("utf-8"))
            with open(join(save_fp, key, '{}.json'.format(time)), 'w') as file:
                dump(content_json, file)
        # if decoding error happens, most likely there was no data to retrieve
        except JSONDecodeError:
            logger.error("Scraping failed for {} at {} due to decoding error".format(key, time))
            post_message_to_slack("Scraping failed for {} at {} due to decoding error".format(key, time))
            continue
        # Otherwise, bloody weird error occured then
        except Exception as e:
            # log when there is an error
            logger.error("Scraping failed for {} at {} because of {}".format(time, key, e))
            post_message_to_slack("Scraping failed for {} due at {} because of {}".format(time, key, e))
            continue
        # If no error, then log a success message
        finally:
            logger.error("Scraping successful for {} at {}".format(key, time))


def main():

    # Make directories for data storage
    for key in query_urls.keys():
        try:
            makedirs(join(save_fp, key))
        except FileExistsError:
            continue
    
    # Schedule and perform the function every 30 mins
    try:
        every(1).minutes.do(scrape_data)
        while True:
            scrape_time = (localtime().tm_min%minutes_between_scraping==0)
            if scrape_time:
                run_pending()
                sleep(60)

    # Log errors when they happen
    except Exception as e:
        time = strftime("%d %b %Y %H %M", localtime())
        logger.error("Scraping failed for at {} for {}".format(time, e))
        post_message_to_slack("Scraping failed at {} due to {}".format(time, e))
    

if __name__ == "__main__":
    main()



