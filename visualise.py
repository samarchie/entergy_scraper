from json import load
from os import getcwd, listdir
from os.path import join
import plotly
from pandas import DataFrame
from datetime import datetime, timedelta
from tqdm import tqdm
from seaborn import color_palette

MINUTES_BETWEEN_SCRAPING = 30
SAVE_FP = join(getcwd(), "scraped_data")
REGION_NAMES = {
    "ARKA" : "Arkansas", 
    "LOIS" : "Louisiana", 
    "MISS" : "Mississippi", 
    "NOLA" : "New Orleans"
}

def visualise_data(save_fp):

    place_types = listdir(save_fp)
    place_types.remove("NOLAfine") # eg ARKAzip, ARKAcounty
    plotly_data = []
    colour_palette = color_palette("tab10", n_colors=len(place_types)).as_hex()

    for index, place_type in tqdm(enumerate(place_types), "Analysing each place", total=len(place_types), leave=True):

        # Determine name of place
        if "zip" in place_type:
            place = REGION_NAMES[place_type.replace("zip", "")]      
        else:
            continue

        # Order all files by date and time
        filenames = listdir(join(save_fp, place_type))
        filenames.sort(key=lambda x: datetime.strptime(x[:-5], "%d %b %Y %H %M"))
        
        # Load each json file and extract the relevant information
        affected_local = {}
                
        delta = timedelta(minutes=MINUTES_BETWEEN_SCRAPING)
        date = datetime.strptime(filenames[0][:-5], "%d %b %Y %H %M")
        end_date = datetime.strptime(filenames[-1][:-5], "%d %b %Y %H %M")


        while date <= end_date:
            date_str = datetime.strftime(date, "%d %b %Y %H %M")
            filename = date_str + ".json"
            try:
                data = load(open(join(save_fp, place_type, filename), "r"))
                sum_affected = 0
                for entry in data:
                    sum_affected += entry["customersAffected"]
                affected_local[datetime.strptime(filename[:-5], "%d %b %Y %H %M")] = sum_affected
            except FileNotFoundError:
                # No data file was found hence assing None so that the scatter plot line does not connect
                affected_local[datetime.strptime(filename[:-5], "%d %b %Y %H %M")] = None
            
            date += delta

        plotly_data.append(plotly.graph_objs.Scatter(name=place, x=list(affected_local.keys()), y=list(affected_local.values()), mode='lines', line=dict(color=colour_palette[index]), connectgaps=False))


    fig = plotly.graph_objs.Figure(data=plotly_data, layout_title_text='Number of Entergy Energy Customers Affected in Selected US States over the duration of Hurricane Ida')
    fig.update_traces(connectgaps=False, selector=dict(type='scatter'))
    fig.layout.xaxis.title = "Time"
    fig.layout.yaxis.title = "Number of Customers Affected"
    fig.layout.hovermode = 'x'
    _ = fig.update_yaxes(rangemode="tozero")
    fig.write_html("customersaffected.html")
    fig.show()



def visualise_fine_data(save_fp):

    # Visualise the fine data for NOLA.
    data_file_names = listdir(join(save_fp, "NOLAfine"))
    
    people_affected = []
    fault_count = []
    time_labels = [data_file_name[:-5] for data_file_name in data_file_names]

    for data_file_name in data_file_names:
        json_file = load(open(join(save_fp, data_file_name), "r"))
        
        num_faults_today = len(json_file["features"])
        num_people_affected_today = 0
        for outage in json_file["features"]:
            num_people_affected_today += outage["attributes"]["numpeople"]

        fault_count.append(num_faults_today)
        people_affected.append(num_people_affected_today)

    df = DataFrame()
    df["Time"] = time_labels
    df["People Affected"] = people_affected
    df["Number of Faults"] = fault_count

    fig = plotly.graph_objs.Figure(data=[plotly.graph_objs.Scatter(name='Number of Faults', x=df['Time'], y=df["Number of Faults"], mode='lines', line=dict(color='#03C03C'))], layout_title_text='Number of Faults in New Orleans')

    fig.layout.xaxis.title = "Time"
    fig.layout.yaxis.title = "Number of Faults"
    fig.layout.hovermode = 'x'

    fig.show()


if __name__ == "__main__":
    visualise_data(SAVE_FP)