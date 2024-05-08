import itertools
import pandas as pd
import sys
import threading
import time

class CBS:

    def __init__(self, url:str):
        
        self.output_dict = self.get_all_data(url)

    def return_data(self):
        return self.output_dict

    def fetch(self, url:str):
        import requests
        try:
            response = requests.get(url)
            data = response.json()
            return data
        
        except Exception as e:
            print(e)
            return None
        
    def extract_data(self, url:str, type:str = 'data'):
    
        skip:int = 0
        top:int = 9999
        data_list = []

        while True:
            # Volgens de API documentatie mag je maximaal 10000 records ophalen per API call
            window:str = f"?$top={top}&$skip={skip}"
            data = self.fetch(url+window)

            if data['value'] == []:
                break

            data_list.append(data)

            if type == 'meta':
                break

            skip += 9999

        if data_list != []:
            # Flatten the list of dictionaries
            flattened_list = [item for sublist in [x['value'] for x in data_list] for item in sublist]

            # Create DataFrame
            df = pd.DataFrame(flattened_list)

            return df
        else:
            return []
    
    def get_all_data(self, url:str):        
        output_dict = {}
        data_sources = []

        data = self.fetch(url)

        for sub_ in data['value']:
            sub_url = sub_["url"]
            data_source = sub_["name"]
            data_sources.append(data_source)

            if data_source in ('UntypedDataSet', 'TypedDataSet'):
                data_type = 'data'
            else:
                data_type = 'meta'

            df = self.extract_data(sub_url, data_type)        

            output_dict[data_source] = df
        print('\n')
        [print(f'Retrieve {x} by using: output["{x}"]') for x in data_sources]

        return output_dict

class CBSDataFetcher(CBS):
    def __init__(self, url: str):
        # Flag to indicate if the program is done
        self.program_done = False
        print('Fetching data...')
        
        # Start the spinner in a separate thread
        spinner_thread = threading.Thread(target=self.print_spinner)
        spinner_thread.start()
        
        # Call the constructor of the parent class
        super().__init__(url)
        
        # Set the program done flag
        self.program_done = True

    # Function to print spinning widget
    def print_spinner(self):
        spinning_chars = itertools.cycle(['-', '/', '|', '\\'])
        while not self.program_done:
            sys.stdout.write(next(spinning_chars))
            sys.stdout.flush()
            sys.stdout.write('\b')
            time.sleep(0.1)