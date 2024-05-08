from funda_scraper import FundaScraper
import numpy as np
from IPython.display import clear_output

stroomverbruik = {
    '>A+': [10, 105],
    'A': [105, 160],
    'B': [160, 190],
    'C': [190, 250],
    'D': [250, 290],
    'E': [290, 335],
    'F': [335, 380],
    'G': [380, 450]  # Ongelimiteerd, dus float('inf') wordt gebruikt om oneindig aan te geven.
}

gasvebruik = {
    1:900,
    2:1100,
    3:1400,
    4:1700,
    5:1900,
    6:2100
}

def process_city(city_name:str) -> None:
    for huis_type in ['buy', 'rent']:
        clear_output(wait=True)
        if '(' in city_name:
            index = city_name.index('(')
            city_name = city_name[:index].strip()
        print(f'\nfetching data for: {city_name} ({huis_type})')

        try:
            scraper = FundaScraper(area=city_name, want_to=huis_type,find_past=False,n_pages=50)
            df = scraper.run(raw_data=False)
            # Sla over als de dataframe leeg is
            if df.empty:
                print(f'No data found for: {city_name}')
                continue

            # Maak de kolommen 'gasvebruik_per_jaar' en 'stroomvebruik_per_jaar' aan en initialiseer met 0
            df['m3_gasverbruik_per_jaar'] = 0
            df['kwh_stroomvebruik_per_jaar'] = 0

            for index, row in df.iterrows():

                # Bereken het stroomverbruik op basis van het energielabel
                key = stroomverbruik.get(row['energy_label'])
                if key is None:
                    df.at[index, 'kwh_stroomvebruik_per_jaar'] = np.nan
                else:
                    onderwaarde, bovenwaarde = key
                    stroom_verbruik = np.random.randint(onderwaarde, bovenwaarde)
                    df.at[index, 'kwh_stroomvebruik_per_jaar'] = (stroom_verbruik * row['living_area']) // 10

                # Bereken het gasverbruik op basis van het aantal slaapkamers (proxy voor aantal personen in de desbetreffende woning)
                key = gasvebruik.get(row['bedroom'])
                if key is None:
                    df.at[index, 'm3_gasverbruik_per_jaar'] = np.nan
                else:                                
                    onderwaarde = key * 0.7
                    bovenwaarde = key * 1.2
                    gas_verbruik_random = np.random.randint(onderwaarde, bovenwaarde)
                    df.at[index, 'm3_gasverbruik_per_jaar'] = gas_verbruik_random
            #sla de data op onder het mapje steden
            try:
                df.to_csv(f'huizen/{huis_type}_{city_name}.csv', index=False, sep=';')
            except Exception as e:
                raise e
        # log als iets verkeerd gaat
        except Exception as e:
            with open('log.txt', 'a') as log:
                new_log = f'{city_name}: {e}\n'
                log.write(new_log)

if __name__ == '__main__':
    with open('steden.txt', 'r') as file:
        for line in file:
            city_name = line.strip()
            if city_name != '':
                process_city(city_name)
