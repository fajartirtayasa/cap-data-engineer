def get_data():
    '''
    Fungsi ini digunakan untuk mendownload data NYC Green Taxi 2020 ke local directory WSL.
    '''
    import requests

    month = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    urls = [f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2020-{i}.parquet' for i in month]
    save_paths = [f'/home/fatir/green-taxi-data/green-tripdata_2020-{i}.parquet' for i in month]

    for url, save_path in zip(urls, save_paths):
        response = requests.get(url)

        with open(save_path, 'wb') as file:
            file.write(response.content)