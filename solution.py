import datetime
import multiprocessing

def process_chunk(chunk: str):
    stations = {}

    chunk_rows = chunk.split("\n")

    for line in chunk_rows:
        if line == "":
            continue

        station_name, temp = line.split(";")

        temp = float(temp)

        if station_name not in stations:
            stations[station_name] = (temp, temp, temp, 1) # min, sum, max, count
        else:
            min_temp, sum, max_temp, count = stations[station_name]

            if temp < min_temp:
                min_temp = temp

            sum += temp

            if temp > max_temp:
                max_temp = temp

            count += 1

            stations[station_name] = (min_temp, sum, max_temp, count)

    return stations

if __name__ == "__main__":
    then = datetime.datetime.now()

    with open("measurements.txt", "r") as file:
        def read_chunk():
            seventy_five_mb_read = file.read(78643200) # 1024 * 1024 * 75
            seventy_five_mb_len = len(seventy_five_mb_read)
            
            if seventy_five_mb_len == 0:
                return ''

            if seventy_five_mb_read[seventy_five_mb_len - 1] == "\n":
                return seventy_five_mb_read
            else:
                return seventy_five_mb_read + file.readline()

        chunks = iter(read_chunk, '')

        pool = multiprocessing.Pool()

        all_stations = {}

        process_chunk_fn = process_chunk
        for result in pool.imap_unordered(process_chunk_fn, chunks):
            all_stations = all_stations | result

        pool.close()
        pool.join()

        print("{", end="")
        print_fn = print
        for station_key in all_stations.keys():
            station_data = all_stations[station_key]
            min = station_data[0]
            mean = station_data[1] / station_data[3]
            max = station_data[2]

            print_fn("{}={:.1f}/{:.1f}/{:.1f}".format(station_key, min, mean, max), end=", ")
        print("}", end="")

    print(f"\nFinished in {datetime.datetime.now() - then}")
