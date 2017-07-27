import json
import argparse


def main(filename):
    data_js = list()
    name = 'combined_data'
    if ',' in filename:
        file_ = filename.split(',')
        for f in file_:
            n, d = get_data(f)
            data_js += d
    else:
        name, data_js = get_data(filename)
    with open('%s.json' % name, 'wb') as f:
        f.write(json.dumps({'data': data_js})) 


def get_data(filename):
    with open(filename, 'rb') as file:
        data = file.readlines()
        data_js = list()
        for row in data:
            row_d = row.split(',')
            data_js.append({'lng': int(row_d[6])/1000000.0, 'lat': int(row_d[7])/1000000.0})
    name = filename.split('.')[0]
    return name, data_js


if __name__ == "__main__":
    # preload_vocabs()
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file")
    
    args = parser.parse_args()
    
    main(args.file)