import xmltodict
import os
import xml.etree.ElementTree as ET
import csv

def flatten_xml_to_csv(xml_file: str, csv_file_path: str) -> None:

    xml_payload = ''

    with open(xml_file, 'r', encoding='utf-8') as xf:
        xml_payload = xf.read()

    # attempts to remove the format declaration spec
    try:
        if xml_payload.index('?>'):
            dec_end = xml_payload.index('?>')+2
            xml_payload = xml_payload[dec_end:]
    except ValueError:
        pass

    #print(xml_payload)

    xd = xmltodict.parse(xml_payload)

    def flatten_array(arr: list, parent_key = '', sep = '/'):
        items = []
        for i, val in enumerate(arr):
            if isinstance(val, dict):
                items.extend(flatten_dict(val, f"{parent_key}[{i}]", sep=sep).item())
            elif isinstance(val, list):
                items.extend(flatten_array(val, f"{parent_key}[{i}]", sep=sep))
            else:
                items.append((f"{parent_key}[{i}]", val))

        return items
    
    def flatten_dict(d: dict, parent_key='', sep='/'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict) and len(v) == 1 and list(v.keys())[0].startswith("@"):
                attr_key = f"{new_key}/{list(v.keys())[0][1:]}"
                items.append((attr_key, list(v.values())[0]))
            elif isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for i, val in enumerate(v):
                    if isinstance(val, dict):
                        items.extend(flatten_dict(val, f"{new_key}[{i}]", sep=sep).items())
                    elif isinstance(val, list):
                        items.extend(flatten_array(val, f"{new_key}[{i}]",sep=sep))
                    else:
                        items.append((f"{new_key}[{i}]",val))
            else:
                items.append((new_key, v))

        return dict(items)
    

    data = flatten_dict(xd)

    if "~" in csv_file_path:
        csv_file_path = os.path.expanduser(csv_file_path)

    print(csv_file_path)

    with open(csv_file_path, 'w', newline='') as f:
        csv_writer = csv.writer(f)

        headers = ["Path", "Value"]
        csv_writer.writerow(headers)

        for k, v in data.items():
            csv_writer.writerow([k,v])


def main():
    x_path = './files/states.xml'
    c_path = './files/states.csv'

    flatten_xml_to_csv(x_path, c_path)

    x_path = './files/states2.xml'
    c_path = './files/states2.csv'

    flatten_xml_to_csv(x_path, c_path)

if __name__ == "__main__":
    main()