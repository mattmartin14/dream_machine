
import xml.etree.ElementTree as ET


def gen_xml_doc_1():
    

    # Create the root element
    root = ET.Element("states")

    # Define the states data
    states_data = [
        {"name": "California", "population": "39538223", "city": "Los Angeles"},
        {"name": "Texas", "population": "29145505", "city": "Houston"},
        {"name": "Florida", "population": "21538187", "city": "Miami"},
        {"name": "New York", "population": "20201249", "city": "New York City"},
        {"name": "Pennsylvania", "population": "13002700", "city": "Philadelphia"},
        {"name": "Illinois", "population": "12812508", "city": "Chicago"},
        {"name": "Ohio", "population": "11799448", "city": "Columbus"},
        {"name": "Georgia", "population": "10711908", "city": "Atlanta"},
        {"name": "North Carolina", "population": "10439388", "city": "Charlotte"},
        {"name": "Michigan", "population": "10077331", "city": "Detroit"}
    ]

    # Populate the XML tree with state data
    for state in states_data:
        state_element = ET.SubElement(root, "state")
        name_element = ET.SubElement(state_element, "name")
        name_element.text = state["name"]
        population_element = ET.SubElement(state_element, "population")
        population_element.text = state["population"]
        city_element = ET.SubElement(state_element, "city")
        city_element.text = state["city"]

    # Create the tree and write to an XML file
    tree = ET.ElementTree(root)
    tree.write("./files/states.xml", encoding="utf-8", xml_declaration=True)


def gen_xml_doc_2():

    # Create the root element
    root = ET.Element("states")

    # Define the states data
    states_data = [
        {"name": "California", "population": "39538223", "city": "Los Angeles", "land_size": "423967"},
        {"name": "Texas", "population": "29145505", "city": "Houston", "land_size": "695662"},
        {"name": "Florida", "population": "21538187", "city": "Miami", "land_size": "170312"},
        {"name": "New York", "population": "20201249", "city": "New York City", "land_size": "141297"},
        {"name": "Pennsylvania", "population": "13002700", "city": "Philadelphia", "land_size": "119280"},
        {"name": "Illinois", "population": "12812508", "city": "Chicago", "land_size": "149995"},
        {"name": "Ohio", "population": "11799448", "city": "Columbus", "land_size": "116096"},
        {"name": "Georgia", "population": "10711908", "city": "Atlanta", "land_size": "153910"},
        {"name": "North Carolina", "population": "10439388", "city": "Charlotte", "land_size": "139391"},
        {"name": "Michigan", "population": "10077331", "city": "Detroit", "land_size": "250487"}
    ]

    # Populate the XML tree with state data
    for state in states_data:
        state_element = ET.SubElement(root, "state", land_size=state["land_size"])
        
        name_element = ET.SubElement(state_element, "name")
        name_element.text = state["name"]
        
        population_element = ET.SubElement(state_element, "population")
        population_element.text = state["population"]
        
        city_element = ET.SubElement(state_element, "city")
        city_element.text = state["city"]

    # Create the tree and write to an XML file
    tree = ET.ElementTree(root)
    tree.write("./files/states2.xml", encoding="utf-8", xml_declaration=True)


if __name__ == "__main__":
    gen_xml_doc_1()
    gen_xml_doc_2()


