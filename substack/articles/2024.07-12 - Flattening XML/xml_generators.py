
import xml.etree.ElementTree as ET
from xml.dom.minidom import parseString

def states_simple_xml():
    
    root = ET.Element("states")

    data = [
        {"name": "Texas", "population": "30503301", "largest_city": "Houston", "land_mass_sqmi": "261232"},
        {"name": "New York", "population": "19571216", "largest_city": "New York City", "land_mass_sqmi": "54555"},
    ]

    for state in data:
        state_element = ET.SubElement(root, "state")
        name_element = ET.SubElement(state_element, "name")
        name_element.text = state["name"]
        population_element = ET.SubElement(state_element, "population")
        population_element.text = state["population"]
        city_element = ET.SubElement(state_element, "largest_city")
        city_element.text = state["largest_city"]
        land_mass_element = ET.SubElement(state_element, "land_mass_sqmi")
        land_mass_element.text = state["land_mass_sqmi"]

    # Convert the ElementTree to a string
    rough_string = ET.tostring(root, encoding="utf-8")
    # Parse the string with minidom for pretty printing
    reparsed = parseString(rough_string)
    pretty_xml_as_string = reparsed.toprettyxml(indent="  ")

    # Write the pretty-printed XML to a file
    with open("./files/states_simple.xml", "w", encoding="utf-8") as f:
        f.write(pretty_xml_as_string)

def states_complex_xml():

    root = ET.Element("states")

    data = [
        {"name": "Georgia", "nickname": "Peach State", "population": "10711908", "largest_city": "Atlanta", "land_mass_sqmi": "57906", "misc": [{"climate": "warm"}, {"state_flower": "Cherokee Rose"}]},
        {"name": "California", "nickname": "Granola State", "population": "39538223", "largest_city": "Los Angeles", "land_mass_sqmi": "163696", "water_sqmi": "7737", "misc":[{"bird":"California Quail"}]},
    ]

    for state in data:
        state_element = ET.SubElement(root, "state")
        attributes = {"nickname": state["nickname"]}
        name_element = ET.SubElement(state_element, "name", **attributes)
        name_element.text = state["name"]
        population_element = ET.SubElement(state_element, "population")
        population_element.text = state["population"]
        city_element = ET.SubElement(state_element, "largest_city")
        city_element.text = state["largest_city"]
        land_mass_element = ET.SubElement(state_element, "land_mass_sqmi")
        land_mass_element.text = state["land_mass_sqmi"]
        if "water_sqmi" in state:
            water_mass_element = ET.SubElement(state_element, "water_sqmi")
            water_mass_element.text = state["water_sqmi"]
        if "misc" in state:
            misc_el = ET.SubElement(state_element, "misc")
            for misc_item in state['misc']:
                for key, value in misc_item.items():
                    misc_sub_el = ET.SubElement(misc_el, key)
                    misc_sub_el.text = value

    # Convert the ElementTree to a string
    rough_string = ET.tostring(root, encoding="utf-8")
    # Parse the string with minidom for pretty printing
    reparsed = parseString(rough_string)
    pretty_xml_as_string = reparsed.toprettyxml(indent="  ")

    # Write the pretty-printed XML to a file
    with open("./files/states_complex.xml", "w", encoding="utf-8") as f:
        f.write(pretty_xml_as_string)


if __name__ == "__main__":
    states_simple_xml()
    states_complex_xml()