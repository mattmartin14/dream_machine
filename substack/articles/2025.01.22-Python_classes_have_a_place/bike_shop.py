class BuildBike:
    def __init__(self, customer_name: str):
        self.customer_name = customer_name
        self.has_big_tires = False
        self.is_18_speed = False  # Renamed attribute for clarity
        self.cup_holder_included = False  # Renamed attribute
        self.reflectors_included = False  # Renamed attribute

    def use_big_tires(self):
        self.has_big_tires = True
        return self
    
    def use_18_speed(self):
        self.is_18_speed = True
        return self

    def include_cup_holder(self):
        self.cup_holder_included = True  # Updated to reflect renamed attribute
        return self

    def include_reflectors(self):
        self.reflectors_included = True  # Updated to reflect renamed attribute
        return self

    def build(self):
        msg = f"Customer {self.customer_name} has chosen the following options for their bike:\n"
        msg += f"Big Tires: {self.has_big_tires} \n"
        msg += f"18 Speed: {self.is_18_speed} \n"
        msg += f"Include Cup Holder: {self.cup_holder_included} \n"
        msg += f"Include Reflectors: {self.reflectors_included} \n"

        print(msg)

if __name__ == "__main__":
    
    c1 = BuildBike("Jack")
    c1.use_big_tires().include_cup_holder().build()

    c2 = (
        BuildBike("Cindy")
            .include_cup_holder()
            .use_18_speed()
            .build()
    )
