import random
from datetime import datetime
SENSORS = {
 "MQ-3": ("Alcohol vapour","mg/L"), "MQ-4": ("Methane","ppm"),
 "MQ-7": ("Carbon monoxide","ppm"), "MQ-8": ("Hydrogen","ppm"),
 "MQ-9": ("CO / combustible response","relative units"),
 "MQ-135": ("Air quality response","relative units")
}
def generate(force_hazard=False):
    hazard = force_hazard or random.random() < .12
    ranges = [(1.1,2.5),(1300,3000),(85,250),(500,1500),(80,200),(500,1200)] if hazard else [(0.05,.8),(100,700),(2,20),(20,180),(5,30),(50,250)]
    values=[round(random.uniform(a,b),2) for a,b in ranges]
    return {"timestamp":datetime.now().astimezone().isoformat(timespec="seconds"),"sensors":list(SENSORS),"values":values,"incident_scenario":hazard}
