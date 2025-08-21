from math import floor, sqrt

# Master dictionary defining the region of interest for all VELO sensors.
populated_regions = {}
for mod in range(52):
    populated_regions[f"Mod{mod}Sens0"] = {'x': [550, 750], 'y': [200, 250]}
    populated_regions[f"Mod{mod}Sens1"] = {'x': [0, 250], 'y': [150, 250]}
    populated_regions[f"Mod{mod}Sens2"] = {'x': [0, 200], 'y': [200, 250]}
    populated_regions[f"Mod{mod}Sens3"] = {'x': [500, 750], 'y': [180, 250]}

# The options function called by the main script.
def options():
    return {
        "name": "Velo_Hotspot_Occupancy",
        "provider": "VeloMon",
        "y_axis_title": "Avg. Occupancy in Hotspot (Entries/hr)",
        # Note: The 'locations' and 'method' keys are no longer needed here,
        # as the new script handles the logic internally.
    }
